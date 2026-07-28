"""Generic source-only object overlap cleaner.

`hep.clean` removes objects from one source collection when they overlap any
object in one or more target collections. Collection references live inside
operation params (`source` and `clean_against`); the `CLEAN_SPEC` dependency
parser expands those references into planner-visible field requirements.

The first supported metric is `delta_r`, evaluated from each collection's
`eta` and `phi` fields using Awkward arrays and vector behavior. Optional
sorting, for example `sort_by: pt`, adds the corresponding source field to the
dependency set. The operation preserves all fields carried by retained source
objects, leaves target collections untouched, and writes a per-event removed
count diagnostic by default. Removed objects are only written when
`diagnostics.keep_removed: true` is configured.
"""

from __future__ import annotations

from typing import Any

import awkward as ak
import vector
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

vector.register_awkward()


CLEAN_SPEC = {
    "name": "hep.clean",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "source": {"type": "string", "required": True},
        "clean_against": {"type": "list[string]", "required": True},
        "output": {"type": "string", "required": False},
        "metric": {"type": "string", "required": False, "default": "delta_r"},
        "min_delta_r": {"type": "number", "required": False, "default": 0.4},
        "mode": {
            "type": "string",
            "required": False,
            "default": "remove_source_on_overlap",
        },
        "sort_by": {"type": "string", "required": False},
        "sort_order": {"type": "string", "required": False, "default": "descending"},
        "diagnostics": {"type": "mapping", "required": False},
    },
    "result": {
        "kind": "event_stream",
        "description": (
            "Event stream with a source collection cleaned by source-only "
            "angular-overlap removal."
        ),
    },
    "dependency_parser": (
        "fasthep_carpenter.operations.clean:parse_clean_data_dependencies"
    ),
}


def parse_clean_data_dependencies(
    params: dict[str, Any],
    **_: Any,
) -> DataDependencyResult:
    source = _required_collection_name(params.get("source"), param="source")
    targets = _target_collection_names(params.get("clean_against"))
    output = clean_output_name(params)

    result = DataDependencyResult(
        consumes={
            f"{source}_eta",
            f"{source}_phi",
            *(f"{target}_eta" for target in targets),
            *(f"{target}_phi" for target in targets),
        },
        produces={output},
    )

    sort_by = params.get("sort_by")
    if sort_by is not None:
        sort_field = _required_collection_field(sort_by, param="sort_by")
        result.consumes.add(f"{source}_{sort_field}")

    removed_count = clean_removed_count_name(params)
    if removed_count is not None:
        result.produces.add(removed_count)

    if _keep_removed(params):
        result.produces.add(f"{output}_removed")

    return result


def clean_output_name(params: dict[str, Any]) -> str:
    output = params.get("output")
    if output is not None:
        return _required_collection_name(output, param="output")
    source = _required_collection_name(params.get("source"), param="source")
    return f"cleaned_{source}"


def clean_removed_count_name(params: dict[str, Any]) -> str | None:
    diagnostics = params.get("diagnostics")
    if diagnostics is False or diagnostics is None:
        output = clean_output_name(params)
        return f"nremoved_{output}"
    if not isinstance(diagnostics, dict):
        raise TypeError("hep.clean diagnostics must be a mapping, false, or omitted")
    removed_count = diagnostics.get("removed_count", f"nremoved_{clean_output_name(params)}")
    if removed_count is False or removed_count is None:
        return None
    if not isinstance(removed_count, str) or not removed_count.strip():
        raise ValueError("hep.clean diagnostics.removed_count must be a field name")
    return removed_count.strip()


def run_clean(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    stream_name = (
        params.get("stream")
        or params.get("stream_id")
        or ctx.get("primary_stream")
        or DEFAULT_PRIMARY_STREAM_ID
    )
    events = _normalise_stream(get_stream_array(data, str(stream_name)))

    source_name = _required_collection_name(params.get("source"), param="source")
    target_names = _target_collection_names(params.get("clean_against"))
    output_name = clean_output_name(params)

    metric = str(params.get("metric", "delta_r"))
    if metric != "delta_r":
        raise ValueError(f"hep.clean only supports metric='delta_r', got {metric!r}")

    mode = str(params.get("mode", "remove_source_on_overlap"))
    if mode != "remove_source_on_overlap":
        raise ValueError(
            "hep.clean only supports mode='remove_source_on_overlap', "
            f"got {mode!r}"
        )

    min_delta_r = float(params.get("min_delta_r", 0.4))
    if min_delta_r < 0:
        raise ValueError("hep.clean min_delta_r must be non-negative")

    source = _read_collection(events, source_name, role="source")
    keep = ak.ones_like(source.array["eta"] == source.array["eta"], dtype=bool)

    source_vectors = _eta_phi_vectors(source.array)
    for target_name in target_names:
        target = _read_collection(events, target_name, role="target")
        target_vectors = _eta_phi_vectors(target.array)
        pairs = ak.cartesian(
            {"source": source_vectors, "target": target_vectors},
            axis=1,
            nested=True,
        )
        delta_r = pairs["source"].deltaR(pairs["target"])
        overlaps = ak.any(delta_r < min_delta_r, axis=2)
        keep = keep & ~overlaps

    cleaned = source.array[keep]
    removed = source.array[~keep]

    sort_by = params.get("sort_by")
    if sort_by is not None:
        sort_field = _required_collection_field(sort_by, param="sort_by")
        if sort_field not in cleaned.fields:
            raise ValueError(
                f"hep.clean sort_by={sort_field!r} requires field "
                f"{source_name}_{sort_field!r}"
            )
        sort_order = str(params.get("sort_order", "descending"))
        if sort_order not in {"ascending", "descending"}:
            raise ValueError(
                "hep.clean sort_order must be 'ascending' or 'descending', "
                f"got {sort_order!r}"
            )
        order = ak.argsort(
            cleaned[sort_field],
            axis=1,
            ascending=sort_order == "ascending",
            stable=True,
        )
        cleaned = cleaned[order]

    out = _write_collection(events, output_name, cleaned, source.style)

    removed_count_name = clean_removed_count_name(params)
    if removed_count_name is not None:
        out = ak.with_field(out, ak.num(removed, axis=1), removed_count_name)

    if _keep_removed(params):
        out = _write_collection(out, f"{output_name}_removed", removed, source.style)

    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_clean_transform(
    *,
    stream: Any,
    source: str,
    clean_against: list[str],
    output: str | None = None,
    metric: str = "delta_r",
    min_delta_r: float = 0.4,
    mode: str = "remove_source_on_overlap",
    sort_by: str | None = None,
    sort_order: str = "descending",
    diagnostics: dict[str, Any] | bool | None = None,
    ctx: dict[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    stream = unwrap_legacy_data_envelope(stream)
    legacy_params: dict[str, Any] = {
        "source": source,
        "clean_against": clean_against,
        "metric": metric,
        "min_delta_r": min_delta_r,
        "mode": mode,
        "sort_order": sort_order,
    }
    if output is not None:
        legacy_params["output"] = output
    if sort_by is not None:
        legacy_params["sort_by"] = sort_by
    if diagnostics is not None:
        legacy_params["diagnostics"] = diagnostics

    out = run_clean(
        data=legacy_data_envelope(stream),
        params=legacy_params,
        ctx=dict(ctx or {}),
        **kwargs,
    )
    return {"events": get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)}


class _Collection:
    def __init__(self, array: ak.Array, style: str) -> None:
        self.array = array
        self.style = style


def _read_collection(events: ak.Array, name: str, *, role: str) -> _Collection:
    if name in events.fields:
        array = events[name]
        _require_collection_fields(array, name=name, role=role)
        return _Collection(array=ak.Array(array), style="record")

    prefix = f"{name}_"
    fields = {
        field.removeprefix(prefix): events[field]
        for field in events.fields
        if field.startswith(prefix)
    }
    if not fields:
        raise ValueError(
            f"hep.clean missing {role} collection {name!r}; expected either "
            f"field {name!r} or flat fields like {name}_eta/{name}_phi"
        )
    array = ak.zip(fields)
    _require_collection_fields(array, name=name, role=role)
    return _Collection(array=array, style="flat")


def _write_collection(
    events: ak.Array,
    name: str,
    collection: ak.Array,
    style: str,
) -> ak.Array:
    if style == "record":
        if name in events.fields:
            raise ValueError(f"hep.clean output field {name!r} already exists")
        return ak.with_field(events, collection, name)

    out = events
    for field in collection.fields:
        output_field = f"{name}_{field}"
        if output_field in out.fields:
            raise ValueError(f"hep.clean output field {output_field!r} already exists")
        out = ak.with_field(out, collection[field], output_field)
    return out


def _eta_phi_vectors(collection: ak.Array) -> ak.Array:
    return ak.zip(
        {
            "pt": ak.ones_like(collection["eta"]),
            "eta": collection["eta"],
            "phi": collection["phi"],
            "mass": ak.zeros_like(collection["eta"]),
        },
        with_name="Momentum4D",
    )


def _require_collection_fields(collection: ak.Array, *, name: str, role: str) -> None:
    missing = [field for field in ("eta", "phi") if field not in collection.fields]
    if missing:
        raise ValueError(
            f"hep.clean {role} collection {name!r} is missing required "
            f"delta-R field(s): {missing}"
        )


def _normalise_stream(stream: Any) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream
    if isinstance(stream, dict):
        return ak.Array(stream)
    raise TypeError("hep.clean expects an awkward.Array or dict[str, array-like]")


def _target_collection_names(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("hep.clean clean_against must be a non-empty list")
    return [_required_collection_name(item, param="clean_against") for item in value]


def _required_collection_name(value: Any, *, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hep.clean {param} must be a non-empty string")
    return value.strip()


def _required_collection_field(value: Any, *, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hep.clean {param} must be a non-empty field name")
    if "_" in value:
        raise ValueError(
            f"hep.clean {param}={value!r} should name a source collection field "
            "such as 'pt', not a full stream field"
        )
    return value.strip()


def _keep_removed(params: dict[str, Any]) -> bool:
    diagnostics = params.get("diagnostics")
    if not isinstance(diagnostics, dict):
        return False
    return bool(diagnostics.get("keep_removed", False))
