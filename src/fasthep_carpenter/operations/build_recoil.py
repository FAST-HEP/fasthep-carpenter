"""Build transverse recoil collections from MET and visible objects."""

from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.registry.defaults import default_expr_registry
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

RECOIL_FIELDS = {"pt", "phi", "eta", "mass"}

BUILD_RECOIL_SPEC = {
    "name": "hep.build_recoil",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "met": {"type": "string", "required": True},
        "visible": {"type": "list[string]", "required": False, "default": []},
        "output": {"type": "string", "required": True},
        "selection": {"type": "list[string]", "required": False, "default": []},
        "keep": {"type": "list[string]", "required": True},
        "reduce": {"type": "mapping", "required": False},
        "legacy": {"type": "mapping", "required": False},
    },
    "result": {
        "kind": "event_stream",
        "description": (
            "Event stream with transverse recoil candidates and an explicit "
            "pre-reduction recoil count."
        ),
    },
    "requires": {
        "symbols": [
            {
                "from": "params.met",
                "kind": "field_prefix",
                "suffixes": ["pt", "phi"],
            },
            {
                "from": "params.visible",
                "kind": "field_prefix",
                "suffixes": ["pt", "phi"],
            },
            {
                "from": "params.selection",
                "kind": "scoped_expr",
                "allowed": ["pt", "phi", "eta", "mass"],
                "dependency": "none",
            },
        ]
    },
    "provides": {
        "symbols": [
            {
                "from": "params.output",
                "kind": "field_prefix",
                "suffixes_from": "params.keep",
            },
            {"from": "params.output", "kind": "count"},
        ]
    },
}


def run_build_recoil(
    data: dict[str, Any],
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> dict[str, Any]:
    stream_name = (
        params.get("stream")
        or params.get("stream_id")
        or ctx.get("primary_stream")
        or DEFAULT_PRIMARY_STREAM_ID
    )
    events = _normalise_stream(get_stream_array(data, str(stream_name)))
    out = _run_build_recoil_on_stream(events, params, ctx)
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_build_recoil_transform(
    *,
    stream: Any,
    met: str,
    output: str,
    keep: list[str],
    visible: list[str] | None = None,
    selection: list[str] | None = None,
    reduce: dict[str, str] | None = None,
    ctx: Any = None,
    **kwargs: Any,
) -> Any:
    del kwargs
    stream = unwrap_legacy_data_envelope(stream)
    params: dict[str, Any] = {
        "met": met,
        "visible": visible or [],
        "output": output,
        "keep": keep,
    }
    if selection is not None:
        params["selection"] = selection
    if reduce is not None:
        params["reduce"] = reduce

    out = run_build_recoil(
        data=legacy_data_envelope(stream),
        params=params,
        ctx=dict(ctx or {}),
    )
    if ctx is not None and hasattr(ctx, "provenance"):
        deps = _dependencies(params)
        ctx.provenance.record_operation(
            inputs={"symbols": sorted(deps.consumes)},
            outputs={"symbols": sorted(deps.produces)},
        )
    return get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)


def _run_build_recoil_on_stream(
    events: ak.Array,
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> ak.Array:
    met = _required_name(params.get("met"), "met")
    visible = _collection_names(params.get("visible", []))
    output = _required_name(params.get("output"), "output")
    selection = _expression_list(params.get("selection", []), "selection")
    keep = _keep_fields(params.get("keep"))
    reduce_cfg = _reduce_config(params.get("reduce"))

    deps = _dependencies(params)
    for field in sorted(deps.consumes):
        _field(events, field)

    recoil = _build_recoil(events, met=met, visible=visible)

    mask = ak.ones_like(recoil["pt"] == recoil["pt"], dtype=bool)
    for expr in selection:
        mask = mask & eval_expr(
            recoil,
            _normalise_boolean_expression(expr),
            dict(ctx or {}),
        )
    recoil = recoil[mask]
    pre_reduction_count = ak.num(recoil, axis=1)

    if reduce_cfg["keep"] == "highest_pt":
        order = ak.argsort(recoil["pt"], axis=1, ascending=False, stable=True)
        recoil = recoil[order][:, :1]
    else:
        raise ValueError(
            "hep.build_recoil reduce.keep currently supports only 'highest_pt'"
        )

    output_fields = sorted(deps.produces)
    for field in output_fields:
        if field in events.fields:
            raise ValueError(f"hep.build_recoil output field {field!r} already exists")

    out = events
    out = ak.with_field(out, pre_reduction_count, f"n{output}")
    for field in keep:
        out = ak.with_field(out, recoil[field], f"{output}_{field}")
    return out


def _build_recoil(events: ak.Array, *, met: str, visible: list[str]) -> ak.Array:
    met_pt = _field(events, f"{met}_pt")
    met_phi = _field(events, f"{met}_phi")

    if visible:
        first_pt = _field(events, f"{visible[0]}_pt")
        broadcast_met_pt = _broadcast_to_visible(first_pt, met_pt, visible[0], met, "pt")
        broadcast_met_phi = _broadcast_to_visible(
            first_pt,
            met_phi,
            visible[0],
            met,
            "phi",
        )
    else:
        broadcast_met_pt = ak.singletons(met_pt)
        broadcast_met_phi = ak.singletons(met_phi)

    recoil_px = broadcast_met_pt * np.cos(broadcast_met_phi)
    recoil_py = broadcast_met_pt * np.sin(broadcast_met_phi)
    for collection in visible:
        pt = _field(events, f"{collection}_pt")
        phi = _field(events, f"{collection}_phi")
        try:
            recoil_px, recoil_py, pt, phi = ak.broadcast_arrays(
                recoil_px,
                recoil_py,
                pt,
                phi,
            )
        except ValueError as exc:
            raise ValueError(
                "hep.build_recoil visible collection "
                f"{collection!r} is not aligned with earlier recoil candidates"
            ) from exc
        recoil_px = recoil_px + pt * np.cos(phi)
        recoil_py = recoil_py + pt * np.sin(phi)

    pt = np.hypot(recoil_px, recoil_py)
    phi = np.arctan2(recoil_py, recoil_px)
    return ak.zip(
        {
            "pt": pt,
            "phi": phi,
            "eta": ak.zeros_like(pt),
            "mass": ak.zeros_like(pt),
        }
    )


def _broadcast_to_visible(
    reference: ak.Array,
    values: ak.Array,
    visible: str,
    met: str,
    field: str,
) -> ak.Array:
    try:
        return ak.broadcast_arrays(reference, values)[1]
    except ValueError as exc:
        raise ValueError(
            "hep.build_recoil scalar MET field "
            f"{met}_{field} could not be broadcast to visible collection "
            f"{visible!r}"
        ) from exc


def _collection_names(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("hep.build_recoil visible must be a list of collection names")
    return [_required_name(item, "visible") for item in value]


def _keep_fields(value: Any) -> list[str]:
    fields = _field_names(value, "keep")
    unsupported = set(fields) - RECOIL_FIELDS
    if unsupported:
        raise ValueError(
            "hep.build_recoil keep may only include "
            f"{sorted(RECOIL_FIELDS)}, got {sorted(unsupported)}"
        )
    return fields


def _reduce_config(value: Any) -> dict[str, str]:
    if value is None:
        return {"count": "before", "keep": "highest_pt"}
    if not isinstance(value, dict):
        raise ValueError("hep.build_recoil reduce must be a mapping")
    count = value.get("count", "before")
    keep = value.get("keep", "highest_pt")
    if count != "before":
        raise ValueError("hep.build_recoil reduce.count currently supports only 'before'")
    if keep != "highest_pt":
        raise ValueError(
            "hep.build_recoil reduce.keep currently supports only 'highest_pt'"
        )
    return {"count": count, "keep": keep}


def _expression_list(value: Any, param: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"hep.build_recoil {param} must be a list of expressions")
    return [item.strip() for item in value]


def _field_names(value: Any, param: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"hep.build_recoil {param} must be a non-empty list")
    fields = [str(item).strip() for item in value]
    if any(not field or "." in field for field in fields):
        raise ValueError(f"hep.build_recoil {param} fields must be relative names")
    return fields


def _normalise_boolean_expression(expression: str) -> str:
    return expression.replace("&&", " and ").replace("||", " or ")


def _required_name(value: Any, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hep.build_recoil {param} must be a non-empty string")
    return value.strip()


def _field(stream: ak.Array, name: str) -> Any:
    if name not in (getattr(stream, "fields", []) or []):
        raise KeyError(f"Required field {name!r} is missing from event stream")
    return stream[name]


def _normalise_stream(stream: Any) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream
    if isinstance(stream, dict):
        return ak.Array(stream)
    raise TypeError("hep.build_recoil expects an awkward.Array or dict[str, array-like]")


def _dependencies(params: dict[str, Any]):
    registry = default_expr_registry()
    return parse_component_data_dependencies(
        spec=BUILD_RECOIL_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(registry.functions),
            known_constants=set(registry.constants),
            context_symbols=set(),
        ),
    )


__all__ = [
    "BUILD_RECOIL_SPEC",
    "run_build_recoil",
    "run_build_recoil_transform",
]
