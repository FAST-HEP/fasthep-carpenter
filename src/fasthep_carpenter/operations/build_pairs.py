"""Build pair-candidate collections from one or more object collections."""

from __future__ import annotations

import ast
from typing import Any

import awkward as ak
import vector
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

vector.register_awkward()

VECTOR_FIELDS = {"pt", "eta", "phi", "mass"}

BUILD_PAIRS_SPEC = {
    "name": "hep.build_pairs",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "collections": {"type": "list[string]", "required": True},
        "output": {"type": "string", "required": True},
        "pair": {"type": "mapping", "required": False},
        "selection": {"type": "mapping", "required": False},
        "sort": {"type": "mapping", "required": False},
        "keep": {"type": "mapping", "required": True},
        "legacy": {"type": "mapping", "required": False},
    },
    "result": {
        "kind": "event_stream",
        "description": (
            "Event stream with pair candidates, aligned constituent collections, "
            "and an explicit candidate count."
        ),
    },
    "requires": {
        "symbols": [
            {
                "from": "params.collections",
                "kind": "field_prefix",
                "suffixes": ["pt", "eta", "phi", "mass"],
            },
            {
                "from": "params.collections",
                "kind": "field_prefix",
                "suffixes_from": "params.keep.constituents",
            },
            {
                "from": "params.selection.pair",
                "kind": "scoped_expr",
                "symbol_prefixes": ["lepton_1_", "lepton_2_"],
                "prefixes_from": "params.collections",
            },
            {
                "from": "params.selection.candidate",
                "kind": "scoped_expr",
                "allowed": ["pt", "eta", "phi", "mass"],
                "dependency": "none",
            },
            {
                "from": "params.sort.by",
                "kind": "scoped_expr",
                "allowed": ["pt", "eta", "phi", "mass"],
                "dependency": "none",
                "skip_if_false": "params.sort",
            },
        ]
    },
    "provides": {
        "symbols": [
            {
                "from": "params.output",
                "kind": "field_prefix",
                "prefix_suffix": "_Z",
                "suffixes_from": "params.keep.candidate",
            },
            {
                "from": "params.output",
                "kind": "field_prefix",
                "prefix_suffix": "_lepton_1",
                "suffixes_from": "params.keep.constituents",
            },
            {
                "from": "params.output",
                "kind": "field_prefix",
                "prefix_suffix": "_lepton_2",
                "suffixes_from": "params.keep.constituents",
            },
            {"from": "params.output", "kind": "count", "prefix_suffix": "_Z"},
        ]
    },
}


def run_build_pairs(
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
    out = _run_build_pairs_on_stream(events, params, ctx)
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_build_pairs_transform(
    *,
    stream: Any,
    collections: list[str],
    output: str,
    pair: dict[str, Any] | None = None,
    selection: dict[str, Any] | None = None,
    sort: dict[str, Any] | bool | None = None,
    keep: dict[str, list[str]],
    ctx: Any = None,
    **kwargs: Any,
) -> Any:
    del kwargs
    stream = unwrap_legacy_data_envelope(stream)
    params: dict[str, Any] = {
        "collections": collections,
        "output": output,
        "keep": keep,
    }
    if pair is not None:
        params["pair"] = pair
    if selection is not None:
        params["selection"] = selection
    if sort is not None:
        params["sort"] = sort

    out = run_build_pairs(
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


def _run_build_pairs_on_stream(
    events: ak.Array,
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> ak.Array:
    collections = _collection_names(params.get("collections"))
    output = _required_name(params.get("output"), "output")
    pair_cfg = _pair_config(params.get("pair"))
    selection = _selection_config(params.get("selection"))
    sort_exprs = _sort_expression(params.get("sort"))
    sort_order = _sort_order(params.get("sort"))
    keep = _keep_config(params.get("keep"))

    if not pair_cfg["unordered"]:
        raise ValueError("hep.build_pairs currently requires pair.unordered=true")
    if not pair_cfg["distinct_objects"]:
        raise ValueError("hep.build_pairs currently requires pair.distinct_objects=true")

    deps = _dependencies(params)
    required_fields = sorted(deps.consumes)
    for field in required_fields:
        _field(events, field)

    source_fields = _source_fields(params)
    merged = ak.concatenate(
        [_read_flat_collection(events, collection, source_fields) for collection in collections],
        axis=1,
    )
    lepton_1, lepton_2 = ak.unzip(ak.combinations(merged, 2))

    pair_context = _pair_context(lepton_1, lepton_2, source_fields)
    pair_mask = ak.ones_like(lepton_1["pt"] == lepton_1["pt"], dtype=bool)
    for expr in selection["pair"]:
        pair_mask = pair_mask & eval_expr(
            pair_context,
            _normalise_boolean_expression(expr),
            dict(ctx or {}),
        )
    lepton_1 = lepton_1[pair_mask]
    lepton_2 = lepton_2[pair_mask]

    candidate = _candidate_vectors(lepton_1, lepton_2)
    candidate_mask = ak.ones_like(candidate["pt"] == candidate["pt"], dtype=bool)
    for expr in selection["candidate"]:
        candidate_mask = candidate_mask & eval_expr(
            candidate,
            _normalise_boolean_expression(expr),
            dict(ctx or {}),
        )
    candidate = candidate[candidate_mask]
    lepton_1 = lepton_1[candidate_mask]
    lepton_2 = lepton_2[candidate_mask]

    if sort_exprs:
        sort_values = eval_expr(
            candidate,
            _normalise_boolean_expression(sort_exprs[0]),
            dict(ctx or {}),
        )
        order = ak.argsort(
            sort_values,
            axis=1,
            ascending=sort_order == "ascending",
            stable=True,
        )
        candidate = candidate[order]
        lepton_1 = lepton_1[order]
        lepton_2 = lepton_2[order]

    output_fields = sorted(deps.produces)
    for field in output_fields:
        if field in events.fields:
            raise ValueError(f"hep.build_pairs output field {field!r} already exists")

    candidate_name = f"{output}_Z"
    out = events
    out = ak.with_field(out, ak.num(candidate, axis=1), f"n{candidate_name}")
    for field in keep["candidate"]:
        out = ak.with_field(out, candidate[field], f"{candidate_name}_{field}")
    for index, constituent in ((1, lepton_1), (2, lepton_2)):
        for field in keep["constituents"]:
            out = ak.with_field(
                out,
                constituent[field],
                f"{output}_lepton_{index}_{field}",
            )
    return out


def _candidate_vectors(lepton_1: ak.Array, lepton_2: ak.Array) -> ak.Array:
    vec1 = ak.zip(
        {
            "pt": lepton_1["pt"],
            "eta": lepton_1["eta"],
            "phi": lepton_1["phi"],
            "mass": lepton_1["mass"],
        },
        with_name="Momentum4D",
    )
    vec2 = ak.zip(
        {
            "pt": lepton_2["pt"],
            "eta": lepton_2["eta"],
            "phi": lepton_2["phi"],
            "mass": lepton_2["mass"],
        },
        with_name="Momentum4D",
    )
    candidate = vec1 + vec2
    return ak.zip(
        {
            "pt": candidate.pt,
            "eta": candidate.eta,
            "phi": candidate.phi,
            "mass": candidate.mass,
        },
        with_name="Momentum4D",
    )


def _source_fields(params: dict[str, Any]) -> list[str]:
    collections = _collection_names(params.get("collections"))
    deps = _dependencies(params)
    fields: set[str] = set()
    for symbol in deps.consumes:
        for collection in collections:
            prefix = f"{collection}_"
            if symbol.startswith(prefix):
                fields.add(symbol.removeprefix(prefix))
    return sorted(fields)


def _read_flat_collection(
    events: ak.Array,
    collection: str,
    fields: list[str],
) -> ak.Array:
    return ak.zip({field: _field(events, f"{collection}_{field}") for field in fields})


def _pair_context(
    lepton_1: ak.Array,
    lepton_2: ak.Array,
    fields: list[str],
) -> ak.Array:
    data = {}
    for field in fields:
        data[f"lepton_1_{field}"] = lepton_1[field]
        data[f"lepton_2_{field}"] = lepton_2[field]
    return ak.zip(data)


def _normalise_boolean_expression(expression: str) -> str:
    return expression.replace("&&", " and ").replace("||", " or ")


def _collection_names(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("hep.build_pairs collections must be a non-empty list")
    return [_required_name(item, "collections") for item in value]


def _pair_config(value: Any) -> dict[str, bool]:
    if value is None:
        return {"unordered": True, "distinct_objects": True}
    if not isinstance(value, dict):
        raise ValueError("hep.build_pairs pair must be a mapping")
    return {
        "unordered": bool(value.get("unordered", True)),
        "distinct_objects": bool(value.get("distinct_objects", True)),
    }


def _selection_config(value: Any) -> dict[str, list[str]]:
    if value is None:
        return {"pair": [], "candidate": []}
    if not isinstance(value, dict):
        raise ValueError("hep.build_pairs selection must be a mapping")
    return {
        "pair": _expression_list(value.get("pair"), "selection.pair"),
        "candidate": _expression_list(
            value.get("candidate"),
            "selection.candidate",
        ),
    }


def _sort_expression(value: Any) -> list[str]:
    if value in (None, False):
        return []
    if not isinstance(value, dict):
        raise ValueError("hep.build_pairs sort must be a mapping, false, or omitted")
    by = value.get("by")
    if by is None:
        return []
    if not isinstance(by, str) or not by.strip():
        raise ValueError("hep.build_pairs sort.by must be a non-empty expression")
    ast.parse(_normalise_boolean_expression(by.strip()), mode="eval")
    return [by.strip()]


def _sort_order(value: Any) -> str:
    if value in (None, False):
        return "ascending"
    if not isinstance(value, dict):
        raise ValueError("hep.build_pairs sort must be a mapping, false, or omitted")
    order = str(value.get("order", "ascending"))
    if order not in {"ascending", "descending"}:
        raise ValueError(
            "hep.build_pairs sort.order must be 'ascending' or 'descending', "
            f"got {order!r}"
        )
    return order


def _keep_config(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        raise ValueError("hep.build_pairs keep must be a mapping")
    candidate = _field_names(value.get("candidate"), "keep.candidate")
    constituents = _field_names(value.get("constituents"), "keep.constituents")
    missing_candidate = set(candidate) - VECTOR_FIELDS
    if missing_candidate:
        raise ValueError(
            "hep.build_pairs keep.candidate may only include "
            f"{sorted(VECTOR_FIELDS)}, got {sorted(missing_candidate)}"
        )
    return {"candidate": candidate, "constituents": constituents}


def _expression_list(value: Any, param: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"hep.build_pairs {param} must be a list of expressions")
    return [item.strip() for item in value]


def _field_names(value: Any, param: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"hep.build_pairs {param} must be a non-empty list")
    fields = [str(item).strip() for item in value]
    if any(not field or "." in field for field in fields):
        raise ValueError(
            f"hep.build_pairs {param} fields must be collection-relative names"
        )
    return fields


def _required_name(value: Any, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hep.build_pairs {param} must be a non-empty string")
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
    raise TypeError("hep.build_pairs expects an awkward.Array or dict[str, array-like]")


def _dependencies(params: dict[str, Any]):
    registry = default_expr_registry()
    return parse_component_data_dependencies(
        spec=BUILD_PAIRS_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(registry.functions),
            known_constants=set(registry.constants),
            context_symbols=set(),
        ),
    )


__all__ = [
    "BUILD_PAIRS_SPEC",
    "run_build_pairs",
    "run_build_pairs_transform",
]
