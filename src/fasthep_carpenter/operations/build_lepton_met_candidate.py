"""Build transverse lepton+MET candidate collections."""

from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
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
CANDIDATE_FIELDS = {"pt", "eta", "phi", "mass", "MT"}

BUILD_LEPTON_MET_CANDIDATE_SPEC = {
    "name": "hep.build_lepton_met_candidate",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "lepton": {"type": "string", "required": True},
        "met": {"type": "string", "required": True},
        "output": {"type": "string", "required": True},
        "selection": {"type": "mapping", "required": False},
        "keep": {"type": "mapping", "required": True},
        "legacy": {"type": "mapping", "required": False},
    },
    "result": {
        "kind": "event_stream",
        "description": (
            "Event stream with lepton+MET candidates, aligned leptons, "
            "and explicit selected-lepton and candidate counts."
        ),
    },
    "requires": {
        "symbols": [
            {
                "from": "params.lepton",
                "kind": "field_prefix",
                "suffixes": ["pt", "eta", "phi", "mass"],
            },
            {
                "from": "params.lepton",
                "kind": "field_prefix",
                "suffixes_from": "params.keep.lepton",
            },
            {
                "from": "params.selection.lepton",
                "kind": "relative_expr",
                "prefix_from": "params.lepton",
            },
            {
                "from": "params.met",
                "kind": "field_prefix",
                "suffixes": ["pt", "phi"],
            },
            {
                "from": "params.selection.candidate",
                "kind": "scoped_expr",
                "allowed": ["pt", "eta", "phi", "mass", "MT"],
                "dependency": "none",
            },
        ]
    },
    "provides": {
        "symbols": [
            {
                "from": "params.output",
                "kind": "field_prefix",
                "prefix_suffix": "_W",
                "suffixes_from": "params.keep.candidate",
            },
            {
                "from": "params.output",
                "kind": "field_prefix",
                "prefix_suffix": "_lepton",
                "suffixes_from": "params.keep.lepton",
            },
            {"from": "params.output", "kind": "count", "prefix_suffix": "_W"},
            {"from": "params.output", "kind": "count", "prefix_suffix": "_lepton"},
        ]
    },
}


def run_build_lepton_met_candidate(
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
    out = _run_build_lepton_met_candidate_on_stream(events, params, ctx)
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_build_lepton_met_candidate_transform(
    *,
    stream: Any,
    lepton: str,
    met: str,
    output: str,
    selection: dict[str, Any] | None = None,
    keep: dict[str, list[str]],
    ctx: Any = None,
    **kwargs: Any,
) -> Any:
    del kwargs
    stream = unwrap_legacy_data_envelope(stream)
    params: dict[str, Any] = {
        "lepton": lepton,
        "met": met,
        "output": output,
        "keep": keep,
    }
    if selection is not None:
        params["selection"] = selection

    out = run_build_lepton_met_candidate(
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


def _run_build_lepton_met_candidate_on_stream(
    events: ak.Array,
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> ak.Array:
    lepton_name = _required_name(params.get("lepton"), "lepton")
    met_name = _required_name(params.get("met"), "met")
    output = _required_name(params.get("output"), "output")
    selection = _selection_config(params.get("selection"))
    keep = _keep_config(params.get("keep"))

    deps = _dependencies(params)
    required_fields = sorted(deps.consumes)
    for field in required_fields:
        _field(events, field)

    lepton_fields = _lepton_fields(params)
    leptons = _read_flat_collection(events, lepton_name, lepton_fields)

    lepton_mask = ak.ones_like(leptons["pt"] == leptons["pt"], dtype=bool)
    for expr in selection["lepton"]:
        lepton_mask = lepton_mask & eval_expr(
            leptons,
            _normalise_boolean_expression(expr),
            dict(ctx or {}),
        )
    selected_leptons = leptons[lepton_mask]
    selected_lepton_count = ak.num(selected_leptons, axis=1)

    met = _read_met(events, met_name)
    candidates = _candidate_vectors(selected_leptons, met)
    candidate_mask = ak.ones_like(candidates["pt"] == candidates["pt"], dtype=bool)
    for expr in selection["candidate"]:
        candidate_mask = candidate_mask & eval_expr(
            candidates,
            _normalise_boolean_expression(expr),
            dict(ctx or {}),
        )
    candidates = candidates[candidate_mask]
    aligned_leptons = selected_leptons[candidate_mask]

    output_fields = sorted(deps.produces)
    for field in output_fields:
        if field in events.fields:
            raise ValueError(
                "hep.build_lepton_met_candidate output field "
                f"{field!r} already exists"
            )

    candidate_name = f"{output}_W"
    lepton_output_name = f"{output}_lepton"
    out = events
    out = ak.with_field(out, selected_lepton_count, f"n{lepton_output_name}")
    out = ak.with_field(out, ak.num(candidates, axis=1), f"n{candidate_name}")
    for field in keep["candidate"]:
        out = ak.with_field(out, candidates[field], f"{candidate_name}_{field}")
    for field in keep["lepton"]:
        out = ak.with_field(out, aligned_leptons[field], f"{lepton_output_name}_{field}")
    return out


def _candidate_vectors(leptons: ak.Array, met: ak.Array) -> ak.Array:
    met_pt = ak.broadcast_arrays(leptons["pt"], met["pt"])[1]
    met_phi = ak.broadcast_arrays(leptons["phi"], met["phi"])[1]
    met_eta = ak.zeros_like(met_pt)
    met_mass = ak.zeros_like(met_pt)
    lepton_vec = ak.zip(
        {
            "pt": leptons["pt"],
            "eta": leptons["eta"],
            "phi": leptons["phi"],
            "mass": leptons["mass"],
        },
        with_name="Momentum4D",
    )
    met_vec = ak.zip(
        {
            "pt": met_pt,
            "eta": met_eta,
            "phi": met_phi,
            "mass": met_mass,
        },
        with_name="Momentum4D",
    )
    candidate = lepton_vec + met_vec
    mt = np.sqrt(2 * met_pt * leptons["pt"] * (1 - np.cos(leptons["phi"] - met_phi)))
    return ak.zip(
        {
            "pt": candidate.pt,
            "eta": candidate.eta,
            "phi": candidate.phi,
            "mass": candidate.mass,
            "MT": mt,
        },
        with_name="Momentum4D",
    )


def _lepton_fields(params: dict[str, Any]) -> list[str]:
    lepton_name = _required_name(params.get("lepton"), "lepton")
    deps = _dependencies(params)
    fields: set[str] = set()
    prefix = f"{lepton_name}_"
    for symbol in deps.consumes:
        if symbol.startswith(prefix):
            fields.add(symbol.removeprefix(prefix))
    return sorted(fields)


def _read_flat_collection(
    events: ak.Array,
    collection: str,
    fields: list[str],
) -> ak.Array:
    return ak.zip({field: _field(events, f"{collection}_{field}") for field in fields})


def _read_met(events: ak.Array, met: str) -> ak.Array:
    return ak.zip(
        {
            "pt": _field(events, f"{met}_pt"),
            "phi": _field(events, f"{met}_phi"),
        }
    )


def _selection_config(value: Any) -> dict[str, list[str]]:
    if value is None:
        return {"lepton": [], "candidate": []}
    if not isinstance(value, dict):
        raise ValueError("hep.build_lepton_met_candidate selection must be a mapping")
    return {
        "lepton": _expression_list(value.get("lepton"), "selection.lepton"),
        "candidate": _expression_list(
            value.get("candidate"),
            "selection.candidate",
        ),
    }


def _keep_config(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        raise ValueError("hep.build_lepton_met_candidate keep must be a mapping")
    candidate = _field_names(value.get("candidate"), "keep.candidate")
    leptons = _field_names(value.get("lepton"), "keep.lepton")
    missing_candidate = set(candidate) - CANDIDATE_FIELDS
    if missing_candidate:
        raise ValueError(
            "hep.build_lepton_met_candidate keep.candidate may only include "
            f"{sorted(CANDIDATE_FIELDS)}, got {sorted(missing_candidate)}"
        )
    return {"candidate": candidate, "lepton": leptons}


def _expression_list(value: Any, param: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(
            f"hep.build_lepton_met_candidate {param} must be a list of expressions"
        )
    return [item.strip() for item in value]


def _field_names(value: Any, param: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(
            f"hep.build_lepton_met_candidate {param} must be a non-empty list"
        )
    fields = [str(item).strip() for item in value]
    if any(not field or "." in field for field in fields):
        raise ValueError(
            "hep.build_lepton_met_candidate "
            f"{param} fields must be collection-relative names"
        )
    return fields


def _normalise_boolean_expression(expression: str) -> str:
    return expression.replace("&&", " and ").replace("||", " or ")


def _required_name(value: Any, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"hep.build_lepton_met_candidate {param} must be a non-empty string"
        )
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
    raise TypeError(
        "hep.build_lepton_met_candidate expects an awkward.Array "
        "or dict[str, array-like]"
    )


def _dependencies(params: dict[str, Any]):
    registry = default_expr_registry()
    return parse_component_data_dependencies(
        spec=BUILD_LEPTON_MET_CANDIDATE_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(registry.functions),
            known_constants=set(registry.constants),
            context_symbols=set(),
        ),
    )


__all__ = [
    "BUILD_LEPTON_MET_CANDIDATE_SPEC",
    "run_build_lepton_met_candidate",
    "run_build_lepton_met_candidate_transform",
]
