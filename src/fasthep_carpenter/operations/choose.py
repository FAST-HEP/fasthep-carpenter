from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.registry.defaults import default_expr_registry
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.operations._validation import (
    validate_event_mask,
    validate_event_value,
)
from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

CHOOSE_SPEC = {
    "name": "hep.choose",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "cases": {"type": "list[mapping]", "required": True},
        "default": {"type": "mapping", "required": False},
        "on_multiple": {"type": "string", "required": False, "default": "error"},
        "on_no_match": {"type": "string", "required": False, "default": "error"},
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with coherently chosen event-level fields.",
    },
    "dependency_parser": (
        "fasthep_carpenter.operations.choose:parse_choose_column_dependencies"
    ),
}

CHOOSE_MASK_GUIDANCE = (
    "hep.choose case predicates must produce one boolean per event ('N * bool'). "
    "For object-level predicates, reduce them explicitly upstream."
)


def parse_choose_column_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    result = DataDependencyResult()
    cases = _case_specs(params.get("cases"))
    on_no_match = _policy(
        params.get("on_no_match", "error"),
        allowed={"error", "default"},
        name="on_no_match",
    )
    produced = set(_validate_output_contract(cases, params, on_no_match=on_no_match))
    result.produces.update(produced)

    def add_expr(expr: Any) -> None:
        if expr is None:
            return
        result.consumes.update(
            data_symbols_in_expr(
                str(expr),
                known_functions=known_functions,
                known_constants=known_constants,
                context_symbols=context_symbols,
                produced=produced,
            )
        )

    for case in cases:
        add_expr(case["when"])
        for expr in case["values"].values():
            add_expr(expr)

    default = params.get("default")
    if isinstance(default, dict):
        for expr in default.values():
            add_expr(expr)

    return result


def run_choose_transform(
    *,
    stream: Any,
    cases: list[dict[str, Any]],
    default: dict[str, Any] | None = None,
    on_multiple: str = "error",
    on_no_match: str = "error",
    ctx: Any = None,
    **kwargs: Any,
) -> Any:
    del kwargs
    stream = unwrap_legacy_data_envelope(stream)
    params: dict[str, Any] = {
        "cases": cases,
        "on_multiple": on_multiple,
        "on_no_match": on_no_match,
    }
    if default is not None:
        params["default"] = default
    out = run_choose(
        data=legacy_data_envelope(stream),
        params=params,
        ctx=dict(ctx or {}),
    )
    if ctx is not None and hasattr(ctx, "provenance"):
        deps = _runtime_dependencies(params, dict(ctx or {}))
        ctx.provenance.record_operation(
            inputs={"symbols": sorted(deps.consumes)},
            outputs={"symbols": sorted(deps.produces)},
        )
    return get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)


def run_choose(
    data: dict[str, Any],
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> dict[str, Any]:
    events = get_stream_array(
        data,
        ctx.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID),
    )
    n_events = len(events)
    cases = _case_specs(params.get("cases"))
    on_multiple = _policy(
        params.get("on_multiple", "error"),
        allowed={"error", "first"},
        name="on_multiple",
    )
    on_no_match = _policy(
        params.get("on_no_match", "error"),
        allowed={"error", "default"},
        name="on_no_match",
    )
    output_names = _validate_output_contract(
        cases,
        params,
        on_no_match=on_no_match,
    )
    _validate_output_collisions(events, output_names)

    case_masks = [
        validate_event_mask(
            eval_expr(events, str(case["when"]), ctx),
            n_events=n_events,
            expression=case["when"],
            context=f"hep.choose case {_case_label(case)!r}",
            guidance=CHOOSE_MASK_GUIDANCE,
        )
        for case in cases
    ]
    match_count = _match_count(case_masks, n_events=n_events)
    if on_multiple == "error":
        _raise_multiple_match_error(
            case_masks,
            match_count,
            cases=cases,
            stage=_stage_label(ctx),
        )
    if on_no_match == "error":
        _raise_no_match_error(match_count, stage=_stage_label(ctx))

    case_values = [
        {
            name: validate_event_value(
                eval_expr(events, str(case["values"][name]), ctx),
                n_events=n_events,
                context=(f"hep.choose case {_case_label(case)!r} output {name!r}"),
            )
            for name in output_names
        }
        for case in cases
    ]
    if on_no_match == "default":
        default_values = _default_values(events, params, output_names, n_events, ctx)
    else:
        default_values = dict(case_values[0])

    chosen = dict(default_values)
    for mask, values in reversed(list(zip(case_masks, case_values, strict=True))):
        for name in output_names:
            chosen[name] = ak.where(mask, values[name], chosen[name])

    out = events
    for name in output_names:
        out = ak.with_field(out, chosen[name], name)
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def _case_specs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("hep.choose cases must be a non-empty list")
    cases = []
    for index, case in enumerate(value):
        if not isinstance(case, dict):
            raise ValueError(f"hep.choose cases[{index}] must be a mapping")
        when = case.get("when")
        if not isinstance(when, str) or not when.strip():
            raise ValueError(f"hep.choose case {_case_label(case, index)} needs 'when'")
        values = case.get("values")
        if not isinstance(values, dict) or not values:
            raise ValueError(
                f"hep.choose case {_case_label(case, index)} needs non-empty values"
            )
        cases.append(case)
    return cases


def _validate_output_contract(
    cases: list[dict[str, Any]],
    params: dict[str, Any],
    *,
    on_no_match: str,
) -> list[str]:
    output_names = _output_names(cases)
    for index, case in enumerate(cases):
        names = _mapping_keys(case["values"])
        if names != set(output_names):
            missing = sorted(set(output_names) - names)
            extra = sorted(names - set(output_names))
            fields = sorted(set(missing) | set(extra))
            raise ValueError(
                f"hep.choose case {_case_label(case, index)} has inconsistent "
                f"outputs for field(s) {fields}; missing={missing}, extra={extra}"
            )

    if on_no_match == "default":
        default = params.get("default")
        if not isinstance(default, dict):
            raise ValueError(
                "hep.choose on_no_match='default' requires a default mapping"
            )
        default_names = _mapping_keys(default)
        if default_names != set(output_names):
            missing = sorted(set(output_names) - default_names)
            extra = sorted(default_names - set(output_names))
            fields = sorted(set(missing) | set(extra))
            raise ValueError(
                "hep.choose default has inconsistent outputs; "
                f"field(s) {fields}; missing={missing}, extra={extra}"
            )
    return output_names


def _output_names(cases: Any) -> list[str]:
    case_specs = _case_specs(cases)
    output_names: list[str] = []
    seen: set[str] = set()
    for case in case_specs:
        for name in case["values"]:
            if not isinstance(name, str) or not name.strip():
                raise ValueError("hep.choose output names must be non-empty strings")
            normalized = name.strip()
            if normalized not in seen:
                seen.add(normalized)
                output_names.append(normalized)
    return output_names


def _mapping_keys(value: dict[str, Any]) -> set[str]:
    names = set()
    for name in value:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("hep.choose output names must be non-empty strings")
        names.add(name.strip())
    return names


def _validate_output_collisions(events: Any, output_names: list[str]) -> None:
    collisions = [name for name in output_names if name in events.fields]
    if collisions:
        raise ValueError(
            f"hep.choose would overwrite existing stream field(s): {sorted(collisions)}"
        )


def _policy(value: Any, *, allowed: set[str], name: str) -> str:
    policy = str(value or "").strip()
    if policy not in allowed:
        raise ValueError(
            f"hep.choose {name} must be one of {sorted(allowed)}, got {value!r}"
        )
    return policy


def _match_count(case_masks: list[Any], *, n_events: int) -> Any:
    count = ak.Array(np.zeros(n_events, dtype=np.int64))
    for mask in case_masks:
        count = count + ak.values_astype(mask, np.int64)
    return count


def _raise_multiple_match_error(
    case_masks: list[Any],
    match_count: Any,
    *,
    cases: list[dict[str, Any]],
    stage: str,
) -> None:
    conflict_indices = _indices_where(match_count > 1)
    if not conflict_indices:
        return
    first = conflict_indices[0]
    matched = [
        _case_label(case, index)
        for index, (case, mask) in enumerate(zip(cases, case_masks, strict=True))
        if bool(mask[first])
    ]
    first_indices = conflict_indices[:5]
    raise ValueError(
        f"hep.choose stage {stage!r} found {len(conflict_indices)} events "
        f"matching multiple cases. First conflict indices: {first_indices}. "
        f"First conflict at event {first} matched: {', '.join(matched)}."
    )


def _raise_no_match_error(match_count: Any, *, stage: str) -> None:
    no_match_indices = _indices_where(match_count == 0)
    if not no_match_indices:
        return
    first = no_match_indices[0]
    raise ValueError(
        f"hep.choose stage {stage!r} found {len(no_match_indices)} events "
        f"matching no cases. First no-match event: {first}."
    )


def _indices_where(mask: Any, *, limit: int | None = None) -> list[int]:
    indices = ak.to_numpy(ak.local_index(mask)[mask])
    out = [int(index) for index in indices]
    return out if limit is None else out[:limit]


def _default_values(
    events: Any,
    params: dict[str, Any],
    output_names: list[str],
    n_events: int,
    ctx: dict[str, Any],
) -> dict[str, Any]:
    default = params["default"]
    return {
        name: validate_event_value(
            eval_expr(events, str(default[name]), ctx),
            n_events=n_events,
            context=f"hep.choose default output {name!r}",
        )
        for name in output_names
    }


def _case_label(case: dict[str, Any], index: int | None = None) -> str:
    name = case.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    if index is None:
        return "case"
    return f"case[{index}]"


def _stage_label(ctx: dict[str, Any]) -> str:
    node_id = str(ctx.get("node_id") or "unknown")
    return node_id.removeprefix("stage.")


def _runtime_dependencies(
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> DataDependencyResult:
    registry = ctx.get("expr_registry") or default_expr_registry()
    return parse_choose_column_dependencies(
        params,
        known_functions=set(getattr(registry, "functions", {})),
        known_constants=set(getattr(registry, "constants", {})),
        context_symbols=set(ctx),
    )
