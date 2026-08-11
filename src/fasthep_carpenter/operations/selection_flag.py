from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.registry.defaults import default_expr_registry
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

SELECTION_FLAG_SPEC = {
    "name": "hep.selection.flag",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "selection": {"type": "list[string]", "required": True},
        "output": {"type": "string", "required": False},
        "legacy": {"type": "mapping", "required": False},
    },
    "normalize_params": {"stage_id_defaults": {"output": "id"}},
    "result": {
        "kind": "event_stream",
        "description": "Event stream with a materialized boolean selection flag.",
    },
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ]
    },
    "requires": {
        "symbols": [
            {"from": "params.selection.*", "kind": "expr"},
        ]
    },
}


def run_selection_flag(
    data: dict[str, Any],
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> dict[str, Any]:
    stream_id = str(
        params.get("stream_id")
        or ctx.get("primary_stream")
        or DEFAULT_PRIMARY_STREAM_ID
    )
    events = get_stream_array(
        data,
        stream_id,
    )
    output = _required_output(params.get("output"))
    selection = _selection_expressions(params.get("selection"))

    deps = _runtime_dependencies(params, ctx)
    missing = [symbol for symbol in sorted(deps.consumes) if symbol not in events.fields]
    if missing:
        raise KeyError(
            "hep.selection.flag missing required field(s): "
            f"{missing}. Output {output!r} was not materialized."
        )

    flag = None
    for expr in selection:
        current = eval_expr(events, expr, ctx)
        flag = current if flag is None else flag & current

    if flag is None:
        raise ValueError(
            "hep.selection.flag selection must contain at least one expression"
        )

    return {DEFAULT_PRIMARY_STREAM_ID: ak.with_field(events, flag, output)}


def run_selection_flag_transform(
    *,
    stream: Any,
    selection: list[str],
    output: str | None = None,
    ctx: Any = None,
    **kwargs: Any,
) -> Any:
    del kwargs
    stream = unwrap_legacy_data_envelope(stream)
    params = {
        "selection": selection,
        "output": output,
    }
    out = run_selection_flag(
        data=legacy_data_envelope(stream),
        params=params,
        ctx=dict(ctx or {}),
    )
    if ctx is not None and hasattr(ctx, "provenance"):
        deps = _runtime_dependencies(params, dict(ctx or {}))
        resolved_output = _required_output(output)
        ctx.provenance.record_operation(
            inputs={"symbols": sorted(deps.consumes)},
            outputs={"symbols": [resolved_output]},
        )
    return get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)


def _selection_expressions(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not value:
        raise ValueError("hep.selection.flag selection must be a non-empty list")
    expressions = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                "hep.selection.flag selection expressions must be non-empty strings"
            )
        expressions.append(item.strip())
    return expressions


def _required_output(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "hep.selection.flag output must be resolved during workflow normalization"
        )
    return value.strip()


def _runtime_dependencies(
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> DataDependencyResult:
    registry = ctx.get("expr_registry") or default_expr_registry()
    result = DataDependencyResult()
    for expr in _selection_expressions(params.get("selection")):
        result.consumes.update(
            data_symbols_in_expr(
                expr,
                known_functions=set(getattr(registry, "functions", {})),
                known_constants=set(getattr(registry, "constants", {})),
                context_symbols=set(ctx),
                produced=set(),
            )
        )
    return result
