from __future__ import annotations

from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult

HIST_TRANSFORM_SPEC = {
    "name": "hep.hist",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.hist_transform:parse_hist_column_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "axes": {
            "type": "list[mapping]",
            "required": True,
        },
        "weight_expr": {
            "type": "string",
            "required": False,
        },
        "dataset_axis": {
            "type": "mapping",
            "required": False,
        },
        "storage": {
            "type": "string",
            "required": False,
            "default": "count",
        },
    },
    "result": {
        "kind": "histogram",
        "description": "Filled histogram object.",
    },
}


def parse_hist_column_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    result = DataDependencyResult()

    axes = list(params.get("axes") or [])
    dataset_axis = params.get("dataset_axis")
    if isinstance(dataset_axis, dict):
        axes.append(dataset_axis)

    for axis in axes:
        if not isinstance(axis, dict):
            continue
        source = axis.get("source")
        if source in (None, "dataset_name"):
            continue
        result.consumes.update(
            data_symbols_in_expr(
                str(source),
                known_functions=known_functions,
                known_constants=known_constants,
                context_symbols=context_symbols,
                produced=set(),
            )
        )

    weight_expr = params.get("weight_expr")
    if weight_expr is not None:
        result.consumes.update(
            data_symbols_in_expr(
                str(weight_expr),
                known_functions=known_functions,
                known_constants=known_constants,
                context_symbols=context_symbols,
                produced=set(),
            )
        )

    return result
