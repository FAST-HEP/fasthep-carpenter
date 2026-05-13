from __future__ import annotations

from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult

CUTFLOW_TRANSFORM_SPEC = {
    "name": "hep.selection.cutflow",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.cutflow_transform:parse_cutflow_column_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "selection": {
            "type": "mapping",
            "required": True,
        },
        "weight_expr": {
            "type": "string",
            "required": False,
        },
    },
    "result": {
        "stream": {
            "kind": "event_stream",
            "description": "Filtered event stream.",
        },
        "cutflow": {
            "kind": "report",
            "description": "Cutflow summary report.",
        },
    },
}


def parse_cutflow_column_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    result = DataDependencyResult()

    def add_expr(expr: Any) -> None:
        if expr is None:
            return
        result.consumes.update(
            data_symbols_in_expr(
                str(expr),
                known_functions=known_functions,
                known_constants=known_constants,
                context_symbols=context_symbols,
                produced=set(),
            )
        )

    selection = params.get("selection") or {}
    steps = selection.get("All", []) if isinstance(selection, dict) else []
    for step in steps:
        if isinstance(step, str):
            add_expr(step)
            continue
        if not isinstance(step, dict):
            continue
        if "expr" in step:
            add_expr(step["expr"])
        reduce_spec = step.get("reduce")
        if isinstance(reduce_spec, dict):
            add_expr(reduce_spec.get("over"))

    add_expr(params.get("weight_expr"))
    add_expr(params.get("weight"))
    return result
