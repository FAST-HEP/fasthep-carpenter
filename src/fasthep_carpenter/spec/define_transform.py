from __future__ import annotations

from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult

DEFINE_TRANSFORM_SPEC = {
    "name": "hep.define",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.define_transform:parse_define_column_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "variables": {
            "type": "list[mapping]",
            "required": True,
            "description": "Variables to define on the stream.",
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with newly defined fields.",
    },
}


def parse_define_column_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    result = DataDependencyResult()
    produced_so_far: set[str] = set()

    for variable in params.get("variables", []) or []:
        if not isinstance(variable, dict):
            continue

        produced_name = variable.get("name")
        if "expr" in variable:
            result.consumes.update(
                data_symbols_in_expr(
                    str(variable["expr"]),
                    known_functions=known_functions,
                    known_constants=known_constants,
                    context_symbols=context_symbols,
                    produced=produced_so_far,
                )
            )

        reduce_spec = variable.get("reduce")
        if isinstance(reduce_spec, dict) and reduce_spec.get("over") is not None:
            result.consumes.update(
                data_symbols_in_expr(
                    str(reduce_spec["over"]),
                    known_functions=known_functions,
                    known_constants=known_constants,
                    context_symbols=context_symbols,
                    produced=produced_so_far,
                )
            )

        if isinstance(produced_name, str) and produced_name:
            produced_so_far.add(produced_name)
            result.produces.add(produced_name)

    return result
