from __future__ import annotations

from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult

DI_OBJECT_MASS_TRANSFORM_SPEC = {
    "name": "hep.di_object_mass",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.di_object_mass_transform:parse_di_object_mass_data_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "collection": {
            "type": "string",
            "required": False,
            "default": "Muon",
        },
        "mask": {
            "type": "string",
            "required": False,
        },
        "out_var": {
            "type": "string",
            "required": False,
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with a di-object invariant mass field.",
    },
}


def di_object_mass_output_name(params: dict[str, Any]) -> str:
    out_var = params.get("out_var")
    if out_var:
        return str(out_var)
    collection = str(params.get("collection", "Muon"))
    return f"Di{collection}_Mass"


def parse_di_object_mass_data_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    collection = str(params.get("collection") or "Muon")
    result = DataDependencyResult(
        consumes={
            f"{collection}_Px",
            f"{collection}_Py",
            f"{collection}_Pz",
            f"{collection}_E",
        },
        produces={di_object_mass_output_name(params)},
    )

    mask = params.get("mask")
    if mask is not None:
        result.consumes.update(
            data_symbols_in_expr(
                str(mask),
                known_functions=known_functions,
                known_constants=known_constants,
                context_symbols=context_symbols,
                produced=set(),
            )
        )

    return result
