from __future__ import annotations

from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult

MATCH_L1T_JETS_OUTPUTS = {
    "n_matched",
    "matched_reco_et",
    "matched_l1_et",
    "unmatched_dr",
}


CMS_MATCH_L1T_JETS_TRANSFORM_SPEC = {
    "name": "cms.match_l1t_jets",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.cms.match_l1t_jets_transform:parse_match_l1t_jets_data_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "reco": {
            "type": "mapping",
            "required": True,
        },
        "l1": {
            "type": "mapping",
            "required": True,
        },
        "dr_max": {
            "type": "number",
            "required": False,
            "default": 0.4,
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with matched reco/L1 jet summary fields.",
    },
}


def match_l1t_jets_output_names(params: dict[str, Any]) -> set[str]:
    return set(MATCH_L1T_JETS_OUTPUTS)


def parse_match_l1t_jets_data_dependencies(
    params: dict[str, Any],
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
) -> DataDependencyResult:
    result = DataDependencyResult(produces=match_l1t_jets_output_names(params))

    for group_name in ("reco", "l1"):
        group = params.get(group_name) or {}
        if not isinstance(group, dict):
            continue
        for field_name in ("et", "eta", "phi"):
            expr = group.get(field_name)
            if expr is None:
                continue
            result.consumes.update(
                data_symbols_in_expr(
                    str(expr),
                    known_functions=known_functions,
                    known_constants=known_constants,
                    context_symbols=context_symbols,
                    produced=set(),
                )
            )

    return result
