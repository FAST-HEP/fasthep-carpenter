from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

DEFINE_SPEC = {
    "name": "hep.define",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.operations.define:parse_define_column_dependencies",
    },
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {"variables": {"type": "list[mapping]", "required": True}},
    "result": {
        "kind": "event_stream",
        "description": "Event stream with newly defined fields.",
    },
    "requires": {
        "symbols": [
            {"from": "params.variables.*.expr", "kind": "expr"},
            {"from": "params.variables.*.reduce.over", "kind": "expr_or_field"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.variables.*.name", "kind": "field_list"},
        ]
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


def run_define(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    events = get_stream_array(
        data, ctx.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID)
    )
    out = events

    for v in params.get("variables", []):
        name = v["name"]

        if "expr" in v:
            value = eval_expr(out, str(v["expr"]), ctx)
            out = ak.with_field(out, value, name)
            continue

        red = v.get("reduce")
        if isinstance(red, dict):
            op = red.get("op")
            over = red.get("over")
            if not isinstance(over, str):
                raise ValueError(f"reduce.over must be string, got {type(over)}")

            arr = eval_expr(out, over, ctx) if over not in out.fields else out[over]

            if op == "count_nonzero":
                value = ak.sum(arr != 0, axis=1)
            elif op == "any":
                value = ak.any(arr, axis=1)
            elif op == "all":
                value = ak.all(arr, axis=1)
            else:
                raise ValueError(f"Unknown reduce op: {op}")

            out = ak.with_field(out, value, name)
            continue

        raise ValueError(f"Unknown variable spec: {v}")

    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_define_transform(*, stream, variables, ctx=None, **kwargs):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_data = legacy_data_envelope(stream)
    legacy_params = {
        "variables": variables,
    }
    legacy_ctx = dict(ctx or {})

    out = run_define(
        data=legacy_data,
        params=legacy_params,
        ctx=legacy_ctx,
        **kwargs,
    )
    return {"events": get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)}
