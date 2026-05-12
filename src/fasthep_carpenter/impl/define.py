from typing import Any

import awkward as ak

from fasthep_carpenter.impl.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr
from hepflow.runtime.stream_readers import get_stream_array


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
