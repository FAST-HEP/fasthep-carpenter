from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.impl.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)


def run_cutflow(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    """
    params:
      weight: optional str column name
      selection:
        All:
          - {expr: "..."}
          - {reduce: {op: any, over: "Muon_Pt > 25"}}
    Returns:
      events: filtered events
      cutflow: {"cuts":[{name, n, sumw, sumw2}, ...]}
    """
    events = data.get(
        ctx["primary_stream"],
        data.get(DEFAULT_PRIMARY_STREAM_ID, next(iter(data.values()))),
    )
    weight_expr = params.get("weight_expr", params.get("weight"))
    if isinstance(weight_expr, str):
        w = events[weight_expr] if weight_expr in events.fields else eval_expr(events, weight_expr, ctx)
    else:
        w = None

    steps = params.get("selection", {}).get("All", [])
    current = ak.Array(np.ones(len(events), dtype=np.bool_))

    cuts = []
    for i, step in enumerate(steps):
        if isinstance(step, dict) and "expr" in step:
            m = eval_expr(events, str(step["expr"]), ctx)
        elif isinstance(step, dict) and "reduce" in step:
            red = step["reduce"]
            op = red.get("op")
            over = str(red.get("over"))
            arr = eval_expr(events, over, ctx)
            if op == "any":
                m = ak.any(arr, axis=1)
            elif op == "all":
                m = ak.all(arr, axis=1)
            else:
                raise ValueError(f"Unsupported reduce op in selection: {op}")
        elif isinstance(step, str):
            m = eval_expr(events, step, ctx)
        else:
            raise ValueError(f"Bad selection step: {step}")

        current = current & m

        n = int(ak.sum(current))
        row = {"name": f"All[{i}]", "n": n}
        if w is not None:
            sw = float(ak.sum(w[current]))
            sw2 = float(ak.sum(w[current] * w[current]))
            row.update({"sumw": sw, "sumw2": sw2})
        cuts.append(row)

    filtered = events[current]
    return {DEFAULT_PRIMARY_STREAM_ID: filtered, "cutflow": {"cuts": cuts}}


def run_cutflow_transform(*, stream, selection, weight_expr=None, ctx=None, **kwargs):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_data = legacy_data_envelope(stream)
    legacy_params = {
        "selection": selection,
    }
    if weight_expr is not None:
        legacy_params["weight_expr"] = weight_expr
    legacy_ctx = ctx or {}
    if "primary_stream" not in legacy_ctx:
        legacy_ctx["primary_stream"] = DEFAULT_PRIMARY_STREAM_ID

    out = run_cutflow(
        data=legacy_data,
        params=legacy_params,
        ctx=legacy_ctx,
        **kwargs,
    )
    return {
        "stream": out[DEFAULT_PRIMARY_STREAM_ID],
        "cutflow": out["cutflow"],
    }
