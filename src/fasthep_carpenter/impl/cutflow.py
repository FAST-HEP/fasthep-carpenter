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

    selection = params.get("selection", {})
    initial_mask = ak.Array(np.ones(len(events), dtype=np.bool_))
    masks_by_node: dict[str, Any] = {}

    cuts = []
    final_mask = initial_mask
    for selection_name, steps, parents in _selection_groups(selection):
        current = _base_mask(initial_mask, parents, masks_by_node)
        for i, step in enumerate(steps):
            before = current
            current = current & _step_mask(events, step, ctx)
            node_id = f"{selection_name}[{i}]"
            masks_by_node[node_id] = current
            cuts.append(_cut_row(node_id, selection_name, i, step, before, current, w))
        final_mask = current

    filtered = events[final_mask]
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


def _selection_groups(selection: Any) -> list[tuple[str, list[Any], list[str]]]:
    if not isinstance(selection, dict):
        return []
    groups: list[tuple[str, list[Any], list[str]]] = []
    for name, raw in selection.items():
        if isinstance(raw, list):
            groups.append((str(name), raw, []))
            continue
        if not isinstance(raw, dict):
            continue
        steps = raw.get("steps", raw.get("cuts", []))
        if not isinstance(steps, list):
            continue
        parent_value = raw.get("parents", raw.get("from", raw.get("parent", [])))
        groups.append((str(name), steps, _parent_ids(parent_value)))
    return groups


def _parent_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _base_mask(
    initial_mask: Any,
    parents: list[str],
    masks_by_node: dict[str, Any],
) -> Any:
    if not parents:
        return initial_mask
    mask = initial_mask
    for parent in parents:
        if parent not in masks_by_node:
            raise ValueError(f"Unknown parent selection node: {parent}")
        mask = mask & masks_by_node[parent]
    return mask


def _step_mask(events: Any, step: Any, ctx: dict[str, Any]) -> Any:
    if isinstance(step, dict) and "expr" in step:
        return eval_expr(events, str(step["expr"]), ctx)
    if isinstance(step, dict) and "reduce" in step:
        red = step["reduce"]
        op = red.get("op")
        over = str(red.get("over"))
        arr = eval_expr(events, over, ctx)
        if op == "any":
            return ak.any(arr, axis=1)
        if op == "all":
            return ak.all(arr, axis=1)
        raise ValueError(f"Unsupported reduce op in selection: {op}")
    if isinstance(step, str):
        return eval_expr(events, step, ctx)
    raise ValueError(f"Bad selection step: {step}")


def _cut_row(
    node_id: str,
    selection_name: str,
    index: int,
    step: Any,
    before: Any,
    after: Any,
    weights: Any,
) -> dict[str, Any]:
    n_unweighted_in = int(ak.sum(before))
    n_unweighted_out = int(ak.sum(after))
    row = {
        "name": node_id,
        "selection": selection_name,
        "index": index,
        "label": _cut_label(step),
        "expr": _cut_expr(step),
        "kind": _cut_kind(step),
        "n_unweighted_in": n_unweighted_in,
        "n_unweighted_out": n_unweighted_out,
    }
    if weights is None:
        row.update(
            {
                "n": float(n_unweighted_out),
                "n_in": float(n_unweighted_in),
                "n_out": float(n_unweighted_out),
                "sumw": float(n_unweighted_out),
                "sumw2": float(n_unweighted_out),
                "sumw_in": float(n_unweighted_in),
                "sumw_out": float(n_unweighted_out),
                "sumw2_in": float(n_unweighted_in),
                "sumw2_out": float(n_unweighted_out),
            }
        )
        return row

    sumw_in = float(ak.sum(weights[before]))
    sumw_out = float(ak.sum(weights[after]))
    sumw2_in = float(ak.sum(weights[before] * weights[before]))
    sumw2_out = float(ak.sum(weights[after] * weights[after]))
    row.update(
        {
            "n": sumw_out,
            "n_in": sumw_in,
            "n_out": sumw_out,
            "sumw": sumw_out,
            "sumw2": sumw2_out,
            "sumw_in": sumw_in,
            "sumw_out": sumw_out,
            "sumw2_in": sumw2_in,
            "sumw2_out": sumw2_out,
        }
    )
    return row


def _cut_label(step: Any) -> str:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if isinstance(step.get("label"), str):
            return step["label"]
        if "expr" in step:
            return str(step["expr"])
        if isinstance(step.get("reduce"), dict):
            reduce_spec = step["reduce"]
            return f"{reduce_spec.get('op', 'reduce')}({reduce_spec.get('over', '')})"
    return str(step)


def _cut_expr(step: Any) -> Any:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if "expr" in step:
            return step["expr"]
        if "reduce" in step:
            return {"reduce": step["reduce"]}
    return step


def _cut_kind(step: Any) -> str:
    if isinstance(step, dict) and "reduce" in step:
        return "reduce"
    return "expression"
