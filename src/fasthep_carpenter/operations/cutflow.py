from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from awkward.types import ArrayType, NumpyType
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.registry.defaults import default_expr_registry
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)

CUTFLOW_SPEC = {
    "name": "hep.selection.cutflow",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "selection": {"type": "mapping", "required": True},
        "weight_expr": {"type": "string", "required": False},
        "output_field": {"type": "string", "required": False},
        "filter": {"type": "boolean", "required": False},
    },
    "result": {
        "stream": {"kind": "event_stream", "description": "Filtered event stream."},
        "cutflow": {"kind": "cutflow", "description": "Cutflow product."},
    },
    "requires": {
        "symbols": [
            {"from": "params.selection", "kind": "cutflow"},
            {"from": "params.weight_expr", "kind": "expr"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.output_field", "kind": "field_list"},
        ]
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
    if isinstance(selection, dict):
        for _, steps, _ in _selection_groups(selection):
            for step in steps:
                if isinstance(step, str):
                    add_expr(step)
                elif isinstance(step, dict):
                    add_expr(step.get("expr"))
                    reduce_spec = step.get("reduce")
                    if isinstance(reduce_spec, dict):
                        add_expr(reduce_spec.get("over"))

    add_expr(params.get("weight_expr"))
    add_expr(params.get("weight"))
    return result


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
            step_mask = _step_mask(events, step, ctx, n_events=len(events))
            current = current & step_mask
            node_id = f"{selection_name}[{i}]"
            masks_by_node[node_id] = current
            cuts.append(_cut_row(node_id, selection_name, i, step, before, current, w))
        final_mask = current

    output_field = params.get("output_field")
    if isinstance(output_field, str) and output_field:
        events = ak.with_field(events, final_mask, output_field)

    should_filter = bool(params.get("filter", True))
    filtered = events[final_mask] if should_filter else events
    return {DEFAULT_PRIMARY_STREAM_ID: filtered, "cutflow": {"cuts": cuts}}


def run_cutflow_transform(
    *,
    stream,
    selection,
    weight_expr=None,
    output_field=None,
    ctx=None,
    **kwargs,
):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_data = legacy_data_envelope(stream)
    legacy_params = {
        "selection": selection,
    }
    if weight_expr is not None:
        legacy_params["weight_expr"] = weight_expr
    if output_field is not None:
        legacy_params["output_field"] = output_field
    legacy_params["filter"] = kwargs.pop("filter", True)
    legacy_ctx = ctx or {}
    if "primary_stream" not in legacy_ctx:
        legacy_ctx["primary_stream"] = DEFAULT_PRIMARY_STREAM_ID

    out = run_cutflow(
        data=legacy_data,
        params=legacy_params,
        ctx=legacy_ctx,
    )
    if ctx is not None and hasattr(ctx, "provenance"):
        deps = _runtime_dependencies(legacy_params, dict(ctx or {}))
        output_symbols = []
        if output_field:
            output_symbols.append(str(output_field))
        ctx.provenance.record_operation(
            inputs={"symbols": sorted(deps.consumes)},
            outputs={"symbols": output_symbols},
        )
    return {
        "stream": out[DEFAULT_PRIMARY_STREAM_ID],
        "cutflow": out["cutflow"],
    }


def _runtime_dependencies(
    params: dict[str, Any],
    ctx: dict[str, Any],
) -> DataDependencyResult:
    registry = ctx.get("expr_registry") or default_expr_registry()
    return parse_cutflow_column_dependencies(
        params,
        known_functions=set(getattr(registry, "functions", {})),
        known_constants=set(getattr(registry, "constants", {})),
        context_symbols=set(ctx),
    )


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


def _step_mask(events: Any, step: Any, ctx: dict[str, Any], *, n_events: int) -> Any:
    if isinstance(step, dict) and "expr" in step:
        return _validate_event_mask(
            eval_expr(events, str(step["expr"]), ctx),
            n_events=n_events,
            step=step,
        )
    if isinstance(step, dict) and "reduce" in step:
        red = step["reduce"]
        op = red.get("op")
        over = str(red.get("over"))
        arr = eval_expr(events, over, ctx)
        if op == "any":
            return _validate_event_mask(
                ak.any(arr, axis=1),
                n_events=n_events,
                step=step,
            )
        if op == "all":
            return _validate_event_mask(
                ak.all(arr, axis=1),
                n_events=n_events,
                step=step,
            )
        raise ValueError(f"Unsupported reduce op in selection: {op}")
    if isinstance(step, str):
        return _validate_event_mask(
            eval_expr(events, step, ctx),
            n_events=n_events,
            step=step,
        )
    raise ValueError(f"Bad selection step: {step}")


def _validate_event_mask(mask: Any, *, n_events: int, step: Any) -> Any:
    mask_type = ak.type(mask)
    try:
        outer_length = len(mask)
    except TypeError as exc:
        raise _event_mask_error(step, mask_type) from exc

    content = mask_type.content if isinstance(mask_type, ArrayType) else None
    if (
        outer_length == n_events
        and isinstance(content, NumpyType)
        and content.primitive == "bool"
    ):
        return mask

    raise _event_mask_error(step, mask_type)


def _event_mask_error(step: Any, mask_type: Any) -> ValueError:
    return ValueError(
        f"Selection expression {_mask_expr(step)!r} produced a non-event-level "
        f"mask with type {str(mask_type)!r}.\n\n"
        "hep.selection.cutflow expressions must produce one boolean per event "
        "('N * bool'). For an object-level expression use an explicit "
        "`reduce: {op: any|all, over: ...}` or convert the quantity to an "
        "event-level field upstream."
    )


def _mask_expr(step: Any) -> Any:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if "expr" in step:
            return step["expr"]
        reduce_spec = step.get("reduce")
        if isinstance(reduce_spec, dict):
            return f"{reduce_spec.get('op', 'reduce')}({reduce_spec.get('over', '')})"
    return step


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
