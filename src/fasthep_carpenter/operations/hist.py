from __future__ import annotations

from typing import Any

import awkward as ak
import hist
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr
from hepflow.runtime.stream_readers import get_stream_array

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)

HIST_SPEC = {
    "name": "hep.hist",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.operations.hist:parse_hist_column_dependencies",
    },
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "axes": {"type": "list[mapping]", "required": True},
        "weight_expr": {"type": "string", "required": False},
        "dataset_axis": {"type": "mapping", "required": False},
        "storage": {"type": "string", "required": False, "default": "count"},
    },
    "result": {"kind": "histogram", "description": "Filled histogram object."},
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


def _make_hist(axes: list[dict[str, Any]], storage: str) -> hist.Hist:
    h_axes: list[Any] = []
    for ax in axes:
        t = ax["type"]
        name = ax["name"]
        if t == "category":
            bins = ax.get("bins", None)
            if isinstance(bins, list):
                h_axes.append(
                    hist.axis.StrCategory(
                        [str(x) for x in bins], name=name, growth=False
                    )
                )
            else:
                h_axes.append(hist.axis.StrCategory([], growth=True, name=name))
        elif t == "int":
            h_axes.append(hist.axis.IntCategory([], growth=True, name=name))
        elif t == "bool":
            h_axes.append(hist.axis.IntCategory([0, 1], name=name))
        elif t == "regular":
            b = ax["bins"]
            h_axes.append(
                hist.axis.Regular(
                    int(b["nbins"]), float(b["low"]), float(b["high"]), name=name
                )
            )
        else:
            raise ValueError(f"Unknown axis type: {t}")

    st = hist.storage.Weight() if storage == "weighted" else hist.storage.Double()
    return hist.Hist(*h_axes, storage=st)


def run_make_hist(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    events = get_stream_array(
        data, ctx.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID)
    )
    axes = params["axes"]
    storage = params.get("storage", "count")
    weight_expr = params.get("weight_expr")
    fill_kwargs = {}
    axis_arrays: dict[str, Any] = {}
    jagged_reference: Any | None = None

    for ax in axes:
        src = ax["source"]
        name = ax["name"]
        if src == "dataset_name":
            continue
        values = events[src]
        axis_arrays[name] = values
        if jagged_reference is None and _is_jagged_array(values):
            jagged_reference = values

    for ax in axes:
        src = ax["source"]
        name = ax["name"]
        if src == "dataset_name":
            if ax.get("bins") is None:
                ax["bins"] = list(ctx.get("dataset_names") or [ctx["dataset_name"]])
            fill_kwargs[name] = _dataset_axis_values(
                ctx.get("dataset_name"),
                jagged_reference=jagged_reference,
            )
        else:
            values = axis_arrays[name]
            if jagged_reference is not None:
                values = _broadcast_axis_to_reference(
                    values,
                    jagged_reference,
                    source=str(src),
                )
            fill_kwargs[name] = _flatten_for_hist(values)

    weight_arr = None

    if storage == "weighted":
        if not (isinstance(weight_expr, str) and weight_expr.strip()):
            raise ValueError(
                "hep.hist storage='weighted' requires non-empty weight_expr"
            )
        weight_arr = eval_expr(events, weight_expr, ctx)
        if jagged_reference is not None:
            weight_arr = _broadcast_weight_to_values(
                weight_arr,
                jagged_reference,
                weight_expr=weight_expr,
            )
        fill_kwargs["weight"] = _flatten_for_hist(weight_arr)

    h = _make_hist(axes, storage=storage)
    h.fill(**fill_kwargs)
    return {"hist": h}


def _is_jagged_array(value: Any) -> bool:
    try:
        return ak.Array(value).ndim > 1
    except Exception:
        return False


def _broadcast_weight_to_values(
    weight: Any, reference: Any, *, weight_expr: str | None = None
) -> Any:
    try:
        broadcast_weight, _ = ak.broadcast_arrays(weight, reference)
    except Exception as exc:
        raise ValueError(
            "hep.hist could not broadcast weight expression "
            f"{weight_expr!r} to jagged histogram values. "
            f"weight type={_type_summary(weight)}, reference type={_type_summary(reference)}"
        ) from exc
    return broadcast_weight


def _broadcast_axis_to_reference(value: Any, reference: Any, *, source: str) -> Any:
    try:
        broadcast_value, _ = ak.broadcast_arrays(value, reference)
    except Exception as exc:
        raise ValueError(
            "hep.hist could not broadcast axis source "
            f"{source!r} to the jagged histogram reference. "
            f"axis type={_type_summary(value)}, reference type={_type_summary(reference)}"
        ) from exc
    return broadcast_value


def _flatten_for_hist(value: Any) -> Any:
    return ak.flatten(value, axis=None)


def _dataset_axis_values(
    dataset_name: Any, *, jagged_reference: Any | None
) -> Any:
    if jagged_reference is None:
        return dataset_name
    counts = ak.num(jagged_reference, axis=1)
    return _flatten_for_hist(ak.unflatten([dataset_name] * int(ak.sum(counts)), counts))


def _type_summary(value: Any) -> str:
    try:
        return str(ak.Array(value).type)
    except Exception:
        return type(value).__name__


def run_hist_transform(
    *,
    stream,
    axes,
    weight_expr=None,
    dataset_axis=None,
    storage="count",
    ctx=None,
    **kwargs,
):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_axes = list(axes)
    if dataset_axis is not None:
        legacy_axes.append(dataset_axis)
    legacy_params = {
        "axes": legacy_axes,
        "storage": storage,
    }
    if weight_expr is not None:
        legacy_params["weight_expr"] = weight_expr

    out = run_make_hist(
        data=legacy_data_envelope(stream),
        params=legacy_params,
        ctx=ctx or {},
        **kwargs,
    )
    return out["hist"]
