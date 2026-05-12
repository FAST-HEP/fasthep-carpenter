from typing import Any

import awkward as ak
import hist

from fasthep_carpenter.impl.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr
from hepflow.runtime.stream_readers import get_stream_array


def _make_hist(axes: list[dict[str, Any]], storage: str) -> hist.Hist:
    h_axes = []
    for ax in axes:
        t = ax["type"]
        name = ax["name"]
        if t == "category":
            bins = ax.get("bins", None)
            if isinstance(bins, list):
                h_axes.append(
                    hist.axis.StrCategory([str(x) for x in bins], name=name, growth=False)
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
    for ax in axes:
        src = ax["source"]
        name = ax["name"]
        if src == "dataset_name":
            fill_kwargs[name] = ctx["dataset_name"]
            if ax.get("bins") is None:
                ax["bins"] = list(ctx.get("dataset_names") or [ctx["dataset_name"]])
        else:
            fill_kwargs[name] = ak.flatten(events[src], axis=None)

    weight_arr = None

    if storage == "weighted":
        if not (isinstance(weight_expr, str) and weight_expr.strip()):
            raise ValueError(
                "hep.hist storage='weighted' requires non-empty weight_expr"
            )
        weight_arr = eval_expr(events, weight_expr, ctx)
        fill_kwargs["weight"] = ak.flatten(weight_arr, axis=None)

    h = _make_hist(axes, storage=storage)
    h.fill(**fill_kwargs)
    return {"hist": h}


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
