from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.engine import eval_expr

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)

DI_OBJECT_MASS_SPEC = {
    "name": "hep.di_object_mass",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.operations.di_object_mass:parse_di_object_mass_data_dependencies",
    },
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "collection": {"type": "string", "required": False, "default": "Muon"},
        "mask": {"type": "string", "required": False},
        "out_var": {"type": "string", "required": False},
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


def _two_object_mass(px, py, pz, E) -> Any:
    """
    Compute invariant mass for first two objects per event.
    px,py,pz,E are jagged arrays (events x objects).
    Returns per-event mass (float), with NaN where <2 objects.
    """
    # Take first two objects
    px0 = ak.firsts(px[:, 0:1], axis=1)  # first element
    px1 = ak.firsts(px[:, 1:2], axis=1)
    py0 = ak.firsts(py[:, 0:1], axis=1)
    py1 = ak.firsts(py[:, 1:2], axis=1)
    pz0 = ak.firsts(pz[:, 0:1], axis=1)
    pz1 = ak.firsts(pz[:, 1:2], axis=1)
    E0 = ak.firsts(E[:, 0:1], axis=1)
    E1 = ak.firsts(E[:, 1:2], axis=1)

    # If any of these are None (missing because <2 objs), result becomes None
    # We'll fill missing with nan at the end.
    pxs = px0 + px1
    pys = py0 + py1
    pzs = pz0 + pz1
    Es = E0 + E1

    m2 = Es * Es - (pxs * pxs + pys * pys + pzs * pzs)
    m2 = ak.where(m2 < 0, 0, m2)  # numerical protection
    m = np.sqrt(m2)

    # Replace missing with nan (so hist fill ignores if you mask later, or you can mask explicitly)
    return ak.fill_none(m, np.nan)


def run_di_object_mass(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    """
    params:
      collection: Muon|Electron|Jet|Photon (default Muon)
      mask: column name or expression producing boolean jagged mask
      out_var: optional explicit output column name
    """
    events = data.get(
        ctx["primary_stream"],
        data.get(DEFAULT_PRIMARY_STREAM_ID, next(iter(data.values()))),
    )
    collection = params.get("collection") or "Muon"
    mask_expr = params.get("mask")
    out_name = di_object_mass_output_name(params)

    # Branch names are dynamic: Muon_Px, ...
    px = events[f"{collection}_Px"]
    py = events[f"{collection}_Py"]
    pz = events[f"{collection}_Pz"]
    E = events[f"{collection}_E"]

    if mask_expr is not None:
        if isinstance(mask_expr, str) and mask_expr in events.fields:
            mask = events[mask_expr]
        else:
            mask = eval_expr(events, str(mask_expr), ctx)
        # apply mask to object arrays
        px = px[mask]
        py = py[mask]
        pz = pz[mask]
        E = E[mask]

    m = _two_object_mass(px, py, pz, E)
    out = ak.with_field(events, m, out_name)
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_di_object_mass_transform(
    *, stream, collection="Muon", mask=None, out_var=None, ctx=None, **kwargs
):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_params = {
        "collection": collection,
    }
    if mask is not None:
        legacy_params["mask"] = mask
    if out_var is not None:
        legacy_params["out_var"] = out_var

    legacy_ctx = ctx or {}
    if "primary_stream" not in legacy_ctx:
        legacy_ctx["primary_stream"] = DEFAULT_PRIMARY_STREAM_ID

    out = run_di_object_mass(
        data=legacy_data_envelope(stream),
        params=legacy_params,
        ctx=legacy_ctx,
        **kwargs,
    )
    return legacy_data_envelope(out.get(DEFAULT_PRIMARY_STREAM_ID, out))
