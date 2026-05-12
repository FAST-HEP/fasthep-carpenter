from typing import Any

import awkward as ak
import numpy as np

from fasthep_carpenter.impl.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID


def _wrap_delta_phi(dphi: ak.Array) -> ak.Array:
    two_pi = 2.0 * np.pi
    return ((dphi + np.pi) % two_pi) - np.pi


def run_match_l1t_jets(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    """
    Per event:
      - For each reco jet, find the closest L1 jet in ΔR
      - Matched if minΔR < dr_max
    Outputs added to events:
      - n_matched (per-event int)
      - matched_reco_et (jagged)
      - matched_l1_et (jagged)
      - unmatched_dr (jagged; closest ΔR for reco jets that did not match)
    """
    dr_max = float(params.get("dr_max", 0.4))
    stream_name = (
        params.get("stream") or ctx.get("primary_stream") or DEFAULT_PRIMARY_STREAM_ID
    )
    events: dict[str, ak.Array] = data.get(stream_name)

    reco_et = events[params["reco"]["et"]]
    reco_eta = events[params["reco"]["eta"]]
    reco_phi = events[params["reco"]["phi"]]

    l1_et = events[params["l1"]["et"]]
    l1_eta = events[params["l1"]["eta"]]
    l1_phi = events[params["l1"]["phi"]]

    has_l1 = ak.num(l1_et, axis=1) > 0
    # only consider events with at least one reco jet with ET > 20 GeV
    has_good_reco = ak.any(reco_et > 20, axis=1)
    event_mask = has_l1 & has_good_reco

    events = events[event_mask]

    reco_j = ak.zip({"et": reco_et, "eta": reco_eta, "phi": reco_phi})
    l1_j = ak.zip({"et": l1_et, "eta": l1_eta, "phi": l1_phi})

    reco_j = reco_j[event_mask]
    l1_j = l1_j[event_mask]

    pairs = ak.cartesian({"reco": reco_j, "l1": l1_j}, axis=1, nested=True)
    deta = pairs["reco"]["eta"] - pairs["l1"]["eta"]
    dphi = pairs["reco"]["phi"] - pairs["l1"]["phi"]
    dphi = _wrap_delta_phi(dphi)
    dr = np.sqrt(deta * deta + dphi * dphi)

    order = ak.argsort(dr, axis=2)
    pairs_sorted = pairs[order]
    dr_sorted = dr[order]

    best_pair = pairs_sorted[:, :, 0]  # events -> reco_jets
    best_dr = dr_sorted[:, :, 0]  # events -> reco_jets

    # Keep reco jets whose best match is within dr_max
    has_match = best_dr <= dr_max

    matched_reco_et = best_pair["reco"]["et"][has_match]
    matched_l1_et = best_pair["l1"]["et"][has_match]
    n_matched = ak.num(matched_reco_et, axis=1)

    # For debug histogram in the NoMatch branch:
    # closest ΔR in the event (over all pairs), scalar per event (or None if no pairs)
    dr_flat = ak.flatten(dr, axis=2)  # events -> reco_jets*(l1_jets)
    dr_flat = ak.flatten(dr_flat, axis=1)  # events -> all_pairs
    unmatched_dr = ak.min(dr_flat)  # scalar per event (None if empty)

    # Add columns onto the *same* events record
    out_events = events
    out_events = ak.with_field(out_events, n_matched, "n_matched")
    out_events = ak.with_field(out_events, matched_reco_et, "matched_reco_et")
    out_events = ak.with_field(out_events, matched_l1_et, "matched_l1_et")
    out_events = ak.with_field(out_events, unmatched_dr, "unmatched_dr")

    return {"events": out_events}


def run_match_l1t_jets_transform(
    *,
    stream,
    reco,
    l1,
    dr_max=0.4,
    ctx=None,
    **kwargs,
):
    stream = unwrap_legacy_data_envelope(stream)
    legacy_params = {
        "reco": reco,
        "l1": l1,
        "dr_max": dr_max,
    }

    out = run_match_l1t_jets(
        data=legacy_data_envelope(stream),
        params=legacy_params,
        ctx={
            **dict(ctx or {}),
            "primary_stream": DEFAULT_PRIMARY_STREAM_ID,
        },
        **kwargs,
    )

    return legacy_data_envelope(out.get(DEFAULT_PRIMARY_STREAM_ID, out))
