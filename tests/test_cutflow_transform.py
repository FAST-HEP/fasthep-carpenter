from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID

from fasthep_carpenter.impl.cutflow import run_cutflow


def test_cutflow_transform_preserves_full_selection_stats() -> None:
    events = ak.Array(
        {
            "nMuon": [2, 1, 3],
            "Muon_Pt": [[30.0, 20.0], [26.0], [10.0, 40.0, 50.0]],
            "weight": [1.0, 2.0, 3.0],
        }
    )

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "weight": "weight",
            "selection": {
                "All": [
                    "nMuon >= 2",
                    {"reduce": {"op": "any", "over": "Muon_Pt > 25"}},
                ]
            },
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    cuts = out["cutflow"]["cuts"]
    assert cuts[0] == {
        "name": "All[0]",
        "selection": "All",
        "index": 0,
        "label": "nMuon >= 2",
        "expr": "nMuon >= 2",
        "kind": "expression",
        "n": 2,
        "n_in": 3,
        "n_out": 2,
        "sumw": 4.0,
        "sumw2": 10.0,
        "sumw_in": 6.0,
        "sumw_out": 4.0,
        "sumw2_in": 14.0,
        "sumw2_out": 10.0,
    }
    assert cuts[1]["label"] == "any(Muon_Pt > 25)"
    assert cuts[1]["expr"] == {"reduce": {"op": "any", "over": "Muon_Pt > 25"}}
    assert cuts[1]["n_in"] == 2
    assert cuts[1]["n_out"] == 2


def test_cutflow_transform_supports_branched_selection_groups() -> None:
    events = ak.Array(
        {
            "nMuon": [2, 2, 1],
            "charge": [0, 1, 0],
        }
    )

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "selection": {
                "preselection": ["nMuon >= 2"],
                "signal": {"from": "preselection[0]", "steps": ["charge == 0"]},
                "control": {"from": "preselection[0]", "steps": ["charge != 0"]},
            }
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    cuts_by_name: dict[str, dict[str, Any]] = {
        str(row["name"]): row for row in out["cutflow"]["cuts"]
    }
    assert cuts_by_name["preselection[0]"]["n_out"] == 2
    assert cuts_by_name["signal[0]"]["n_in"] == 2
    assert cuts_by_name["signal[0]"]["n_out"] == 1
    assert cuts_by_name["control[0]"]["n_in"] == 2
    assert cuts_by_name["control[0]"]["n_out"] == 1
