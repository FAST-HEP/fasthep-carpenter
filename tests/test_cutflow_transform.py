from __future__ import annotations

from typing import Any

import awkward as ak
import pytest
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID

from fasthep_carpenter.operations.cutflow import run_cutflow, run_cutflow_transform


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
        "n_unweighted_in": 3,
        "n_unweighted_out": 2,
        "n": 4.0,
        "n_in": 6.0,
        "n_out": 4.0,
        "sumw": 4.0,
        "sumw2": 10.0,
        "sumw_in": 6.0,
        "sumw_out": 4.0,
        "sumw2_in": 14.0,
        "sumw2_out": 10.0,
    }
    assert cuts[1]["label"] == "any(Muon_Pt > 25)"
    assert cuts[1]["expr"] == {"reduce": {"op": "any", "over": "Muon_Pt > 25"}}
    assert cuts[1]["n_unweighted_in"] == 2
    assert cuts[1]["n_unweighted_out"] == 2
    assert cuts[1]["n_in"] == 4.0
    assert cuts[1]["n_out"] == 4.0


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
    assert cuts_by_name["preselection[0]"]["n_unweighted_out"] == 2
    assert cuts_by_name["signal[0]"]["n_in"] == 2
    assert cuts_by_name["signal[0]"]["n_out"] == 1
    assert cuts_by_name["signal[0]"]["n_unweighted_in"] == 2
    assert cuts_by_name["signal[0]"]["n_unweighted_out"] == 1
    assert cuts_by_name["control[0]"]["n_in"] == 2
    assert cuts_by_name["control[0]"]["n_out"] == 1


def test_cutflow_transform_can_materialize_final_selection_without_filtering() -> None:
    events = ak.Array(
        {
            "Flag_A": [True, True, False, True],
            "Flag_B": [True, False, True, False],
            "Flag_C": [True, True, False, False],
        }
    )

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "filter": False,
            "output_field": "Noise_filter_selection",
            "selection": {"Noise_filter_selection": ["Flag_A", "Flag_B", "Flag_C"]},
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    selected = ak.to_list(out[DEFAULT_PRIMARY_STREAM_ID]["Noise_filter_selection"])
    assert selected == [True, False, False, False]
    assert len(out[DEFAULT_PRIMARY_STREAM_ID]) == 4
    assert out["cutflow"]["cuts"][-1]["selection"] == "Noise_filter_selection"


def test_cutflow_transform_defaults_to_filtering_after_materializing_selection() -> None:
    events = ak.Array(
        {
            "Flag_A": [True, True, False],
            "Flag_B": [True, False, True],
        }
    )

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "output_field": "Noise_filter_selection",
            "selection": {"Noise_filter_selection": ["Flag_A", "Flag_B"]},
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    selected = ak.to_list(out[DEFAULT_PRIMARY_STREAM_ID]["Noise_filter_selection"])
    assert selected == [True]


def test_cutflow_transform_missing_required_field_fails_clearly() -> None:
    events = ak.Array({"Flag_A": [True, False]})

    with pytest.raises(Exception, match="Flag_B"):
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={
                "filter": False,
                "output_field": "Noise_filter_selection",
                "selection": {"Noise_filter_selection": ["Flag_A", "Flag_B"]},
            },
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )


def test_cutflow_runtime_wrapper_ignores_metadata_kwargs() -> None:
    events = ak.Array({"Flag_A": [True, False]})

    out = run_cutflow_transform(
        stream=events,
        selection={"Noise_filter_selection": ["Flag_A"]},
        output_field="Noise_filter_selection",
        filter=False,
        legacy={"module": "NoiseFilterSelection"},
    )

    assert ak.to_list(out["stream"]["Noise_filter_selection"]) == [True, False]
