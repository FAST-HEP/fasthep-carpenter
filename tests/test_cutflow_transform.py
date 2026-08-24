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


def test_cutflow_transform_accepts_event_level_boolean_expression() -> None:
    events = ak.Array({"Flag_A": [True, False, True, False]})

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={"selection": {"All": ["Flag_A"]}},
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    assert ak.to_list(out[DEFAULT_PRIMARY_STREAM_ID]["Flag_A"]) == [True, True]
    assert out["cutflow"]["cuts"][0]["n_unweighted_out"] == 2


def test_cutflow_transform_rejects_jagged_expression_mask() -> None:
    events = ak.Array({"Muon_Pt": [[30.0], [20.0], [], [40.0, 10.0]]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["Muon_Pt > 25"]}},
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )

    message = str(excinfo.value)
    assert "Selection expression 'Muon_Pt > 25'" in message
    assert "4 * var * bool" in message
    assert "one boolean per event ('N * bool')" in message
    assert "reduce: {op: any|all, over: ...}" in message


def test_cutflow_transform_rejects_union_of_event_and_jagged_masks() -> None:
    events = ak.Array({"event": [1, 2, 3]})
    mixed_mask = ak.concatenate([ak.Array([True]), ak.Array([[False], []])], axis=0)

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["mixed_mask"]}},
            ctx={
                "primary_stream": DEFAULT_PRIMARY_STREAM_ID,
                "mixed_mask": mixed_mask,
            },
        )

    message = str(excinfo.value)
    assert "Selection expression 'mixed_mask'" in message
    assert "3 * union[bool, var * bool]" in message


def test_cutflow_transform_rejects_optional_jagged_expression_mask() -> None:
    events = ak.Array({"Muon_Pass": [[True], None, []]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["Muon_Pass"]}},
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )

    message = str(excinfo.value)
    assert "Selection expression 'Muon_Pass'" in message
    assert "3 * option[var * bool]" in message


def test_cutflow_transform_rejects_wrong_outer_mask_length() -> None:
    events = ak.Array({"event": [1, 2, 3]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["short_mask"]}},
            ctx={
                "primary_stream": DEFAULT_PRIMARY_STREAM_ID,
                "short_mask": ak.Array([True, False]),
            },
        )

    message = str(excinfo.value)
    assert "Selection expression 'short_mask'" in message
    assert "2 * bool" in message


def test_cutflow_transform_rejects_scalar_boolean_mask() -> None:
    events = ak.Array({"event": [1, 2, 3]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["True"]}},
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )

    message = str(excinfo.value)
    assert "Selection expression 'True'" in message
    assert "type 'bool'" in message


def test_cutflow_transform_rejects_numeric_mask() -> None:
    events = ak.Array({"Flag_A": [1, 0, 1]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={"selection": {"All": ["Flag_A"]}},
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )

    message = str(excinfo.value)
    assert "Selection expression 'Flag_A'" in message
    assert "3 * int64" in message


def test_cutflow_transform_accepts_explicit_reduce_any_over_jagged_bool() -> None:
    events = ak.Array({"Muon_Pass": [[True], [False], [], [True, False]]})

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "selection": {"All": [{"reduce": {"op": "any", "over": "Muon_Pass"}}]}
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    assert ak.to_list(out[DEFAULT_PRIMARY_STREAM_ID]["Muon_Pass"]) == [
        [True],
        [True, False],
    ]
    assert out["cutflow"]["cuts"][0]["n_unweighted_out"] == 2


def test_cutflow_transform_accepts_explicit_reduce_all_over_jagged_bool() -> None:
    events = ak.Array({"Muon_Pass": [[True], [False], [], [True, False]]})

    out = run_cutflow(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params={
            "selection": {"All": [{"reduce": {"op": "all", "over": "Muon_Pass"}}]}
        },
        ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
    )

    assert ak.to_list(out[DEFAULT_PRIMARY_STREAM_ID]["Muon_Pass"]) == [
        [True],
        [],
    ]
    assert out["cutflow"]["cuts"][0]["n_unweighted_out"] == 2


def test_cutflow_transform_rejects_malformed_reduced_mask() -> None:
    events = ak.Array({"Muon_Pass": [[[True]], [[False, True]], []]})

    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        run_cutflow(
            data={DEFAULT_PRIMARY_STREAM_ID: events},
            params={
                "selection": {"All": [{"reduce": {"op": "any", "over": "Muon_Pass"}}]}
            },
            ctx={"primary_stream": DEFAULT_PRIMARY_STREAM_ID},
        )

    message = str(excinfo.value)
    assert "Selection expression 'any(Muon_Pass)'" in message
    assert "3 * var * bool" in message


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


def test_cutflow_runtime_wrapper_records_symbol_provenance() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.operations: list[dict[str, Any]] = []

        def record_operation(self, **kwargs: Any) -> None:
            self.operations.append(kwargs)

    class RuntimeContext(dict[str, Any]):
        @property
        def provenance(self) -> Recorder:
            return self["provenance"]

    events = ak.Array(
        {
            "Flag_A": [True, False, False],
            "Flag_B": [False, True, False],
        }
    )
    recorder = Recorder()
    ctx = RuntimeContext({"provenance": recorder})

    run_cutflow_transform(
        stream=events,
        selection={"FilterRecoil_selection": ["(Flag_A) | (Flag_B)"]},
        output_field="FilterRecoil_selection",
        filter=True,
        ctx=ctx,
    )

    assert recorder.operations == [
        {
            "inputs": {"symbols": ["Flag_A", "Flag_B"]},
            "outputs": {"symbols": ["FilterRecoil_selection"]},
        }
    ]
