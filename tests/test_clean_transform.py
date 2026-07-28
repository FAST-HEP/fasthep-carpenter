from __future__ import annotations

import awkward as ak
import pytest

from fasthep_carpenter.operations.clean import run_clean_transform


def test_clean_no_overlap_keeps_all_source_objects() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0]],
            "src_phi": [[0.0, 2.0]],
            "src_pt": [[10.0, 20.0]],
            "tgt_eta": [[1.0]],
            "tgt_phi": [[1.0]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[0.0, 2.0]]
    assert ak.to_list(out["events"]["cleaned_src_pt"]) == [[10.0, 20.0]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [0]


def test_clean_one_overlapping_target_removes_source_object() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0]],
            "src_phi": [[0.0, 2.0]],
            "src_pt": [[10.0, 20.0]],
            "tgt_eta": [[0.1]],
            "tgt_phi": [[0.0]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[2.0]]
    assert ak.to_list(out["events"]["cleaned_src_pt"]) == [[20.0]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [1]


def test_clean_multiple_targets_remove_on_any_overlap() -> None:
    out = run_clean_transform(
        stream=ak.Array(
            {
                "src_eta": [[0.0, 2.0, 4.0]],
                "src_phi": [[0.0, 2.0, 4.0]],
                "src_pt": [[10.0, 20.0, 30.0]],
                "mu_eta": [[9.0]],
                "mu_phi": [[9.0]],
                "ele_eta": [[2.05]],
                "ele_phi": [[2.0]],
            }
        ),
        source="src",
        clean_against=["mu", "ele"],
        min_delta_r=0.4,
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[0.0, 4.0]]
    assert ak.to_list(out["events"]["cleaned_src_pt"]) == [[10.0, 30.0]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [1]


def test_clean_empty_target_collection_removes_nothing() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0]],
            "src_phi": [[0.0, 2.0]],
            "src_pt": [[10.0, 20.0]],
            "tgt_eta": [[]],
            "tgt_phi": [[]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[0.0, 2.0]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [0]


def test_clean_empty_source_collection_is_valid() -> None:
    out = _run(
        {
            "src_eta": [[]],
            "src_phi": [[]],
            "src_pt": [[]],
            "tgt_eta": [[0.0]],
            "tgt_phi": [[0.0]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [0]


def test_clean_multiple_events_stay_event_local() -> None:
    out = _run(
        {
            "src_eta": [[0.0], [2.0]],
            "src_phi": [[0.0], [2.0]],
            "src_pt": [[10.0], [20.0]],
            "tgt_eta": [[], [2.1]],
            "tgt_phi": [[], [2.0]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[0.0], []]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [0, 1]


def test_clean_threshold_boundary_is_strictly_less_than_min_delta_r() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 1.0]],
            "src_phi": [[0.0, 1.0]],
            "src_pt": [[10.0, 20.0]],
            "tgt_eta": [[0.4, 1.399]],
            "tgt_phi": [[0.0, 1.0]],
        },
        min_delta_r=0.4,
    )

    assert ak.to_list(out["events"]["cleaned_src_eta"]) == [[0.0]]
    assert ak.to_list(out["events"]["nremoved_cleaned_src"]) == [1]


def test_clean_preserves_target_fields_unchanged() -> None:
    out = _run(
        {
            "src_eta": [[0.0]],
            "src_phi": [[0.0]],
            "src_pt": [[10.0]],
            "tgt_eta": [[0.1]],
            "tgt_phi": [[0.0]],
            "tgt_label": [[42]],
        }
    )

    assert ak.to_list(out["events"]["tgt_eta"]) == [[0.1]]
    assert ak.to_list(out["events"]["tgt_label"]) == [[42]]


def test_clean_preserves_arbitrary_source_fields() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0]],
            "src_phi": [[0.0, 2.0]],
            "src_pt": [[10.0, 20.0]],
            "src_charge": [[-1, 1]],
            "tgt_eta": [[0.1]],
            "tgt_phi": [[0.0]],
        }
    )

    assert ak.to_list(out["events"]["cleaned_src_charge"]) == [[1]]


def test_clean_sorts_descending_pt_when_requested() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0, 3.0]],
            "src_phi": [[0.0, 2.0, 3.0]],
            "src_pt": [[10.0, 30.0, 20.0]],
            "tgt_eta": [[]],
            "tgt_phi": [[]],
        },
        sort_by="pt",
        sort_order="descending",
    )

    assert ak.to_list(out["events"]["cleaned_src_pt"]) == [[30.0, 20.0, 10.0]]


def test_clean_removed_count_diagnostic_can_be_named() -> None:
    out = _run(
        {
            "src_eta": [[0.0, 2.0]],
            "src_phi": [[0.0, 2.0]],
            "src_pt": [[10.0, 20.0]],
            "tgt_eta": [[0.1]],
            "tgt_phi": [[0.0]],
        },
        diagnostics={"removed_count": "nremoved_src_overlap"},
    )

    assert ak.to_list(out["events"]["nremoved_src_overlap"]) == [1]
    assert "nremoved_cleaned_src" not in out["events"].fields


def test_clean_missing_eta_phi_fails_clearly() -> None:
    with pytest.raises(ValueError, match="missing required delta-R field"):
        _run(
            {
                "src_eta": [[0.0]],
                "src_pt": [[10.0]],
                "tgt_eta": [[0.1]],
                "tgt_phi": [[0.0]],
            }
        )


def test_clean_supports_record_collection_output() -> None:
    stream = ak.Array(
        {
            "src": [[{"eta": 0.0, "phi": 0.0, "pt": 10.0, "tag": 1}]],
            "tgt": [[{"eta": 9.0, "phi": 9.0}]],
        }
    )

    out = run_clean_transform(
        stream=stream,
        source="src",
        clean_against=["tgt"],
        output="cleaned",
    )

    assert ak.to_list(out["events"]["cleaned"]) == [
        [{"eta": 0.0, "phi": 0.0, "pt": 10.0, "tag": 1}]
    ]


def _run(stream: dict, **params):
    return run_clean_transform(
        stream=ak.Array(stream),
        source="src",
        clean_against=["tgt"],
        **params,
    )
