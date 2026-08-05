from __future__ import annotations

import awkward as ak
import pytest

from fasthep_carpenter.operations.selection_flag import run_selection_flag_transform


def test_selection_flag_materializes_boolean_without_filtering() -> None:
    out = run_selection_flag_transform(
        stream=ak.Array({"pt": [10.0, 3.0, 12.0], "event": [1, 2, 3]}),
        selection=["pt > 5"],
        output="pass_pt",
    )

    assert len(out) == 3
    assert ak.to_list(out["event"]) == [1, 2, 3]
    assert ak.to_list(out["pass_pt"]) == [True, False, True]


def test_selection_flag_combines_multiple_predicates_with_and() -> None:
    out = run_selection_flag_transform(
        stream=ak.Array({"pt": [10.0, 3.0, 12.0], "eta": [0.5, 0.2, 3.0]}),
        selection=["pt > 5", "abs(eta) < 2.4"],
        output="pass_selection",
    )

    assert ak.to_list(out["pass_selection"]) == [True, False, False]


def test_selection_flag_uses_explicit_collection_count_product() -> None:
    out = run_selection_flag_transform(
        stream=ak.Array(
            {
                "ncleaned_veto_Electron": [0, 1, 2],
                "cleaned_veto_Electron_pt": [[], [30.0], [25.0, 20.0]],
            }
        ),
        selection=["ncleaned_veto_Electron == 0"],
        output="veto_Electron_veto_selection",
    )

    assert ak.to_list(out["veto_Electron_veto_selection"]) == [True, False, False]


def test_selection_flag_missing_count_field_fails_clearly() -> None:
    with pytest.raises(KeyError, match="ncleaned_veto_Electron"):
        run_selection_flag_transform(
            stream=ak.Array({"event": [1]}),
            selection=["ncleaned_veto_Electron == 0"],
            output="veto_Electron_veto_selection",
        )


def test_selection_flag_does_not_accept_count_function_sugar() -> None:
    with pytest.raises(KeyError, match="count"):
        run_selection_flag_transform(
            stream=ak.Array({"ncleaned_veto_Electron": [0]}),
            selection=["count(cleaned_veto_Electron) == 0"],
            output="veto_Electron_veto_selection",
        )


def test_selection_flag_requires_normalized_output() -> None:
    with pytest.raises(ValueError, match="output must be resolved"):
        run_selection_flag_transform(
            stream=ak.Array({"pt": [10.0]}),
            selection=["pt > 5"],
        )


def test_selection_flag_records_runtime_provenance() -> None:
    ctx = _Context()

    run_selection_flag_transform(
        stream=ak.Array({"ncleaned_veto_Electron": [0, 1]}),
        selection=["ncleaned_veto_Electron == 0"],
        output="veto_Electron_veto_selection",
        ctx=ctx,
    )

    assert ctx.provenance.records == [
        {
            "inputs": {"symbols": ["ncleaned_veto_Electron"]},
            "outputs": {"symbols": ["veto_Electron_veto_selection"]},
        }
    ]


def test_selection_flag_supports_multiple_independent_vetoes() -> None:
    stream = ak.Array(
        {
            "nselected_tight_Muon": [0, 1, 0],
            "ncleaned_loose_Tau": [0, 0, 2],
        }
    )

    out = run_selection_flag_transform(
        stream=stream,
        selection=["nselected_tight_Muon == 0"],
        output="tight_Muon_veto_selection",
    )
    out = run_selection_flag_transform(
        stream=out,
        selection=["ncleaned_loose_Tau == 0"],
        output="loose_Tau_veto_selection",
    )

    assert ak.to_list(out["tight_Muon_veto_selection"]) == [True, False, True]
    assert ak.to_list(out["loose_Tau_veto_selection"]) == [True, True, False]
    assert ak.to_list(out["nselected_tight_Muon"]) == [0, 1, 0]
    assert ak.to_list(out["ncleaned_loose_Tau"]) == [0, 0, 2]


def test_selection_flag_can_consume_previous_boolean_flag() -> None:
    out = run_selection_flag_transform(
        stream=ak.Array({"ncleaned_veto_Electron": [0, 1]}),
        selection=["ncleaned_veto_Electron == 0"],
        output="veto_Electron_veto_selection",
    )
    out = run_selection_flag_transform(
        stream=out,
        selection=["veto_Electron_veto_selection"],
        output="downstream_selection",
    )

    assert ak.to_list(out["downstream_selection"]) == [True, False]


class _Provenance:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def record_operation(self, **record) -> None:
        self.records.append(record)


class _Context:
    def __init__(self) -> None:
        self.provenance = _Provenance()

    def __iter__(self):
        return iter(())
