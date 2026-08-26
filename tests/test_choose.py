from __future__ import annotations

from typing import Any

import awkward as ak
import pytest
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID

from fasthep_carpenter.expr_helpers import leading
from fasthep_carpenter.operations.choose import (
    CHOOSE_SPEC,
    run_choose,
    run_choose_transform,
)


def test_choose_selects_mutually_exclusive_cases() -> None:
    out = _run_choose(
        ak.Array(
            {
                "is_a": [True, False, False],
                "is_b": [False, True, False],
                "a_pt": [10.0, 11.0, 12.0],
                "b_pt": [20.0, 21.0, 22.0],
            }
        ),
        {
            "cases": [
                {"name": "a", "when": "is_a", "values": {"pt": "a_pt"}},
                {"name": "b", "when": "is_b", "values": {"pt": "b_pt"}},
            ],
            "default": {"pt": "-999.0"},
            "on_no_match": "default",
        },
    )

    assert ak.to_list(out["pt"]) == [10.0, 21.0, -999.0]


def test_choose_keeps_multiple_outputs_coherent() -> None:
    out = _run_choose(
        ak.Array(
            {
                "is_a": [False, True],
                "is_b": [True, False],
                "a_pt": [10.0, 11.0],
                "a_phi": [0.1, 0.2],
                "b_pt": [20.0, 21.0],
                "b_phi": [1.1, 1.2],
            }
        ),
        {
            "cases": [
                {
                    "name": "a",
                    "when": "is_a",
                    "values": {"pt": "a_pt", "phi": "a_phi"},
                },
                {
                    "name": "b",
                    "when": "is_b",
                    "values": {"pt": "b_pt", "phi": "b_phi"},
                },
            ]
        },
    )

    assert ak.to_list(out["pt"]) == [20.0, 11.0]
    assert ak.to_list(out["phi"]) == [1.1, 0.2]


def test_choose_no_match_error_fails_clearly() -> None:
    with pytest.raises(ValueError, match="matching no cases") as excinfo:
        _run_choose(
            ak.Array({"is_a": [False, True], "a_pt": [1.0, 2.0]}),
            {"cases": [{"name": "a", "when": "is_a", "values": {"pt": "a_pt"}}]},
            ctx={"node_id": "stage.ChooseThing"},
        )

    assert "ChooseThing" in str(excinfo.value)
    assert "First no-match event: 0" in str(excinfo.value)


def test_choose_multiple_match_error_reports_cases() -> None:
    with pytest.raises(ValueError, match="matching multiple cases") as excinfo:
        _run_choose(
            ak.Array(
                {
                    "is_a": [True, False, True],
                    "is_b": [True, True, False],
                    "a_pt": [1.0, 2.0, 3.0],
                    "b_pt": [4.0, 5.0, 6.0],
                }
            ),
            {
                "cases": [
                    {"name": "a", "when": "is_a", "values": {"pt": "a_pt"}},
                    {"name": "b", "when": "is_b", "values": {"pt": "b_pt"}},
                ]
            },
            ctx={"node_id": "stage.ChooseThing"},
        )

    message = str(excinfo.value)
    assert "ChooseThing" in message
    assert "1 events" in message
    assert "event 0 matched: a, b" in message


def test_choose_multiple_match_first_uses_authored_order() -> None:
    out = _run_choose(
        ak.Array(
            {
                "is_a": [True, False],
                "is_b": [True, True],
                "a_pt": [1.0, 2.0],
                "b_pt": [4.0, 5.0],
            }
        ),
        {
            "cases": [
                {"name": "a", "when": "is_a", "values": {"pt": "a_pt"}},
                {"name": "b", "when": "is_b", "values": {"pt": "b_pt"}},
            ],
            "on_multiple": "first",
        },
    )

    assert ak.to_list(out["pt"]) == [1.0, 5.0]


def test_choose_rejects_malformed_when() -> None:
    with pytest.raises(ValueError, match="non-event-level mask") as excinfo:
        _run_choose(
            ak.Array({"Muon_pass": [[True], [], [False]], "pt": [1.0, 2.0, 3.0]}),
            {
                "cases": [
                    {"name": "muon", "when": "Muon_pass", "values": {"pt_out": "pt"}}
                ]
            },
        )

    message = str(excinfo.value)
    assert "case 'muon'" in message
    assert "3 * var * bool" in message


def test_choose_rejects_jagged_output() -> None:
    with pytest.raises(ValueError, match="expected one value per event") as excinfo:
        _run_choose(
            ak.Array({"is_a": [True, False, True], "Muon_pt": [[1.0], [], [3.0]]}),
            {
                "cases": [
                    {
                        "name": "muon",
                        "when": "is_a",
                        "values": {"pt": "Muon_pt"},
                    }
                ],
                "default": {"pt": "-999.0"},
                "on_no_match": "default",
            },
        )

    message = str(excinfo.value)
    assert "case 'muon' output 'pt'" in message
    assert "3 * var * float64" in message


def test_choose_rejects_inconsistent_output_mappings() -> None:
    with pytest.raises(ValueError, match="inconsistent outputs") as excinfo:
        _run_choose(
            ak.Array(
                {
                    "is_a": [True],
                    "is_b": [False],
                    "a_pt": [1.0],
                    "a_phi": [0.1],
                    "b_pt": [2.0],
                }
            ),
            {
                "cases": [
                    {
                        "name": "a",
                        "when": "is_a",
                        "values": {"pt": "a_pt", "phi": "a_phi"},
                    },
                    {"name": "b", "when": "is_b", "values": {"pt": "b_pt"}},
                ]
            },
        )

    assert "missing=['phi']" in str(excinfo.value)


def test_choose_rejects_incomplete_default_mapping() -> None:
    with pytest.raises(ValueError, match="default has inconsistent outputs"):
        _run_choose(
            ak.Array({"is_a": [False], "a_pt": [1.0], "a_phi": [0.1]}),
            {
                "cases": [
                    {
                        "name": "a",
                        "when": "is_a",
                        "values": {"pt": "a_pt", "phi": "a_phi"},
                    }
                ],
                "default": {"pt": "-999.0"},
                "on_no_match": "default",
            },
        )


def test_choose_broadcasts_scalar_default_expressions() -> None:
    out = _run_choose(
        ak.Array({"is_a": [False, False], "a_pt": [1.0, 2.0]}),
        {
            "cases": [{"name": "a", "when": "is_a", "values": {"pt": "a_pt"}}],
            "default": {"pt": "-999.0"},
            "on_no_match": "default",
        },
    )

    assert ak.to_list(out["pt"]) == [-999.0, -999.0]
    assert str(ak.type(out["pt"])) == "2 * float64"


def test_choose_dependency_parser_consumes_when_values_and_defaults() -> None:
    deps = parse_component_data_dependencies(
        spec=CHOOSE_SPEC,
        params={
            "cases": [
                {
                    "name": "a",
                    "when": "is_a & trigger",
                    "values": {"pt": "leading(a_pt, -999.0)", "phi": "a_phi"},
                },
                {
                    "name": "b",
                    "when": "is_b",
                    "values": {"pt": "leading(b_pt, -999.0)", "phi": "b_phi"},
                },
            ],
            "default": {"pt": "fallback_pt", "phi": "-999.0"},
            "on_no_match": "default",
        },
        dep_ctx=DependencyContext(
            known_functions={"leading"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.consumes == {
        "a_phi",
        "a_pt",
        "b_phi",
        "b_pt",
        "fallback_pt",
        "is_a",
        "is_b",
        "trigger",
    }
    assert deps.produces == {"pt", "phi"}


def test_choose_dependency_parser_derives_outputs_from_all_case_values() -> None:
    deps = parse_component_data_dependencies(
        spec=CHOOSE_SPEC,
        params={
            "cases": [
                {
                    "name": "muon",
                    "when": "SingleMuon_CR_selection",
                    "values": {
                        "Recoil_pt": "leading(muon_recoil_pt, -999.0)",
                        "Recoil_phi": "leading(muon_recoil_phi, -999.0)",
                    },
                },
                {
                    "name": "electron",
                    "when": "SingleElectron_CR_selection",
                    "values": {
                        "Recoil_phi": "leading(electron_recoil_phi, -999.0)",
                        "Recoil_pt": "leading(electron_recoil_pt, -999.0)",
                    },
                },
            ],
            "default": {"Recoil_pt": "-999.0", "Recoil_phi": "-999.0"},
            "on_no_match": "default",
        },
        dep_ctx=DependencyContext(
            known_functions={"leading"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.produces == {"Recoil_pt", "Recoil_phi"}
    assert deps.consumes == {
        "SingleMuon_CR_selection",
        "SingleElectron_CR_selection",
        "muon_recoil_pt",
        "muon_recoil_phi",
        "electron_recoil_pt",
        "electron_recoil_phi",
    }
    assert "Recoil_pt" not in deps.consumes
    assert "Recoil_phi" not in deps.consumes


def test_choose_dependency_parser_rejects_mismatched_case_outputs() -> None:
    with pytest.raises(ValueError, match="Recoil_phi"):
        parse_component_data_dependencies(
            spec=CHOOSE_SPEC,
            params={
                "cases": [
                    {
                        "name": "muon",
                        "when": "SingleMuon_CR_selection",
                        "values": {
                            "Recoil_pt": "leading(muon_recoil_pt, -999.0)",
                        },
                    },
                    {
                        "name": "electron",
                        "when": "SingleElectron_CR_selection",
                        "values": {
                            "Recoil_pt": "leading(electron_recoil_pt, -999.0)",
                            "Recoil_phi": "leading(electron_recoil_phi, -999.0)",
                        },
                    },
                ],
            },
            dep_ctx=DependencyContext(
                known_functions={"leading"},
                known_constants=set(),
                context_symbols=set(),
            ),
        )


def test_choose_rejects_existing_output_collision() -> None:
    with pytest.raises(ValueError, match="overwrite existing stream field"):
        _run_choose(
            ak.Array({"is_a": [True], "pt": [1.0]}),
            {"cases": [{"name": "a", "when": "is_a", "values": {"pt": "1.0"}}]},
        )


def test_choose_records_runtime_provenance() -> None:
    ctx = _Context()

    run_choose_transform(
        stream=ak.Array({"is_a": [True], "a_pt": [1.0]}),
        cases=[{"name": "a", "when": "is_a", "values": {"pt": "a_pt"}}],
        ctx=ctx,
    )

    assert ctx.provenance.records == [
        {
            "inputs": {"symbols": ["a_pt", "is_a"]},
            "outputs": {"symbols": ["pt"]},
        }
    ]


def test_choose_recoil_style_inputs_remain_jagged_but_outputs_are_scalar() -> None:
    events = ak.Array(
        {
            "SingleMuon_CR_selection": [True, False, False, False, False, False],
            "DiMuon_CR_selection": [False, True, False, False, False, False],
            "SingleElectron_CR_selection": [False, False, True, False, False, False],
            "DiElectron_CR_selection": [False, False, False, True, False, False],
            "SR_selection": [False, False, False, False, True, False],
            "singleMuon_CR_jec_Nominal_recoil_pt": [[110.0], [], [], [], [], []],
            "singleMuon_CR_jec_Nominal_recoil_phi": [[0.11], [], [], [], [], []],
            "diMuon_CR_jec_Nominal_recoil_pt": [[], [220.0], [], [], [], []],
            "diMuon_CR_jec_Nominal_recoil_phi": [[], [0.22], [], [], [], []],
            "singleElectron_CR_jec_Nominal_recoil_pt": [[], [], [330.0], [], [], []],
            "singleElectron_CR_jec_Nominal_recoil_phi": [[], [], [0.33], [], [], []],
            "diElectron_CR_jec_Nominal_recoil_pt": [[], [], [], [440.0], [], []],
            "diElectron_CR_jec_Nominal_recoil_phi": [[], [], [], [0.44], [], []],
            "SR_jec_Nominal_recoil_pt": [[], [], [], [], [550.0], []],
            "SR_jec_Nominal_recoil_phi": [[], [], [], [], [0.55], []],
        }
    )

    out = _run_choose(events, _recoil_choose_params())

    assert str(ak.type(out["singleMuon_CR_jec_Nominal_recoil_pt"])) == (
        "6 * var * float64"
    )
    assert str(ak.type(out["Recoil_pt"])) == "6 * float64"
    assert str(ak.type(out["Recoil_phi"])) == "6 * float64"
    assert ak.to_list(out["Recoil_pt"]) == [110.0, 220.0, 330.0, 440.0, 550.0, -999.0]
    assert ak.to_list(out["Recoil_phi"]) == [0.11, 0.22, 0.33, 0.44, 0.55, -999.0]


def test_choose_recoil_style_overlap_errors() -> None:
    events = ak.Array(
        {
            "SingleMuon_CR_selection": [True],
            "DiMuon_CR_selection": [False],
            "SingleElectron_CR_selection": [False],
            "DiElectron_CR_selection": [False],
            "SR_selection": [True],
            "singleMuon_CR_jec_Nominal_recoil_pt": [[110.0]],
            "singleMuon_CR_jec_Nominal_recoil_phi": [[0.11]],
            "diMuon_CR_jec_Nominal_recoil_pt": [[]],
            "diMuon_CR_jec_Nominal_recoil_phi": [[]],
            "singleElectron_CR_jec_Nominal_recoil_pt": [[]],
            "singleElectron_CR_jec_Nominal_recoil_phi": [[]],
            "diElectron_CR_jec_Nominal_recoil_pt": [[]],
            "diElectron_CR_jec_Nominal_recoil_phi": [[]],
            "SR_jec_Nominal_recoil_pt": [[550.0]],
            "SR_jec_Nominal_recoil_phi": [[0.55]],
        }
    )

    with pytest.raises(ValueError, match="matching multiple cases") as excinfo:
        _run_choose(events, _recoil_choose_params())

    assert "singleMuon, signalRegion" in str(excinfo.value)


def test_choose_matches_nested_where_after_scalarisation_for_exclusive_cases() -> None:
    events = ak.Array(
        {
            "is_a": [True, False, False, False],
            "is_b": [False, True, False, False],
            "is_c": [False, False, True, False],
            "a_pt": [[10.0], [], [], []],
            "b_pt": [[], [20.0], [], []],
            "c_pt": [[], [], [30.0], []],
        }
    )

    out = _run_choose(
        events,
        {
            "cases": [
                {
                    "name": "a",
                    "when": "is_a",
                    "values": {"pt": "leading(a_pt, -999.0)"},
                },
                {
                    "name": "b",
                    "when": "is_b",
                    "values": {"pt": "leading(b_pt, -999.0)"},
                },
                {
                    "name": "c",
                    "when": "is_c",
                    "values": {"pt": "leading(c_pt, -999.0)"},
                },
            ],
            "default": {"pt": "-999.0"},
            "on_no_match": "default",
        },
    )
    expected = ak.where(
        events["is_a"],
        ak.fill_none(ak.pad_none(events["a_pt"], 1, clip=True)[:, 0], -999.0),
        ak.where(
            events["is_b"],
            ak.fill_none(ak.pad_none(events["b_pt"], 1, clip=True)[:, 0], -999.0),
            ak.where(
                events["is_c"],
                ak.fill_none(ak.pad_none(events["c_pt"], 1, clip=True)[:, 0], -999.0),
                -999.0,
            ),
        ),
    )

    assert ak.to_list(out["pt"]) == ak.to_list(expected)


def _recoil_choose_params() -> dict[str, Any]:
    return {
        "cases": [
            {
                "name": "singleMuon",
                "when": "SingleMuon_CR_selection",
                "values": {
                    "Recoil_pt": (
                        "leading(singleMuon_CR_jec_Nominal_recoil_pt, -999.0)"
                    ),
                    "Recoil_phi": (
                        "leading(singleMuon_CR_jec_Nominal_recoil_phi, -999.0)"
                    ),
                },
            },
            {
                "name": "diMuon",
                "when": "DiMuon_CR_selection",
                "values": {
                    "Recoil_pt": "leading(diMuon_CR_jec_Nominal_recoil_pt, -999.0)",
                    "Recoil_phi": "leading(diMuon_CR_jec_Nominal_recoil_phi, -999.0)",
                },
            },
            {
                "name": "singleElectron",
                "when": "SingleElectron_CR_selection",
                "values": {
                    "Recoil_pt": (
                        "leading(singleElectron_CR_jec_Nominal_recoil_pt, -999.0)"
                    ),
                    "Recoil_phi": (
                        "leading(singleElectron_CR_jec_Nominal_recoil_phi, -999.0)"
                    ),
                },
            },
            {
                "name": "diElectron",
                "when": "DiElectron_CR_selection",
                "values": {
                    "Recoil_pt": (
                        "leading(diElectron_CR_jec_Nominal_recoil_pt, -999.0)"
                    ),
                    "Recoil_phi": (
                        "leading(diElectron_CR_jec_Nominal_recoil_phi, -999.0)"
                    ),
                },
            },
            {
                "name": "signalRegion",
                "when": "SR_selection",
                "values": {
                    "Recoil_pt": "leading(SR_jec_Nominal_recoil_pt, -999.0)",
                    "Recoil_phi": "leading(SR_jec_Nominal_recoil_phi, -999.0)",
                },
            },
        ],
        "default": {"Recoil_pt": "-999.0", "Recoil_phi": "-999.0"},
        "on_multiple": "error",
        "on_no_match": "default",
    }


def _run_choose(
    events: ak.Array,
    params: dict[str, Any],
    *,
    ctx: dict[str, Any] | None = None,
) -> ak.Array:
    out = run_choose(
        data={DEFAULT_PRIMARY_STREAM_ID: events},
        params=params,
        ctx={
            "primary_stream": DEFAULT_PRIMARY_STREAM_ID,
            "leading": leading,
            **dict(ctx or {}),
        },
    )
    return out[DEFAULT_PRIMARY_STREAM_ID]


class _Provenance:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def record_operation(self, **record: Any) -> None:
        self.records.append(record)


class _Context(dict[str, Any]):
    @property
    def provenance(self) -> _Provenance:
        return self["provenance"]

    def __init__(self) -> None:
        super().__init__({"provenance": _Provenance()})
