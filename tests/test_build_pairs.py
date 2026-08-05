from __future__ import annotations

import math

import awkward as ak
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.build_pairs import (
    BUILD_PAIRS_SPEC,
    run_build_pairs_transform,
)


def test_one_collection_exactly_two_objects_builds_one_pair() -> None:
    out = run_build_pairs_transform(
        stream=_events(),
        collections=["Muon"],
        output="diMuon",
        selection={"pair": ["lepton_1_charge * lepton_2_charge < 0"]},
        keep=_keep(),
    )

    assert ak.to_list(out["ndiMuon_Z"]) == [1, 0, 2, 0]
    assert ak.to_list(out["diMuon_lepton_1_charge"][0]) == [1]
    assert ak.to_list(out["diMuon_lepton_2_charge"][0]) == [-1]


def test_multiple_collections_are_concatenated_in_declared_order() -> None:
    stream = ak.Array(
        {
            "tight_pt": [[40.0]],
            "tight_eta": [[0.0]],
            "tight_phi": [[0.0]],
            "tight_mass": [[0.0]],
            "tight_charge": [[1]],
            "loose_pt": [[25.0, 15.0]],
            "loose_eta": [[0.0, 0.0]],
            "loose_phi": [[math.pi, math.pi / 2]],
            "loose_mass": [[0.0, 0.0]],
            "loose_charge": [[-1, 1]],
        }
    )

    out = run_build_pairs_transform(
        stream=stream,
        collections=["tight", "loose"],
        output="pair",
        keep=_keep(),
        sort=False,
    )

    assert ak.to_list(out["pair_lepton_1_pt"]) == [[40.0, 40.0, 25.0]]
    assert ak.to_list(out["pair_lepton_2_pt"]) == [[25.0, 15.0, 15.0]]


def test_one_object_and_empty_events_produce_zero_candidates() -> None:
    out = run_build_pairs_transform(
        stream=_events(),
        collections=["Muon"],
        output="diMuon",
        keep=_keep(),
    )

    assert ak.to_list(out["ndiMuon_Z"]) == [1, 0, 3, 0]
    assert ak.to_list(out["diMuon_Z_pt"][1]) == []
    assert ak.to_list(out["diMuon_Z_pt"][3]) == []


def test_pair_and_candidate_predicates_preserve_jagged_events() -> None:
    out = run_build_pairs_transform(
        stream=_events(),
        collections=["Muon"],
        output="diMuon",
        selection={
            "pair": [
                "lepton_1_charge * lepton_2_charge < 0",
                "lepton_1_pt >= 20",
                "lepton_2_pt >= 10",
            ],
            "candidate": ["pt >= 20", "mass > 60", "mass < 120"],
        },
        keep=_keep(),
    )

    assert len(out) == 4
    assert ak.to_list(out["ndiMuon_Z"]) == [0, 0, 0, 0]


def test_strict_candidate_mass_boundaries() -> None:
    stream = ak.Array(
        {
            "Muon_pt": [[30.0, 30.0], [60.0, 60.0], [40.0, 40.0]],
            "Muon_eta": [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
            "Muon_phi": [[0.0, math.pi], [0.0, math.pi], [0.0, math.pi]],
            "Muon_mass": [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
            "Muon_charge": [[1, -1], [1, -1], [1, -1]],
        }
    )

    out = run_build_pairs_transform(
        stream=stream,
        collections=["Muon"],
        output="diMuon",
        selection={"candidate": ["mass > 60", "mass < 120"]},
        keep=_keep(),
    )

    assert ak.to_list(out["ndiMuon_Z"]) == [0, 0, 1]


def test_candidate_four_vector_mass_for_back_to_back_massless_pair() -> None:
    out = run_build_pairs_transform(
        stream=ak.Array(
            {
                "Muon_pt": [[45.0, 45.0]],
                "Muon_eta": [[0.0, 0.0]],
                "Muon_phi": [[0.0, math.pi]],
                "Muon_mass": [[0.0, 0.0]],
                "Muon_charge": [[1, -1]],
            }
        ),
        collections=["Muon"],
        output="diMuon",
        keep=_keep(),
    )

    assert ak.to_list(abs(out["diMuon_Z_pt"]) < 1e-12) == [[True]]
    assert ak.to_list(abs(out["diMuon_Z_mass"] - 90.0) < 1e-12) == [[True]]


def test_stable_sort_by_expression_preserves_alignment_and_does_not_truncate() -> None:
    out = run_build_pairs_transform(
        stream=ak.Array(
            {
                "Muon_pt": [[45.0, 50.0, 45.0]],
                "Muon_eta": [[0.0, 0.0, 0.0]],
                "Muon_phi": [[0.0, math.pi, math.pi]],
                "Muon_mass": [[0.0, 0.0, 0.0]],
                "Muon_charge": [[1, -1, -1]],
                "Muon_label": [["a", "b", "c"]],
            }
        ),
        collections=["Muon"],
        output="diMuon",
        selection={"pair": ["lepton_1_charge * lepton_2_charge < 0"]},
        sort={"by": "abs(mass - 91)", "order": "ascending"},
        keep={
            "candidate": ["mass"],
            "constituents": ["pt", "eta", "phi", "mass", "charge", "label"],
        },
    )

    assert ak.to_list(out["ndiMuon_Z"]) == [2]
    assert ak.to_list(out["diMuon_lepton_2_label"]) == [["c", "b"]]
    assert ak.to_list(out["diMuon_lepton_1_label"]) == [["a", "a"]]


def test_arbitrary_constituent_fields_are_retained_and_sources_unchanged() -> None:
    out = run_build_pairs_transform(
        stream=_events(),
        collections=["Muon"],
        output="diMuon",
        keep={
            "candidate": ["mass"],
            "constituents": ["pt", "eta", "phi", "mass", "charge", "tag"],
        },
    )

    assert ak.to_list(out["diMuon_lepton_1_tag"][2]) == [1, 1, 2]
    assert ak.to_list(out["Muon_tag"][2]) == [1, 2, 3]


def test_dependencies_are_derived_from_collections_expressions_sort_and_keep() -> None:
    params = {
        "collections": ["tight", "loose"],
        "output": "diMuon",
        "selection": {
            "pair": ["lepton_1_charge * lepton_2_charge < 0"],
            "candidate": ["pt >= 20", "abs(eta) <= 2.4"],
        },
        "sort": {"by": "abs(mass - 91)", "order": "ascending"},
        "keep": {
            "candidate": ["pt", "eta", "phi", "mass"],
            "constituents": ["pt", "eta", "phi", "mass", "charge", "tag"],
        },
    }
    deps = parse_component_data_dependencies(
        spec=BUILD_PAIRS_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions={"abs"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.consumes == {
        f"{collection}_{field}"
        for collection in ("tight", "loose")
        for field in ("pt", "eta", "phi", "mass", "charge", "tag")
    }
    assert deps.produces == {
        "ndiMuon_Z",
        "diMuon_Z_pt",
        "diMuon_Z_eta",
        "diMuon_Z_phi",
        "diMuon_Z_mass",
        "diMuon_lepton_1_pt",
        "diMuon_lepton_1_eta",
        "diMuon_lepton_1_phi",
        "diMuon_lepton_1_mass",
        "diMuon_lepton_1_charge",
        "diMuon_lepton_1_tag",
        "diMuon_lepton_2_pt",
        "diMuon_lepton_2_eta",
        "diMuon_lepton_2_phi",
        "diMuon_lepton_2_mass",
        "diMuon_lepton_2_charge",
        "diMuon_lepton_2_tag",
    }


def test_runtime_provenance_links_inputs_and_outputs() -> None:
    ctx = _Context()

    run_build_pairs_transform(
        stream=_events(),
        collections=["Muon"],
        output="diMuon",
        selection={"pair": ["lepton_1_charge * lepton_2_charge < 0"]},
        keep=_keep(),
        ctx=ctx,
    )

    assert ctx.provenance.records == [
        {
            "inputs": {
                "symbols": [
                    "Muon_charge",
                    "Muon_eta",
                    "Muon_mass",
                    "Muon_phi",
                    "Muon_pt",
                ]
            },
            "outputs": {
                "symbols": [
                    "diMuon_Z_eta",
                    "diMuon_Z_mass",
                    "diMuon_Z_phi",
                    "diMuon_Z_pt",
                    "diMuon_lepton_1_charge",
                    "diMuon_lepton_1_eta",
                    "diMuon_lepton_1_mass",
                    "diMuon_lepton_1_phi",
                    "diMuon_lepton_1_pt",
                    "diMuon_lepton_2_charge",
                    "diMuon_lepton_2_eta",
                    "diMuon_lepton_2_mass",
                    "diMuon_lepton_2_phi",
                    "diMuon_lepton_2_pt",
                    "ndiMuon_Z",
                ]
            },
        }
    ]


def _keep() -> dict[str, list[str]]:
    return {
        "candidate": ["pt", "eta", "phi", "mass"],
        "constituents": ["pt", "eta", "phi", "mass", "charge"],
    }


def _events() -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": [[45.0, 45.0], [30.0], [30.0, 30.0, 30.0], []],
            "Muon_eta": [[0.0, 0.0], [0.1], [0.0, 0.0, 0.0], []],
            "Muon_phi": [[0.0, math.pi], [0.2], [0.0, math.pi, math.pi / 2], []],
            "Muon_mass": [[0.0, 0.0], [0.0], [0.0, 0.0, 0.0], []],
            "Muon_charge": [[1, -1], [1], [1, -1, 1], []],
            "Muon_tag": [[1, 2], [1], [1, 2, 3], []],
        }
    )


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
