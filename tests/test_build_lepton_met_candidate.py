from __future__ import annotations

import math

import awkward as ak
import pytest
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.build_lepton_met_candidate import (
    BUILD_LEPTON_MET_CANDIDATE_SPEC,
    run_build_lepton_met_candidate_transform,
)


def test_empty_lepton_collection_preserves_events_and_counts_zero() -> None:
    out = _run(_events(pt=[[], [25.0]]))

    assert len(out) == 2
    assert ak.to_list(out.nW_lepton) == [0, 1]
    assert ak.to_list(out.nW_W) == [0, 1]
    assert ak.to_list(out.W_W_pt[0]) == []


def test_exactly_one_lepton_builds_candidate_and_keeps_arbitrary_fields() -> None:
    out = _run(_events(pt=[[25.0]], charge=[[-1]], tag=[[42]]))

    assert ak.to_list(out.nW_lepton) == [1]
    assert ak.to_list(out.nW_W) == [1]
    assert ak.to_list(out.W_lepton_charge) == [[-1]]
    assert ak.to_list(out.W_lepton_tag) == [[42]]


def test_multiple_leptons_build_multiple_candidates_without_sorting_or_truncation() -> None:
    out = _run(
        _events(
            pt=[[50.0, 30.0, 45.0]],
            phi=[[0.0, 1.0, 2.0]],
            charge=[[1, -1, 1]],
        )
    )

    assert ak.to_list(out.nW_lepton) == [3]
    assert ak.to_list(out.nW_W) == [3]
    assert ak.to_list(out.W_lepton_pt) == [[50.0, 30.0, 45.0]]


def test_lepton_and_candidate_selection_are_inclusive() -> None:
    out = _run(
        _events(
            pt=[[20.0, 19.9, 80.0, 81.0]],
            phi=[[0.0, 0.0, 0.0, math.pi]],
            met_pt=[80.0],
            met_phi=[0.0],
        ),
        selection={
            "lepton": ["pt >= 20"],
            "candidate": ["MT >= 0", "MT <= 160"],
        },
    )

    assert ak.to_list(out.nW_lepton) == [3]
    assert ak.to_list(out.nW_W) == [2]
    assert ak.to_list(out.W_lepton_pt) == [[20.0, 80.0]]


def test_candidate_four_vector_and_transverse_mass_are_correct() -> None:
    out = _run(
        _events(pt=[[40.0]], eta=[[0.0]], phi=[[0.0]], mass=[[0.0]], met_pt=[30.0], met_phi=[math.pi])
    )

    assert ak.to_list(abs(out.W_W_pt) < 10.0000000001) == [[True]]
    assert ak.to_list(abs(out.W_W_mass - math.sqrt(4 * 40.0 * 30.0)) < 1e-10) == [
        [True]
    ]
    assert ak.to_list(abs(out.W_W_MT - math.sqrt(4 * 40.0 * 30.0)) < 1e-10) == [
        [True]
    ]


def test_phi_difference_is_periodic_through_cosine() -> None:
    near_pi = _run(
        _events(pt=[[40.0]], phi=[[0.0]], met_pt=[30.0], met_phi=[math.pi])
    )
    shifted = _run(
        _events(pt=[[40.0]], phi=[[0.0]], met_pt=[30.0], met_phi=[math.pi + 2 * math.pi])
    )

    assert ak.to_list(abs(near_pi.W_W_MT - shifted.W_W_MT) < 1e-12) == [[True]]


def test_candidate_mask_preserves_lepton_alignment() -> None:
    out = _run(
        _events(
            pt=[[50.0, 50.0]],
            phi=[[0.0, math.pi]],
            met_pt=[50.0],
            met_phi=[0.0],
            tag=[[1, 2]],
        ),
        selection={"candidate": ["MT > 10"]},
    )

    assert ak.to_list(out.nW_lepton) == [2]
    assert ak.to_list(out.nW_W) == [1]
    assert ak.to_list(out.W_lepton_tag) == [[2]]


def test_sources_are_unchanged_and_output_collisions_fail() -> None:
    events = _events(pt=[[25.0]])
    out = _run(events)

    assert ak.to_list(out.Muon_pt) == [[25.0]]
    with pytest.raises(ValueError, match="output field 'nW_W' already exists"):
        _run(ak.with_field(events, [1], "nW_W"))


def test_missing_required_field_fails_clearly() -> None:
    events = ak.without_field(_events(pt=[[25.0]]), "MET_phi")

    with pytest.raises(KeyError, match="MET_phi"):
        _run(events)


def test_dependencies_are_derived_from_params() -> None:
    deps = parse_component_data_dependencies(
        spec=BUILD_LEPTON_MET_CANDIDATE_SPEC,
        params={
            "lepton": "selected_tight_Muon",
            "met": "jec_Nominal_TypeIPuppiMET",
            "output": "WFinder_singleMuon_jec_Nominal",
            "selection": {
                "lepton": ["pt >= 20", "abs(eta) <= 2.4"],
                "candidate": ["MT >= 0", "MT <= 160"],
            },
            "keep": {
                "candidate": ["pt", "eta", "phi", "mass", "MT"],
                "lepton": ["pt", "eta", "phi", "mass", "charge", "tag"],
            },
        },
        dep_ctx=DependencyContext(
            known_functions={"abs"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.consumes == {
        "selected_tight_Muon_pt",
        "selected_tight_Muon_eta",
        "selected_tight_Muon_phi",
        "selected_tight_Muon_mass",
        "selected_tight_Muon_charge",
        "selected_tight_Muon_tag",
        "jec_Nominal_TypeIPuppiMET_pt",
        "jec_Nominal_TypeIPuppiMET_phi",
    }
    assert deps.produces == {
        "WFinder_singleMuon_jec_Nominal_W_pt",
        "WFinder_singleMuon_jec_Nominal_W_eta",
        "WFinder_singleMuon_jec_Nominal_W_phi",
        "WFinder_singleMuon_jec_Nominal_W_mass",
        "WFinder_singleMuon_jec_Nominal_W_MT",
        "WFinder_singleMuon_jec_Nominal_lepton_pt",
        "WFinder_singleMuon_jec_Nominal_lepton_eta",
        "WFinder_singleMuon_jec_Nominal_lepton_phi",
        "WFinder_singleMuon_jec_Nominal_lepton_mass",
        "WFinder_singleMuon_jec_Nominal_lepton_charge",
        "WFinder_singleMuon_jec_Nominal_lepton_tag",
        "nWFinder_singleMuon_jec_Nominal_W",
        "nWFinder_singleMuon_jec_Nominal_lepton",
    }


def test_runtime_provenance_links_inputs_and_outputs() -> None:
    ctx = _Context()

    _run(_events(pt=[[25.0]], tag=[[7]]), ctx=ctx)

    assert ctx.provenance.records == [
        {
            "inputs": {
                "symbols": [
                    "MET_phi",
                    "MET_pt",
                    "Muon_charge",
                    "Muon_eta",
                    "Muon_mass",
                    "Muon_phi",
                    "Muon_pt",
                    "Muon_tag",
                ]
            },
            "outputs": {
                "symbols": [
                    "W_W_MT",
                    "W_W_eta",
                    "W_W_mass",
                    "W_W_phi",
                    "W_W_pt",
                    "W_lepton_charge",
                    "W_lepton_eta",
                    "W_lepton_mass",
                    "W_lepton_phi",
                    "W_lepton_pt",
                    "W_lepton_tag",
                    "nW_W",
                    "nW_lepton",
                ]
            },
        }
    ]


def _run(events, selection=None, ctx=None):
    return run_build_lepton_met_candidate_transform(
        stream=events,
        lepton="Muon",
        met="MET",
        output="W",
        selection=selection
        or {
            "lepton": ["pt >= 0"],
            "candidate": ["MT >= 0", "MT <= 999"],
        },
        keep={
            "candidate": ["pt", "eta", "phi", "mass", "MT"],
            "lepton": ["pt", "eta", "phi", "mass", "charge", "tag"],
        },
        ctx=ctx,
    )


def _events(
    *,
    pt,
    eta=None,
    phi=None,
    mass=None,
    charge=None,
    tag=None,
    met_pt=None,
    met_phi=None,
) -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": pt,
            "Muon_eta": eta or [[0.0 for _ in event] for event in pt],
            "Muon_phi": phi or [[0.0 for _ in event] for event in pt],
            "Muon_mass": mass or [[0.0 for _ in event] for event in pt],
            "Muon_charge": charge or [[1 for _ in event] for event in pt],
            "Muon_tag": tag or [[index for index, _ in enumerate(event)] for event in pt],
            "MET_pt": met_pt or [50.0 for _ in pt],
            "MET_phi": met_phi or [0.0 for _ in pt],
        }
    )


class _Provenance:
    def __init__(self):
        self.records = []

    def record_operation(self, *, inputs, outputs):
        self.records.append({"inputs": inputs, "outputs": outputs})


class _Context(dict):
    def __init__(self):
        super().__init__()
        self.provenance = _Provenance()
