from __future__ import annotations

import math

import awkward as ak
import pytest
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.build_recoil import (
    BUILD_RECOIL_SPEC,
    run_build_recoil_transform,
)


def test_met_only_builds_one_recoil_per_event() -> None:
    out = _run(
        _events(met_pt=[250.0, 199.0], met_phi=[0.5, -0.5]),
        visible=[],
    )

    assert len(out) == 2
    assert ak.to_list(out.nRecoil) == [1, 0]
    assert ak.to_list(out.Recoil_pt) == [[250.0], []]
    assert ak.to_list(abs(out.Recoil_phi - 0.5) < 1e-12) == [[True], []]
    assert ak.to_list(out.Recoil_eta) == [[0.0], []]
    assert ak.to_list(out.Recoil_mass) == [[0.0], []]


def test_visible_objects_are_added_to_met_not_subtracted() -> None:
    out = _run(
        _events(
            mu_pt=[[20.0]],
            mu_phi=[[0.0]],
            met_pt=[30.0],
            met_phi=[math.pi],
        ),
        selection=["pt >= 0"],
    )

    assert ak.to_list(abs(out.Recoil_pt - 10.0) < 1e-12) == [[True]]
    assert ak.to_list(abs(abs(out.Recoil_phi) - math.pi) < 1e-12) == [[True]]


def test_two_visible_collections_build_aligned_recoil_candidates() -> None:
    out = _run(
        _events(
            mu_pt=[[30.0, 20.0]],
            mu_phi=[[0.0, math.pi]],
            ele_pt=[[10.0, 5.0]],
            ele_phi=[[0.0, math.pi]],
            met_pt=[50.0],
            met_phi=[0.0],
        ),
        visible=["Muon", "Electron"],
        selection=["pt >= 0"],
    )

    assert ak.to_list(out.nRecoil) == [2]
    assert ak.to_list(abs(out.Recoil_pt - 90.0) < 1e-12) == [[True]]
    assert ak.to_list(abs(out.Recoil_phi) < 1e-12) == [[True]]


def test_selection_is_inclusive_count_is_before_highest_pt_truncation() -> None:
    out = _run(
        _events(
            mu_pt=[[10.0, 60.0, 210.0]],
            mu_phi=[[0.0, 0.0, 0.0]],
            met_pt=[190.0],
            met_phi=[0.0],
        ),
    )

    assert ak.to_list(out.nRecoil) == [3]
    assert ak.to_list(out.Recoil_pt) == [[400.0]]


def test_below_threshold_candidates_are_removed_before_counting() -> None:
    out = _run(
        _events(mu_pt=[[9.9]], mu_phi=[[0.0]], met_pt=[190.0], met_phi=[0.0])
    )

    assert ak.to_list(out.nRecoil) == [0]
    assert ak.to_list(out.Recoil_pt) == [[]]


def test_empty_visible_collection_preserves_event_without_filtering() -> None:
    out = _run(_events(mu_pt=[[], [15.0]], met_pt=[250.0, 250.0]))

    assert len(out) == 2
    assert ak.to_list(out.nRecoil) == [0, 1]


def test_inputs_are_unchanged_and_output_collisions_fail() -> None:
    events = _events(mu_pt=[[25.0]], met_pt=[250.0])
    out = _run(events)

    assert ak.to_list(out.Muon_pt) == [[25.0]]
    with pytest.raises(ValueError, match="output field 'Recoil_pt' already exists"):
        _run(ak.with_field(events, [[1.0]], "Recoil_pt"))


def test_missing_and_misaligned_fields_fail_clearly() -> None:
    with pytest.raises(KeyError, match="MET_phi"):
        _run(ak.without_field(_events(mu_pt=[[25.0]]), "MET_phi"))

    with pytest.raises(ValueError, match="not aligned"):
        _run(
            _events(mu_pt=[[25.0, 30.0]], ele_pt=[[10.0]], met_pt=[250.0]),
            visible=["Muon", "Electron"],
        )


def test_dependencies_are_derived_from_params() -> None:
    deps = parse_component_data_dependencies(
        spec=BUILD_RECOIL_SPEC,
        params={
            "met": "jec_Nominal_TypeIPuppiMET",
            "visible": ["diMuon_lepton_1", "diMuon_lepton_2"],
            "output": "diMuon_CR_jec_Nominal_recoil",
            "selection": ["pt >= 200", "abs(phi) < 3.2"],
            "keep": ["pt", "phi", "eta", "mass"],
        },
        dep_ctx=DependencyContext(
            known_functions={"abs"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.consumes == {
        "jec_Nominal_TypeIPuppiMET_pt",
        "jec_Nominal_TypeIPuppiMET_phi",
        "diMuon_lepton_1_pt",
        "diMuon_lepton_1_phi",
        "diMuon_lepton_2_pt",
        "diMuon_lepton_2_phi",
    }
    assert deps.produces == {
        "diMuon_CR_jec_Nominal_recoil_pt",
        "diMuon_CR_jec_Nominal_recoil_phi",
        "diMuon_CR_jec_Nominal_recoil_eta",
        "diMuon_CR_jec_Nominal_recoil_mass",
        "ndiMuon_CR_jec_Nominal_recoil",
    }


def test_runtime_provenance_links_inputs_and_outputs() -> None:
    ctx = _Context()

    _run(_events(mu_pt=[[25.0]], met_pt=[250.0]), ctx=ctx)

    assert ctx.provenance.records == [
        {
            "inputs": {"symbols": ["MET_phi", "MET_pt", "Muon_phi", "Muon_pt"]},
            "outputs": {
                "symbols": [
                    "Recoil_eta",
                    "Recoil_mass",
                    "Recoil_phi",
                    "Recoil_pt",
                    "nRecoil",
                ]
            },
        }
    ]


def _run(
    events: ak.Array,
    *,
    visible: list[str] | None = None,
    selection: list[str] | None = None,
    ctx=None,
) -> ak.Array:
    return run_build_recoil_transform(
        stream=events,
        met="MET",
        visible=["Muon"] if visible is None else visible,
        output="Recoil",
        selection=["pt >= 200"] if selection is None else selection,
        keep=["pt", "phi", "eta", "mass"],
        reduce={"count": "before", "keep": "highest_pt"},
        ctx=ctx,
    )


def _events(
    *,
    mu_pt=None,
    mu_phi=None,
    ele_pt=None,
    ele_phi=None,
    met_pt=None,
    met_phi=None,
) -> ak.Array:
    n_events = len(met_pt) if met_pt is not None else 1
    mu_pt = mu_pt if mu_pt is not None else [[20.0] for _ in range(n_events)]
    ele_pt = ele_pt if ele_pt is not None else [[10.0 for _ in event] for event in mu_pt]
    return ak.Array(
        {
            "Muon_pt": mu_pt,
            "Muon_phi": mu_phi or [[0.0 for _ in event] for event in mu_pt],
            "Electron_pt": ele_pt,
            "Electron_phi": ele_phi or [[0.0 for _ in event] for event in ele_pt],
            "MET_pt": met_pt or [250.0 for _ in mu_pt],
            "MET_phi": met_phi or [0.0 for _ in mu_pt],
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
