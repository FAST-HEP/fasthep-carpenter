from __future__ import annotations

from copy import deepcopy

from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.build_lepton_met_candidate import (
    BUILD_LEPTON_MET_CANDIDATE_SPEC,
)
from fasthep_carpenter.operations.build_pairs import BUILD_PAIRS_SPEC
from fasthep_carpenter.operations.build_recoil import BUILD_RECOIL_SPEC
from fasthep_carpenter.operations.clean import CLEAN_SPEC
from fasthep_carpenter.operations.cms.match_l1t_jets import (
    CMS_MATCH_L1T_JETS_SPEC,
    MATCH_L1T_JETS_OUTPUTS,
)
from fasthep_carpenter.operations.cutflow import CUTFLOW_SPEC
from fasthep_carpenter.operations.define import DEFINE_SPEC
from fasthep_carpenter.operations.di_object_mass import DI_OBJECT_MASS_SPEC
from fasthep_carpenter.operations.hist import HIST_SPEC
from fasthep_carpenter.operations.project_fields import PROJECT_FIELDS_SPEC
from fasthep_carpenter.operations.selection_flag import SELECTION_FLAG_SPEC
from fasthep_carpenter.operations.weights.lookup_csv import LOOKUP_CSV_SPEC
from fasthep_carpenter.operations.weights.pdf_envelope import PDF_ENVELOPE_SPEC


def _declarative_dependencies(
    spec: dict,
    params: dict,
    *,
    known_functions: set[str] | None = None,
):
    declarative_spec = deepcopy(spec)
    return parse_component_data_dependencies(
        spec=declarative_spec,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(known_functions or set()),
            known_constants=set(),
            context_symbols=set(),
        ),
    )


def test_define_declarative_dependencies() -> None:
    deps = _declarative_dependencies(
        DEFINE_SPEC,
        {
            "variables": [
                {"name": "Muon_Pt", "expr": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"},
                {
                    "name": "NIsolatedMuon",
                    "reduce": {"op": "count_nonzero", "over": "Muon_Iso < 0.1"},
                },
            ]
        },
        known_functions={"sqrt"},
    )

    assert deps.consumes == {"Muon_Px", "Muon_Py", "Muon_Iso"}
    assert deps.produces == {"Muon_Pt", "NIsolatedMuon"}


def test_hist_declarative_dependencies() -> None:
    deps = _declarative_dependencies(
        HIST_SPEC,
        {
            "axes": [
                {"name": "pt", "source": "Muon_Pt"},
                {"name": "eta", "source": "abs(Muon_Eta)"},
            ],
            "weight_expr": "event_weight",
        },
        known_functions={"abs"},
    )

    assert deps.consumes == {"Muon_Pt", "Muon_Eta", "event_weight"}
    assert deps.produces == set()


def test_hist_variation_weight_dependencies() -> None:
    deps = _declarative_dependencies(
        HIST_SPEC,
        {
            "axes": [{"name": "npv", "source": "PV_npvs"}],
            "variations": {
                "axis": "variation",
                "weights": {
                    "nominal": "weight_pu_nominal",
                    "up": "weight_pu_up",
                    "down": "weight_pu_down",
                },
            },
        },
    )

    assert deps.consumes == {
        "PV_npvs",
        "weight_pu_nominal",
        "weight_pu_up",
        "weight_pu_down",
    }
    assert deps.produces == set()


def test_cutflow_declarative_dependencies_find_nested_selections() -> None:
    deps = _declarative_dependencies(
        CUTFLOW_SPEC,
        {
            "selection": {
                "preselection": {
                    "steps": [
                        {"expr": "Muon_Pt > 25"},
                        {"reduce": {"op": "any", "over": "Muon_Iso < 0.1"}},
                    ]
                },
                "signal": ["triggerIsoMu24 == 1"],
            }
        },
    )

    assert deps.consumes == {"Muon_Pt", "Muon_Iso", "triggerIsoMu24"}


def test_project_fields_declarative_dependencies_map_inputs_and_outputs() -> None:
    deps = _declarative_dependencies(
        PROJECT_FIELDS_SPEC,
        {
            "stream_id": "events",
            "aliases": {
                "analysis_pt": "Muon_Pt",
                "analysis_iso": "Muon_Iso",
            },
        },
    )

    assert deps.consumes == {"Muon_Pt", "Muon_Iso"}
    assert deps.produces == {"analysis_pt", "analysis_iso"}


def test_di_object_mass_declarative_requirements_use_collection_default() -> None:
    deps = _declarative_dependencies(
        DI_OBJECT_MASS_SPEC,
        {"mask": "IsoMuon_Idx"},
    )

    assert deps.consumes == {
        "Muon_Px",
        "Muon_Py",
        "Muon_Pz",
        "Muon_E",
        "IsoMuon_Idx",
    }
    assert deps.produces == {"DiMuon_Mass"}


def test_cms_match_l1t_jets_declarative_requirements() -> None:
    params = {
        "reco": {"et": "reco_et", "eta": "reco_eta", "phi": "reco_phi"},
        "l1": {"et": "l1_et", "eta": "l1_eta", "phi": "l1_phi"},
    }

    deps = _declarative_dependencies(CMS_MATCH_L1T_JETS_SPEC, params)

    assert deps.consumes == {
        "reco_et",
        "reco_eta",
        "reco_phi",
        "l1_et",
        "l1_eta",
        "l1_phi",
    }
    assert deps.produces == MATCH_L1T_JETS_OUTPUTS


def test_clean_dependencies_expose_param_collection_references() -> None:
    deps = _declarative_dependencies(
        CLEAN_SPEC,
        {
            "source": "selected_photons",
            "clean_against": ["selected_muons", "selected_electrons"],
            "output": "cleaned_photons",
            "diagnostics": {"removed_count": "nremoved_photon_overlap"},
        },
    )

    assert deps.consumes == {
        "selected_photons_eta",
        "selected_photons_phi",
        "selected_muons_eta",
        "selected_muons_phi",
        "selected_electrons_eta",
        "selected_electrons_phi",
    }
    assert deps.produces == {
        "cleaned_photons",
        "ncleaned_photons",
        "nremoved_photon_overlap",
    }


def test_clean_dependencies_require_source_sort_field_only_when_sorting() -> None:
    unsorted = _declarative_dependencies(
        CLEAN_SPEC,
        {
            "source": "selected_photons",
            "clean_against": ["selected_muons"],
            "output": "cleaned_photons",
            "diagnostics": {"removed_count": False},
        },
    )
    sorted_deps = _declarative_dependencies(
        CLEAN_SPEC,
        {
            "source": "selected_photons",
            "clean_against": ["selected_muons"],
            "output": "cleaned_photons",
            "sort_by": "pt",
            "diagnostics": {"removed_count": False},
        },
    )

    assert "selected_photons_pt" not in unsorted.consumes
    assert "selected_photons_pt" in sorted_deps.consumes


def test_clean_dependencies_expand_collection_relative_fields() -> None:
    deps = _declarative_dependencies(
        CLEAN_SPEC,
        {
            "source": "selected_veto_Electron",
            "clean_against": ["selected_tight_Muon"],
            "output": "cleaned_veto_Electron",
            "fields": ["pt", "eta", "phi", "miniPFRelIso_all"],
            "diagnostics": {"removed_count": "nremoved_veto_Electron_overlap"},
        },
    )

    assert {
        "selected_veto_Electron_pt",
        "selected_veto_Electron_eta",
        "selected_veto_Electron_phi",
        "selected_veto_Electron_miniPFRelIso_all",
        "selected_tight_Muon_eta",
        "selected_tight_Muon_phi",
    } <= deps.consumes
    assert {
        "cleaned_veto_Electron",
        "cleaned_veto_Electron_pt",
        "cleaned_veto_Electron_eta",
        "cleaned_veto_Electron_phi",
        "cleaned_veto_Electron_miniPFRelIso_all",
        "ncleaned_veto_Electron",
        "nremoved_veto_Electron_overlap",
    } <= deps.produces


def test_build_pairs_dependencies_expand_collections_expressions_and_outputs() -> None:
    deps = _declarative_dependencies(
        BUILD_PAIRS_SPEC,
        {
            "collections": ["selected_tight_Muon", "cleaned_CRloose_Muon"],
            "output": "diMuon",
            "selection": {
                "pair": [
                    "lepton_1_charge * lepton_2_charge < 0",
                    "lepton_1_pt >= 20",
                    "lepton_2_pt >= 10",
                ],
                "candidate": ["pt >= 20", "abs(eta) <= 2.4", "mass > 60"],
            },
            "sort": {"by": "abs(mass - 91)", "order": "ascending"},
            "keep": {
                "candidate": ["pt", "eta", "phi", "mass"],
                "constituents": ["pt", "eta", "phi", "mass", "charge", "tag"],
            },
        },
        known_functions={"abs"},
    )

    assert deps.consumes == {
        f"{collection}_{field}"
        for collection in ("selected_tight_Muon", "cleaned_CRloose_Muon")
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


def test_build_lepton_met_candidate_dependencies_expand_params() -> None:
    deps = _declarative_dependencies(
        BUILD_LEPTON_MET_CANDIDATE_SPEC,
        {
            "lepton": "selected_tight_Electron",
            "met": "jec_Nominal_TypeIPuppiMET",
            "output": "WFinder_singleElectron_jec_Nominal",
            "selection": {
                "lepton": ["pt >= 40"],
                "candidate": ["MT >= 0", "MT <= 160"],
            },
            "keep": {
                "candidate": ["pt", "eta", "phi", "mass", "MT"],
                "lepton": ["pt", "eta", "phi", "mass", "charge"],
            },
        },
    )

    assert deps.consumes == {
        "selected_tight_Electron_pt",
        "selected_tight_Electron_eta",
        "selected_tight_Electron_phi",
        "selected_tight_Electron_mass",
        "selected_tight_Electron_charge",
        "jec_Nominal_TypeIPuppiMET_pt",
        "jec_Nominal_TypeIPuppiMET_phi",
    }
    assert deps.produces == {
        "WFinder_singleElectron_jec_Nominal_W_pt",
        "WFinder_singleElectron_jec_Nominal_W_eta",
        "WFinder_singleElectron_jec_Nominal_W_phi",
        "WFinder_singleElectron_jec_Nominal_W_mass",
        "WFinder_singleElectron_jec_Nominal_W_MT",
        "WFinder_singleElectron_jec_Nominal_lepton_pt",
        "WFinder_singleElectron_jec_Nominal_lepton_eta",
        "WFinder_singleElectron_jec_Nominal_lepton_phi",
        "WFinder_singleElectron_jec_Nominal_lepton_mass",
        "WFinder_singleElectron_jec_Nominal_lepton_charge",
        "nWFinder_singleElectron_jec_Nominal_W",
        "nWFinder_singleElectron_jec_Nominal_lepton",
    }


def test_build_recoil_dependencies_expand_params() -> None:
    deps = _declarative_dependencies(
        BUILD_RECOIL_SPEC,
        {
            "met": "jec_Nominal_TypeIPuppiMET",
            "visible": ["diMuon_lepton_1", "diMuon_lepton_2"],
            "output": "diMuon_CR_jec_Nominal_recoil",
            "selection": ["pt >= 200"],
            "keep": ["pt", "phi", "eta", "mass"],
        },
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


def test_selection_flag_uses_declarative_dependency_contract() -> None:
    assert SELECTION_FLAG_SPEC["requires"] == {
        "symbols": [{"from": "params.selection.*", "kind": "expr"}]
    }
    assert SELECTION_FLAG_SPEC["provides"] == {
        "symbols": [{"from": "params.output", "kind": "field_list"}]
    }
    assert "dependency_parser" not in SELECTION_FLAG_SPEC
    assert "dependencies" not in SELECTION_FLAG_SPEC


def test_weight_operations_declarative_dependencies() -> None:
    lookup = _declarative_dependencies(
        LOOKUP_CSV_SPEC,
        {
            "variable": "Muon_Pt",
            "outputs": {"nominal": "Weight", "up": "WeightUp"},
        },
    )
    envelope = _declarative_dependencies(
        PDF_ENVELOPE_SPEC,
        {
            "inputs": "LHEPdfWeight",
            "outputs": {"up": "PdfUp", "down": "PdfDown"},
        },
    )

    assert lookup.consumes == {"Muon_Pt"}
    assert lookup.produces == {"Weight", "WeightUp"}
    assert envelope.consumes == {"LHEPdfWeight"}
    assert envelope.produces == {"PdfUp", "PdfDown"}
