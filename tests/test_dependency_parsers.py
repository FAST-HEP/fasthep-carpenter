from __future__ import annotations

from copy import deepcopy

from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.cms.match_l1t_jets import (
    CMS_MATCH_L1T_JETS_SPEC,
    MATCH_L1T_JETS_OUTPUTS,
    parse_match_l1t_jets_data_dependencies,
)
from fasthep_carpenter.operations.cutflow import (
    CUTFLOW_SPEC,
    parse_cutflow_column_dependencies,
)
from fasthep_carpenter.operations.define import (
    DEFINE_SPEC,
    parse_define_column_dependencies,
)
from fasthep_carpenter.operations.di_object_mass import (
    DI_OBJECT_MASS_SPEC,
    parse_di_object_mass_data_dependencies,
)
from fasthep_carpenter.operations.hist import parse_hist_column_dependencies
from fasthep_carpenter.operations.project_fields import PROJECT_FIELDS_SPEC
from fasthep_carpenter.operations.weights.lookup_csv import LOOKUP_CSV_SPEC
from fasthep_carpenter.operations.weights.pdf_envelope import PDF_ENVELOPE_SPEC


def _declarative_dependencies(
    spec: dict,
    params: dict,
    *,
    known_functions: set[str] | None = None,
):
    declarative_spec = deepcopy(spec)
    declarative_spec.pop("dependencies", None)
    return parse_component_data_dependencies(
        spec=declarative_spec,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(known_functions or set()),
            known_constants=set(),
            context_symbols=set(),
        ),
    )


def test_define_data_dependencies() -> None:
    deps = parse_define_column_dependencies(
        {
            "variables": [
                {
                    "name": "Muon_Pt",
                    "expr": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)",
                },
                {
                    "name": "IsoMuon_Idx",
                    "expr": "(Muon_Iso / Muon_Pt) < 0.10",
                },
            ],
        },
        known_functions={"sqrt"},
        known_constants=set(),
        context_symbols=set(),
    )

    assert deps.consumes == {"Muon_Px", "Muon_Py", "Muon_Iso"}
    assert deps.produces == {"Muon_Pt", "IsoMuon_Idx"}


def test_define_declarative_dependencies_match_parser() -> None:
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


def test_hist_data_dependencies() -> None:
    deps = parse_hist_column_dependencies(
        {
            "axes": [
                {"name": "pt", "source": "Muon_Pt"},
                {"name": "eta", "source": "abs(Muon_Eta)"},
            ],
            "weight_expr": "event_weight",
        },
        known_functions={"abs"},
        known_constants=set(),
        context_symbols=set(),
    )

    assert deps.consumes == {"Muon_Pt", "Muon_Eta", "event_weight"}
    assert deps.produces == set()


def test_cutflow_data_dependencies() -> None:
    deps = parse_cutflow_column_dependencies(
        {
            "selection": {
                "All": [
                    {"expr": "Muon_Pt > 25"},
                    {"reduce": {"op": "any", "over": "Muon_Iso < 0.1"}},
                ],
            },
            "weight_expr": "event_weight",
        },
        known_functions=set(),
        known_constants=set(),
        context_symbols=set(),
    )

    assert {"Muon_Pt", "Muon_Iso", "event_weight"} <= deps.consumes
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


def test_di_object_mass_data_dependencies() -> None:
    deps = parse_di_object_mass_data_dependencies(
        {
            "collection": "Muon",
            "mask": "IsoMuon_Idx",
        },
        known_functions=set(),
        known_constants=set(),
        context_symbols=set(),
    )

    assert {
        "Muon_Px",
        "Muon_Py",
        "Muon_Pz",
        "Muon_E",
        "IsoMuon_Idx",
    } <= deps.consumes
    assert deps.produces == {"DiMuon_Mass"}


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


def test_cms_match_l1t_jets_data_dependencies() -> None:
    deps = parse_match_l1t_jets_data_dependencies(
        {
            "reco": {
                "et": "reco_jetEtCorr",
                "eta": "reco_jetEta",
                "phi": "reco_jetPhi",
            },
            "l1": {
                "et": "l1_jetEt",
                "eta": "l1_jetEta",
                "phi": "l1_jetPhi",
            },
        },
        known_functions=set(),
        known_constants=set(),
        context_symbols=set(),
    )

    assert {
        "reco_jetEtCorr",
        "reco_jetEta",
        "reco_jetPhi",
        "l1_jetEt",
        "l1_jetEta",
        "l1_jetPhi",
    } <= deps.consumes
    assert deps.produces >= MATCH_L1T_JETS_OUTPUTS


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
