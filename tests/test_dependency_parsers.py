from __future__ import annotations

from copy import deepcopy

from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)

from fasthep_carpenter.operations.cms.match_l1t_jets import (
    CMS_MATCH_L1T_JETS_SPEC,
    MATCH_L1T_JETS_OUTPUTS,
)
from fasthep_carpenter.operations.cutflow import CUTFLOW_SPEC
from fasthep_carpenter.operations.define import DEFINE_SPEC
from fasthep_carpenter.operations.di_object_mass import DI_OBJECT_MASS_SPEC
from fasthep_carpenter.operations.hist import HIST_SPEC
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
