from __future__ import annotations

from fasthep_carpenter.spec.cms.match_l1t_jets_transform import (
    MATCH_L1T_JETS_OUTPUTS,
    parse_match_l1t_jets_data_dependencies,
)
from fasthep_carpenter.spec.cutflow_transform import (
    parse_cutflow_column_dependencies,
)
from fasthep_carpenter.spec.define_transform import parse_define_column_dependencies
from fasthep_carpenter.spec.di_object_mass_transform import (
    parse_di_object_mass_data_dependencies,
)
from fasthep_carpenter.spec.hist_transform import parse_hist_column_dependencies


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
