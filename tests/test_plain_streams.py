"""Operations take and return plain streams, never a ``{"events": stream}`` envelope."""

from __future__ import annotations

from typing import Any

import awkward as ak
import pytest

from fasthep_carpenter.operations.build_lepton_met_candidate import (
    run_build_lepton_met_candidate,
    run_build_lepton_met_candidate_transform,
)
from fasthep_carpenter.operations.build_pairs import (
    run_build_pairs,
    run_build_pairs_transform,
)
from fasthep_carpenter.operations.build_recoil import (
    run_build_recoil,
    run_build_recoil_transform,
)
from fasthep_carpenter.operations.cms.match_l1t_jets import (
    run_match_l1t_jets,
    run_match_l1t_jets_transform,
)
from fasthep_carpenter.operations.di_object_mass import (
    run_di_object_mass,
    run_di_object_mass_transform,
)

# --- builders -------------------------------------------------------------------------


def _recoil_events() -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": [[20.0], [30.0]],
            "Muon_phi": [[0.0], [0.0]],
            "MET_pt": [250.0, 199.0],
            "MET_phi": [0.5, -0.5],
        }
    )


_RECOIL: dict[str, Any] = {
    "met": "MET",
    "visible": [],
    "output": "Recoil",
    "selection": ["pt >= 200"],
    "keep": ["pt", "phi"],
}


def test_build_recoil_accepts_and_returns_plain_stream() -> None:
    out = run_build_recoil(_recoil_events(), dict(_RECOIL), {})

    assert isinstance(out, ak.Array)
    assert ak.to_list(out.nRecoil) == [1, 0]


def test_build_recoil_transform_returns_plain_stream() -> None:
    out = run_build_recoil_transform(stream=_recoil_events(), **_RECOIL)

    assert isinstance(out, ak.Array)


def test_transform_rejects_an_enveloped_stream() -> None:
    with pytest.raises(KeyError, match="missing from event stream"):
        run_build_recoil_transform(stream={"events": _recoil_events()}, **_RECOIL)


def _muons() -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": [[45.0, 45.0], [30.0]],
            "Muon_eta": [[0.0, 0.0], [0.1]],
            "Muon_phi": [[0.0, 3.0], [0.2]],
            "Muon_mass": [[0.0, 0.0], [0.0]],
            "Muon_charge": [[1, -1], [1]],
            "Muon_tag": [[1, 2], [1]],
        }
    )


_PAIRS: dict[str, Any] = {
    "collections": ["Muon"],
    "output": "diMuon",
    "selection": {"pair": ["lepton_1_charge * lepton_2_charge < 0"]},
    "keep": {
        "constituents": ["pt", "charge"],
        "candidate": ["pt", "mass"],
    },
}


def test_build_pairs_accepts_and_returns_plain_stream() -> None:
    out = run_build_pairs(_muons(), dict(_PAIRS), {})

    assert isinstance(out, ak.Array)
    assert ak.to_list(out["ndiMuon_Z"]) == [1, 0]


def test_build_pairs_transform_returns_plain_stream() -> None:
    out = run_build_pairs_transform(stream=_muons(), **_PAIRS)

    assert isinstance(out, ak.Array)


def _lepton_met_events() -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": [[40.0], []],
            "Muon_eta": [[0.0], []],
            "Muon_phi": [[0.0], []],
            "Muon_mass": [[0.0], []],
            "MET_pt": [50.0, 50.0],
            "MET_phi": [0.0, 0.0],
        }
    )


_LEPTON_MET: dict[str, Any] = {
    "lepton": "Muon",
    "met": "MET",
    "output": "W",
    "selection": {"lepton": ["pt >= 0"], "candidate": ["MT >= 0"]},
    "keep": {
        "candidate": ["pt", "MT"],
        "lepton": ["pt"],
    },
}


def test_build_lepton_met_candidate_accepts_and_returns_plain_stream() -> None:
    out = run_build_lepton_met_candidate(_lepton_met_events(), dict(_LEPTON_MET), {})

    assert isinstance(out, ak.Array)
    assert len(out) == 2


def test_build_lepton_met_candidate_transform_returns_plain_stream() -> None:
    out = run_build_lepton_met_candidate_transform(
        stream=_lepton_met_events(), **_LEPTON_MET
    )

    assert isinstance(out, ak.Array)


def _four_vectors() -> ak.Array:
    return ak.Array(
        {
            "Muon_Px": [[10.0, -10.0], [5.0]],
            "Muon_Py": [[0.0, 0.0], [0.0]],
            "Muon_Pz": [[0.0, 0.0], [0.0]],
            "Muon_E": [[10.0, 10.0], [5.0]],
        }
    )


def test_di_object_mass_accepts_and_returns_plain_stream() -> None:
    out = run_di_object_mass(_four_vectors(), {"collection": "Muon"}, {})

    assert isinstance(out, ak.Array)


def test_di_object_mass_transform_returns_plain_stream() -> None:
    out = run_di_object_mass_transform(stream=_four_vectors(), collection="Muon")

    assert isinstance(out, ak.Array)


def _jets() -> ak.Array:
    return ak.Array(
        {
            "reco_et": [[30.0, 25.0], [10.0]],
            "reco_eta": [[0.0, 1.0], [0.0]],
            "reco_phi": [[0.0, 1.0], [0.0]],
            "l1_et": [[28.0], [9.0]],
            "l1_eta": [[0.05], [0.0]],
            "l1_phi": [[0.0], [0.0]],
        }
    )


_MATCH: dict[str, Any] = {
    "reco": {"et": "reco_et", "eta": "reco_eta", "phi": "reco_phi"},
    "l1": {"et": "l1_et", "eta": "l1_eta", "phi": "l1_phi"},
}


def test_match_l1t_jets_accepts_and_returns_plain_stream() -> None:
    out = run_match_l1t_jets(_jets(), dict(_MATCH), {})

    assert isinstance(out, ak.Array)
    assert "n_matched" in out.fields


def test_match_l1t_jets_transform_returns_plain_stream() -> None:
    out = run_match_l1t_jets_transform(stream=_jets(), **_MATCH)

    assert isinstance(out, ak.Array)
    assert "n_matched" in out.fields
