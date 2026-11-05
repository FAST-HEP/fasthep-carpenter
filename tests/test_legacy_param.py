"""The optional ``legacy`` mapping is no longer declared; authored workflows that still pass it keep working."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import awkward as ak
import pytest
import yaml
from hepflow.api import compile_workflow_file

from fasthep_carpenter.operations.build_lepton_met_candidate import (
    BUILD_LEPTON_MET_CANDIDATE_SPEC,
)
from fasthep_carpenter.operations.build_pairs import BUILD_PAIRS_SPEC
from fasthep_carpenter.operations.build_recoil import BUILD_RECOIL_SPEC
from fasthep_carpenter.operations.select_objects import (
    SELECT_OBJECTS_SPEC,
    run_select_objects,
)
from fasthep_carpenter.operations.selection_flag import (
    SELECTION_FLAG_SPEC,
    run_selection_flag_transform,
)

_LEGACY = {"module": "legacy_module", "source": "legacy.py"}


@pytest.mark.parametrize(
    "spec",
    [
        BUILD_RECOIL_SPEC,
        BUILD_PAIRS_SPEC,
        BUILD_LEPTON_MET_CANDIDATE_SPEC,
        SELECTION_FLAG_SPEC,
        SELECT_OBJECTS_SPEC,
    ],
    ids=lambda spec: spec["name"],
)
def test_spec_does_not_declare_a_legacy_param(spec: dict[str, Any]) -> None:
    assert "legacy" not in spec["params"]


def test_selection_flag_still_accepts_a_legacy_param() -> None:
    out = run_selection_flag_transform(
        stream=ak.Array({"pt": [10.0, 3.0]}),
        selection=["pt > 5"],
        output="pass_pt",
        legacy=_LEGACY,
    )

    assert ak.to_list(out["pass_pt"]) == [True, False]


def test_select_objects_still_accepts_a_legacy_param() -> None:
    out = run_select_objects(
        stream=ak.Array({"Muon_pt": [[30.0, 10.0]], "Muon_eta": [[0.0, 0.1]]}),
        collection="Muon",
        output="selected_Muon",
        selection=["pt > 20"],
        keep=["pt", "eta"],
        legacy=_LEGACY,
    )

    assert ak.to_list(out["selected_Muon_pt"]) == [[30.0]]


def test_workflow_with_a_legacy_param_still_compiles(tmp_path: Path) -> None:
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["registry", "fasthep_carpenter:registry"]},
        "data": {"datasets": [], "defaults": {}},
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
            }
        },
        "analysis": {
            "stages": [
                {
                    "id": "Flag",
                    "op": "hep.selection.flag",
                    "params": {
                        "selection": ["pt > 5"],
                        "output": "pass_pt",
                        "legacy": _LEGACY,
                    },
                },
            ]
        },
    }
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(path, outdir=tmp_path / "build")

    assert plan.get_node("stage.Flag").impl == "hep.selection.flag"
