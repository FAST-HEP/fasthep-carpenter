from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

import awkward as ak
import pytest
import uproot
import yaml
from hepflow.compiler.normalize import normalize_workflow

from fasthep_carpenter.operations.define import DEFINE_SPEC
from fasthep_carpenter.sinks.root_tree import manifest_path
from fasthep_carpenter.sources.root_tree import (
    RootTreeSchema,
    run_root_tree_source,
)

NEW_LINEAGE_DEFINE_SPEC = {
    **DEFINE_SPEC,
    "name": "test.new_lineage",
    "result": {
        "stream": {
            "kind": "event_stream",
            "lineage": "new",
        }
    },
}


def test_compile_resolves_root_tree_source_from_carpenter_profile(tmp_path: Path) -> None:
    compile_workflow_file = _hepflow_api().compile_workflow_file
    workflow_path = tmp_path / "workflow.yaml"
    workflow = {
        "version": "1.0",
        "use": {
            "profiles": [
                "registry",
                "fasthep_carpenter:registry",
            ],
        },
        "data": {
            "datasets": [],
            "defaults": {},
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
            },
        },
        "analysis": {
            "stages": [
                {
                    "id": "BasicVars",
                    "op": "hep.define",
                    "params": {
                        "variables": [
                            {
                                "name": "Muon_Pt",
                                "expr": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)",
                            },
                        ],
                    },
                },
            ],
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert plan.registry["sources"]["root_tree"]["impl"] == (
        "fasthep_carpenter.sources.root_tree:run_root_tree_source"
    )
    assert plan.registry["transforms"]["hep.define"]["impl"] == (
        "fasthep_carpenter.operations.define:run_define_transform"
    )
    assert "read.events" in {node.id for node in plan.nodes}


def test_define_variables_expand_mapping_matrix_before_plan(tmp_path: Path) -> None:
    compile_workflow_file = _hepflow_api().compile_workflow_file
    workflow_path = tmp_path / "workflow.yaml"
    workflow = {
        "version": "1.0",
        "use": {
            "profiles": [
                "registry",
                "fasthep_carpenter:registry",
            ],
        },
        "data": {"datasets": [], "defaults": {}},
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
            },
        },
        "analysis": {
            "stages": [
                {
                    "id": "Weights",
                    "op": "hep.define",
                    "params": {
                        "variables": [
                            {"name": "weight_nominal", "expr": "1.0"},
                            {
                                "name": "weight_pdf_{index}",
                                "expr": "1.0",
                                "dtype": "float32",
                                "matrix": {
                                    "index": {
                                        "range": {
                                            "start": 0,
                                            "stop": 3,
                                        }
                                    }
                                },
                            },
                        ]
                    },
                },
            ]
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    node = plan.get_node("stage.Weights")
    assert node.params["variables"] == [
        {"name": "weight_nominal", "expr": "1.0"},
        {"name": "weight_pdf_0", "expr": "1.0", "dtype": "float32"},
        {"name": "weight_pdf_1", "expr": "1.0", "dtype": "float32"},
        {"name": "weight_pdf_2", "expr": "1.0", "dtype": "float32"},
    ]
    assert "matrix" not in node.params["variables"][1]
    assert node.meta["compile_hooks"]["variables"][0]["generated"] == 3
    assert set(plan.data_flow["origins"]) >= {
        "weight_nominal",
        "weight_pdf_0",
        "weight_pdf_1",
        "weight_pdf_2",
    }


def test_merge_fields_spec_uses_stream_merge_contract(tmp_path: Path) -> None:
    plan = _compile_merge_workflow(
        tmp_path,
        left_field="left_pt",
        right_field="right_pt",
    )

    merge = plan.get_node("stage.Merge")
    fields = plan.data_flow["origins"]
    lineage = plan.data_flow["_stream_lineage"]["stage.Merge:stream"]["identity"]

    assert merge.impl == "hep.merge_fields"
    assert {"Muon_pt", "left_pt", "right_pt"} <= set(fields)
    assert fields["left_pt"]["node"] == "stage.Left"
    assert fields["right_pt"]["node"] == "stage.Right"
    assert lineage == plan.data_flow["_stream_lineage"]["stage.Left:stream"]["identity"]


def test_merge_fields_duplicate_error_fails_at_compile(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate event-stream field"):
        _compile_merge_workflow(
            tmp_path,
            left_field="shared_pt",
            right_field="shared_pt",
            on_conflict="error",
        )


def test_merge_fields_keep_first_keeps_logical_duplicate_origins(
    tmp_path: Path,
) -> None:
    plan = _compile_merge_workflow(
        tmp_path,
        left_field="shared_pt",
        right_field="shared_pt",
        on_conflict="keep_first",
    )

    assert _logical_origin_nodes(plan, "shared_pt") == {"stage.Left", "stage.Right"}


def test_merge_fields_keep_last_keeps_logical_duplicate_origins(
    tmp_path: Path,
) -> None:
    plan = _compile_merge_workflow(
        tmp_path,
        left_field="shared_pt",
        right_field="shared_pt",
        on_conflict="keep_last",
    )

    assert _logical_origin_nodes(plan, "shared_pt") == {"stage.Left", "stage.Right"}


def test_merge_fields_rejects_incompatible_lineage(tmp_path: Path) -> None:
    workflow = _merge_workflow(
        left_field="left_pt",
        right_field="right_pt",
        left_op="hep.define",
        right_op="hep.define",
    )
    workflow["registry"] = {
        "transforms": {
            "test.new_lineage": {
                "spec": "test_flow_integration:NEW_LINEAGE_DEFINE_SPEC",
                "impl": "fasthep_carpenter.operations.define:run_define_transform",
            }
        }
    }
    workflow["analysis"]["stages"][2]["op"] = "test.new_lineage"

    with pytest.raises(ValueError, match="incompatible event-stream lineages"):
        _compile_workflow_dict(tmp_path, workflow)


def test_merge_fields_outputs_are_visible_to_downstream_field_glob(
    tmp_path: Path,
) -> None:
    workflow = _merge_workflow(left_field="left_pt", right_field="right_pt")
    workflow["analysis"]["stages"].append(
        {
            "id": "Align",
            "op": "hep.align_schema",
            "from": "Merge",
            "params": {
                "schema": {"version": 1, "fields": {}},
                "missing": "ignore",
                "extra": "drop",
                "keep": ["left_*", "right_*"],
            },
        }
    )

    plan = _compile_workflow_dict(tmp_path, workflow)
    align = plan.get_node("stage.Align")

    assert align.params["keep"] == ["left_pt", "right_pt"]
    assert {"left_pt", "right_pt"} <= set(plan.data_flow["origins"])


def test_attached_root_tree_writer_produces_output_artifact(tmp_path: Path) -> None:
    run_workflow_file = _hepflow_api().run_workflow_file
    input_path = tmp_path / "input.root"
    with uproot.recreate(input_path) as root_file:
        root_file["events"] = {"Muon_Pt": [1, 2, 3]}

    workflow_path = tmp_path / "workflow.yaml"
    workflow = {
        "version": "1.0",
        "use": {
            "profiles": [
                "registry",
                "fasthep_carpenter:registry",
            ],
        },
        "data": {
            "datasets": [
                {
                    "name": "sample",
                    "files": [str(input_path)],
                    "nevents": 3,
                }
            ],
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
            },
        },
        "outputs": {
            "small": {
                "tree": "events",
                "keep": ["Muon_Pt"],
            }
        },
        "analysis": {
            "stages": [
                {
                    "id": "DerivedValue",
                    "op": "hep.define",
                    "params": {
                        "variables": [
                            {
                                "name": "doubled",
                                "expr": "Muon_Pt * 2",
                            }
                        ],
                    },
                    "write": [
                        {
                            "kind": "root_tree",
                            "path": "skim.root",
                            "use": "small",
                        }
                    ],
                }
            ],
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    result = run_workflow_file(workflow_path, outdir=build_dir, chunk_size=2)

    output_path = build_dir / "artifacts" / "files" / "skim" / "sample" / "0_0.root"
    second_output_path = (
        build_dir / "artifacts" / "files" / "skim" / "sample" / "0_1.root"
    )
    assert result.success is True
    assert output_path.exists()
    assert second_output_path.exists()
    with uproot.open(output_path) as root_file:
        assert root_file["events"].keys() == ["Muon_Pt"]
        assert root_file["events"]["Muon_Pt"].array(library="np").tolist() == [1, 2]
    with uproot.open(second_output_path) as root_file:
        assert root_file["events"].keys() == ["Muon_Pt"]
        assert root_file["events"]["Muon_Pt"].array(library="np").tolist() == [3]

    manifest = json.loads(
        (build_dir / "artifacts" / "files" / "skim" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    file_entries = manifest["datasets"]["sample"]["files"]
    provenance_links = [item.pop("provenance") for item in file_entries]
    assert manifest == {
        "kind": "root_tree",
        "name": "skim",
        "node_id": "write.DerivedValue.0",
        "input_node": "stage.DerivedValue",
        "tree": "events",
        "format": "rntuple",
        "root_classname": "ROOT::RNTuple",
        "total_entries": 3,
        "datasets": {
            "sample": {
                "total_entries": 3,
                "files": [
                    {
                        "path": "artifacts/files/skim/sample/0_0.root",
                        "path_type": "relative_to_outdir",
                        "dataset": "sample",
                        "partition": 0,
                        "attempt": 0,
                        "entries": 2,
                        "size_bytes": output_path.stat().st_size,
                        "format": "rntuple",
                        "root_classname": "ROOT::RNTuple",
                    },
                    {
                        "path": "artifacts/files/skim/sample/0_1.root",
                        "path_type": "relative_to_outdir",
                        "dataset": "sample",
                        "partition": 1,
                        "attempt": 0,
                        "entries": 1,
                        "size_bytes": second_output_path.stat().st_size,
                        "format": "rntuple",
                        "root_classname": "ROOT::RNTuple",
                    },
                ],
            }
        },
    }
    for link in provenance_links:
        assert link["record"].startswith("artifacts/provenance/records/artifact-")
        assert link["record"].endswith(".json")
        assert link["record_hash"].startswith("sha256:")
        assert (build_dir / link["record"]).is_file()


def test_histogram_loads_unlisted_axis_and_weight_fields(tmp_path: Path) -> None:
    api = _hepflow_api()
    compile_workflow_file = api.compile_workflow_file
    run_workflow_file = api.run_workflow_file
    input_path = tmp_path / "input.root"
    with uproot.recreate(input_path) as root_file:
        root_file["events"] = {
            "Muon_Pz": [10.0, 20.0, 30.0],
            "EventWeight": [1.0, 0.5, 2.0],
            "Unused": [4, 5, 6],
        }

    workflow_path = tmp_path / "workflow.yaml"
    workflow = {
        "version": "1.0",
        "use": {
            "profiles": [
                "registry",
                "fasthep_carpenter:registry",
            ],
        },
        "data": {
            "datasets": [
                {
                    "name": "sample",
                    "files": [str(input_path)],
                    "nevents": 3,
                }
            ],
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
            },
        },
        "analysis": {
            "stages": [
                {
                    "id": "MuonPz",
                    "op": "hep.hist",
                    "params": {
                        "storage": "weighted",
                        "axes": [
                            {
                                "name": "muon_pz",
                                "source": "Muon_Pz",
                                "type": "regular",
                                "bins": {"low": 0, "high": 40, "nbins": 4},
                            }
                        ],
                        "weight_expr": "EventWeight",
                    },
                }
            ],
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    plan = compile_workflow_file(workflow_path, outdir=build_dir)
    result = run_workflow_file(workflow_path, outdir=build_dir)

    assert plan.get_node("read.events").params["branches"] == [
        "EventWeight",
        "Muon_Pz",
    ]
    assert result.success is True
    assert (build_dir / "artifacts" / "histograms" / "MuonPz.pkl").exists()


def test_clean_params_collection_references_are_visible_to_data_flow(
    tmp_path: Path,
) -> None:
    workflow = {
        "version": "1.0",
        "registry": {
            "sources": {
                "root_tree": {
                    "spec": "fasthep_carpenter.sources.root_tree:ROOT_TREE_SOURCE_SPEC",
                    "impl": "fasthep_carpenter.sources.root_tree:run_root_tree_source",
                },
            },
            "transforms": {
                "hep.define": {
                    "spec": "fasthep_carpenter.operations.define:DEFINE_SPEC",
                    "impl": "fasthep_carpenter.operations.define:run_define_transform",
                },
                "hep.clean": {
                    "spec": "fasthep_carpenter.operations.clean:CLEAN_SPEC",
                    "impl": "fasthep_carpenter.operations.clean:run_clean_transform",
                },
            },
        },
        "data": {
            "datasets": [],
            "defaults": {},
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "Events",
                "stream_type": "event_stream",
            },
        },
        "analysis": {
            "stages": [
                {
                    "id": "SelectedObjects",
                    "op": "hep.define",
                    "params": {
                        "variables": [
                            {"name": "selected_photons_eta", "expr": "Photon_eta"},
                            {"name": "selected_photons_phi", "expr": "Photon_phi"},
                            {"name": "selected_photons_pt", "expr": "Photon_pt"},
                            {"name": "selected_muons_eta", "expr": "Muon_eta"},
                            {"name": "selected_muons_phi", "expr": "Muon_phi"},
                            {"name": "selected_electrons_eta", "expr": "Electron_eta"},
                            {"name": "selected_electrons_phi", "expr": "Electron_phi"},
                        ],
                    },
                },
                {
                    "id": "PhotonTightCleanAgainstLeptons",
                    "op": "hep.clean",
                    "params": {
                        "source": "selected_photons",
                        "clean_against": [
                            "selected_muons",
                            "selected_electrons",
                        ],
                        "output": "cleaned_photons",
                        "sort_by": "pt",
                        "sort_order": "descending",
                        "diagnostics": {
                            "removed_count": "nremoved_photon_overlap",
                        },
                    },
                },
            ],
        },
    }

    _graph, plan = _build_plan_from_normalized(normalize_workflow(workflow))

    assert plan.registry["transforms"]["hep.clean"]["impl"] == (
        "fasthep_carpenter.operations.clean:run_clean_transform"
    )
    assert plan.data_flow["consumers"]["selected_photons_eta"] == [
        "stage.PhotonTightCleanAgainstLeptons"
    ]
    assert plan.data_flow["consumers"]["selected_muons_eta"] == [
        "stage.PhotonTightCleanAgainstLeptons"
    ]
    assert plan.data_flow["consumers"]["selected_electrons_eta"] == [
        "stage.PhotonTightCleanAgainstLeptons"
    ]
    assert plan.data_flow["consumers"]["selected_photons_pt"] == [
        "stage.PhotonTightCleanAgainstLeptons"
    ]
    assert plan.data_flow["origins"]["cleaned_photons"] == {
        "kind": "produced",
        "node": "stage.PhotonTightCleanAgainstLeptons",
    }
    assert plan.data_flow["origins"]["nremoved_photon_overlap"] == {
        "kind": "produced",
        "node": "stage.PhotonTightCleanAgainstLeptons",
    }
    assert plan.get_node("read.events").params["branches"] == [
        "Electron_eta",
        "Electron_phi",
        "Muon_eta",
        "Muon_phi",
        "Photon_eta",
        "Photon_phi",
        "Photon_pt",
    ]


def test_manifest_path_uses_relative_path_for_output_below_outdir(
    tmp_path: Path,
) -> None:
    outdir = tmp_path / "build"
    output = outdir / "artifacts" / "files" / "skim.root"

    assert manifest_path(output, outdir) == (
        "artifacts/files/skim.root",
        "relative_to_outdir",
    )


def test_manifest_path_uses_absolute_path_for_external_output(
    tmp_path: Path,
) -> None:
    outdir = tmp_path / "build"
    output = tmp_path / "external" / "skim.root"

    assert manifest_path(output, outdir) == (
        output.resolve().as_posix(),
        "absolute",
    )


def test_root_tree_source_accepts_stream_type() -> None:
    assert (
        run_root_tree_source(
            datasets=[],
            tree="events",
            stream_type="event_stream",
        )
        == {}
    )


def test_root_tree_source_allows_remote_root_uri(
    monkeypatch,
) -> None:
    opened: list[Any] = []

    class FakeTree:
        def arrays(self, *args: Any, **kwargs: Any) -> dict[str, list[int]]:
            return {"Muon_pt": [1, 2, 3]}

    class FakeFile:
        def __enter__(self) -> dict[str, FakeTree]:
            return {"Events": FakeTree()}

        def __exit__(self, *exc_info: object) -> None:
            return None

    def fake_open(path: Any) -> FakeFile:
        opened.append(path)
        return FakeFile()

    monkeypatch.setattr("fasthep_carpenter.sources.root_tree.uproot.open", fake_open)

    result = run_root_tree_source(
        datasets=[
            {
                "name": "DoubleMuon",
                "files": ["root://example.test//store/data.root"],
            }
        ],
        tree="Events",
    )

    assert opened == ["root://example.test//store/data.root"]
    assert isinstance(result, dict)
    assert "DoubleMuon" in result


def test_root_tree_source_metadata_only_does_not_call_arrays(monkeypatch) -> None:
    class FakeTree:
        num_entries = 42

        def keys(self) -> list[str]:
            return ["Muon_pt", "Muon_eta"]

        def typenames(self) -> dict[str, str]:
            return {"Muon_pt": "float[]", "Muon_eta": "float[]"}

        def interpretations(self) -> dict[str, str]:
            return {
                "Muon_pt": "AsJagged(AsDtype('>f4'))",
                "Muon_eta": "AsJagged(AsDtype('>f4'))",
            }

        def arrays(self, *args: Any, **kwargs: Any) -> None:
            raise AssertionError("metadata-only schema inspection must not call arrays()")

    class FakeFile:
        def __enter__(self) -> dict[str, FakeTree]:
            return {"Events": FakeTree()}

        def __exit__(self, *exc_info: object) -> None:
            return None

    monkeypatch.setattr(
        "fasthep_carpenter.sources.root_tree.uproot.open",
        lambda path: FakeFile(),
    )

    result = run_root_tree_source(
        datasets=[],
        tree="Events",
        branches=["Muon_pt"],
        start=0,
        stop=10,
        metadata_only=True,
        ctx={
            "partition": {
                "file": "root://example.test//store/data.root",
                "start": None,
                "stop": None,
            }
        },
    )

    assert isinstance(result, RootTreeSchema)
    assert result.fields == ["Muon_pt"]
    assert result.awkward_type == {"Muon_pt": "float[]"}
    assert result.entry_count == 10


def test_root_tree_source_partition_null_range_keeps_source_range(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class FakeTree:
        def arrays(self, *args: Any, **kwargs: Any) -> ak.Array:
            calls.append(dict(kwargs))
            return ak.Array({"Muon_pt": [1.0, 2.0]})

    class FakeFile:
        def __enter__(self) -> dict[str, FakeTree]:
            return {"Events": FakeTree()}

        def __exit__(self, *exc_info: object) -> None:
            return None

    monkeypatch.setattr(
        "fasthep_carpenter.sources.root_tree.uproot.open",
        lambda path: FakeFile(),
    )

    run_root_tree_source(
        datasets=[],
        tree="Events",
        branches=["Muon_pt"],
        start=0,
        stop=1000,
        ctx={
            "partition": {
                "file": "root://example.test//store/data.root",
                "start": None,
                "stop": None,
            }
        },
    )

    assert calls == [
        {"entry_start": 0, "entry_stop": 1000, "library": "ak"},
    ]


def test_root_tree_source_can_ignore_missing_requested_branches(tmp_path: Path) -> None:
    input_path = tmp_path / "input.root"
    with uproot.recreate(input_path) as root_file:
        root_file["Events"] = ak.Array({"Muon_pt": [1.0, 2.0]})

    strict_ctx = {
        "partition": {
            "file": str(input_path),
            "start": None,
            "stop": None,
        }
    }
    result = run_root_tree_source(
        datasets=[],
        tree="Events",
        branches=["Muon_pt", "GenJetAK8_eta"],
        missing_branches="ignore",
        ctx=strict_ctx,
    )

    assert isinstance(result, ak.Array)
    assert result.fields == ["Muon_pt"]
    assert ak.to_list(result["Muon_pt"]) == [1.0, 2.0]


def _hepflow_api() -> Any:
    return import_module("hepflow.api")


def _build_plan_from_normalized(normalized: dict[str, Any]) -> Any:
    return import_module("hepflow.compiler.plan").build_plan_from_normalized(normalized)


def _compile_merge_workflow(
    tmp_path: Path,
    *,
    left_field: str,
    right_field: str,
    on_conflict: str = "keep_first",
) -> Any:
    workflow = _merge_workflow(
        left_field=left_field,
        right_field=right_field,
        on_conflict=on_conflict,
    )
    return _compile_workflow_dict(tmp_path, workflow)


def _compile_workflow_dict(tmp_path: Path, workflow: dict[str, Any]) -> Any:
    compile_workflow_file = _hepflow_api().compile_workflow_file
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def _merge_workflow(
    *,
    left_field: str,
    right_field: str,
    on_conflict: str = "keep_first",
    left_op: str = "hep.define",
    right_op: str = "hep.define",
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "use": {
            "profiles": [
                "registry",
                "fasthep_carpenter:registry",
            ],
        },
        "data": {
            "datasets": [
                {"name": "sample", "files": ["sample.root"], "eventtype": "mc"}
            ]
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "Events",
                "stream_type": "event_stream",
                "branches": ["Muon_pt"],
            },
        },
        "analysis": {
            "stages": [
                _define_stage("Prepare", "base_pt", op="hep.define"),
                _define_stage("Left", left_field, op=left_op, upstream="Prepare"),
                _define_stage("Right", right_field, op=right_op, upstream="Prepare"),
                {
                    "id": "Merge",
                    "op": "hep.merge_fields",
                    "from": [
                        {"node": "Left", "as": "left"},
                        {"node": "Right", "as": "right"},
                    ],
                    "params": {"on_conflict": on_conflict},
                },
            ],
        },
    }


def _define_stage(
    node_id: str,
    field: str,
    *,
    op: str,
    upstream: str | None = None,
) -> dict[str, Any]:
    stage: dict[str, Any] = {
        "id": node_id,
        "op": op,
        "params": {
            "variables": [
                {
                    "name": field,
                    "expr": "Muon_pt",
                }
            ]
        },
    }
    if upstream is not None:
        stage["from"] = upstream
    return stage


def _logical_origin_nodes(plan: Any, field: str) -> set[str]:
    origin = plan.data_flow["origins"][field]
    if origin.get("kind") != "stream_scoped":
        return {str(origin.get("node"))}
    return {str(item.get("node")) for item in origin["origins"]}
