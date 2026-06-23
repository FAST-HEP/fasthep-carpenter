from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import awkward as ak
import uproot
import yaml
from hepflow.api import compile_author_file, run_author_file

from fasthep_carpenter.sinks.root_tree import manifest_path
from fasthep_carpenter.sources.root_tree import (
    RootTreeSchema,
    run_root_tree_source,
)


def test_compile_resolves_root_tree_source_from_carpenter_profile(tmp_path: Path) -> None:
    author_path = tmp_path / "author.yaml"
    author = {
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
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")

    plan = compile_author_file(author_path, outdir=tmp_path / "build")

    assert plan.registry["sources"]["root_tree"]["impl"] == (
        "fasthep_carpenter.sources.root_tree:run_root_tree_source"
    )
    assert plan.registry["transforms"]["hep.define"]["impl"] == (
        "fasthep_carpenter.operations.define:run_define_transform"
    )
    assert "read.events" in {node.id for node in plan.nodes}


def test_attached_root_tree_writer_produces_output_artifact(tmp_path: Path) -> None:
    input_path = tmp_path / "input.root"
    with uproot.recreate(input_path) as root_file:
        root_file["events"] = {"Muon_Pt": [1, 2, 3]}

    author_path = tmp_path / "author.yaml"
    author = {
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
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    result = run_author_file(author_path, outdir=build_dir, chunk_size=2)

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
    assert manifest == {
        "kind": "root_tree",
        "name": "skim",
        "node_id": "write.DerivedValue.0",
        "input_node": "stage.DerivedValue",
        "tree": "events",
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
                    },
                    {
                        "path": "artifacts/files/skim/sample/0_1.root",
                        "path_type": "relative_to_outdir",
                        "dataset": "sample",
                        "partition": 1,
                        "attempt": 0,
                        "entries": 1,
                        "size_bytes": second_output_path.stat().st_size,
                    },
                ],
            }
        },
    }


def test_histogram_loads_unlisted_axis_and_weight_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "input.root"
    with uproot.recreate(input_path) as root_file:
        root_file["events"] = {
            "Muon_Pz": [10.0, 20.0, 30.0],
            "EventWeight": [1.0, 0.5, 2.0],
            "Unused": [4, 5, 6],
        }

    author_path = tmp_path / "author.yaml"
    author = {
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
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    plan = compile_author_file(author_path, outdir=build_dir)
    result = run_author_file(author_path, outdir=build_dir)

    assert plan.get_node("read.events").params["branches"] == [
        "EventWeight",
        "Muon_Pz",
    ]
    assert result.success is True
    assert (build_dir / "artifacts" / "histograms" / "MuonPz.pkl").exists()


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
