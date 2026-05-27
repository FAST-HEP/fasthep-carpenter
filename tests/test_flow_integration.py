from __future__ import annotations

from pathlib import Path

import yaml
from fasthep_carpenter.impl.read.root_tree import run_root_tree_source
from hepflow.api import compile_author_file


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
        "fasthep_carpenter.impl.read.root_tree:run_root_tree_source"
    )
    assert plan.registry["transforms"]["hep.define"]["impl"] == (
        "fasthep_carpenter.impl.define:run_define_transform"
    )
    assert "read.events" in {node.id for node in plan.nodes}


def test_root_tree_source_accepts_stream_type() -> None:
    assert (
        run_root_tree_source(
            datasets=[],
            tree="events",
            stream_type="event_stream",
        )
        == {}
    )
