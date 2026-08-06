from __future__ import annotations

from pathlib import Path

import awkward as ak
import numpy as np
import pytest
import uproot

from fasthep_carpenter.sinks.root_tree import run_root_tree_write


def test_default_root_tree_writer_outputs_rntuple(tmp_path: Path) -> None:
    result = run_root_tree_write(_payload(), path=str(tmp_path / "default.root"))

    assert result.metadata["format"] == "rntuple"
    assert result.metadata["root_classname"] == "ROOT::RNTuple"
    assert _classname(tmp_path / "default.root") == "ROOT::RNTuple"


@pytest.mark.parametrize(
    ("output_format", "classname"),
    [
        ("rntuple", "ROOT::RNTuple"),
        ("ttree", "TTree"),
    ],
)
def test_explicit_root_tree_format_round_trips_scalar_and_jagged_branches(
    tmp_path: Path,
    output_format: str,
    classname: str,
) -> None:
    output_path = tmp_path / f"{output_format}.root"

    result = run_root_tree_write(
        _payload(),
        path=str(output_path),
        tree="Events",
        format=output_format,
    )

    assert result.path == str(output_path)
    assert result.metadata["format"] == output_format
    assert result.metadata["root_classname"] == classname
    assert result.metadata["branches"] == [
        "int32_branch",
        "float64_branch",
        "jagged_branch",
    ]
    manifest = result.metadata["writer_manifest"]
    assert manifest["format"] == output_format
    assert manifest["root_classname"] == classname

    with uproot.open(output_path) as root_file:
        tree = root_file["Events"]
        assert tree.classname == classname
        keys = set(tree.keys())
        assert {"int32_branch", "float64_branch", "jagged_branch"} <= keys
        assert tree["int32_branch"].array(library="np").dtype == np.dtype("int32")
        assert tree["float64_branch"].array(library="np").dtype == np.dtype("float64")
        assert tree["int32_branch"].array(library="np").tolist() == [1, 2, 3]
        assert tree["float64_branch"].array(library="np").tolist() == [
            1.5,
            2.5,
            3.5,
        ]
        assert ak.to_list(tree["jagged_branch"].array()) == [[1.0, 2.0], [], [3.0]]


def test_root_tree_writer_rejects_invalid_format(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported ROOT output format"):
        run_root_tree_write(_payload(), path=str(tmp_path / "bad.root"), format="root")


def _payload() -> ak.Array:
    return ak.Array(
        {
            "int32_branch": np.array([1, 2, 3], dtype=np.int32),
            "float64_branch": np.array([1.5, 2.5, 3.5], dtype=np.float64),
            "jagged_branch": [[1.0, 2.0], [], [3.0]],
        }
    )


def _classname(path: Path) -> str:
    with uproot.open(path) as root_file:
        return str(root_file["events"].classname)
