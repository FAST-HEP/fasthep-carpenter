from __future__ import annotations

from pathlib import Path

import awkward as ak
import numpy as np
import pytest
import uproot

import fasthep_carpenter.sinks.root_tree as root_tree_sink
from fasthep_carpenter.sinks.root_tree import run_root_tree_write


def test_default_root_tree_writer_outputs_rntuple(tmp_path: Path) -> None:
    result = run_root_tree_write(_payload(), path=str(tmp_path / "default.root"))
    reference = getattr(result, "uri", result.path)

    assert reference == str(tmp_path / "default.root")
    assert getattr(result, "output_name", None) == "artifact"
    assert getattr(result, "dataset_name", None) == "dataset"
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


def test_root_tree_writer_does_not_reopen_output_to_build_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_open(*args, **kwargs):
        del args, kwargs
        raise AssertionError("writer should not reopen artifact")

    monkeypatch.setattr(root_tree_sink.uproot, "open", fail_open)

    result = run_root_tree_write(_payload(), path=str(tmp_path / "reference.root"))

    assert result.metadata["root_classname"] == "ROOT::RNTuple"


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


def _collection_payload() -> ak.Array:
    return ak.Array(
        {
            "event": [1, 2, 3],
            "SR_pt": [[1.0, 2.0], [3.0], []],
            "SR_eta": [[0.1, 0.2], [0.3], []],
            "SR_selection": [[True, False], [True], []],
            "Jet_pt": [[10.0], [], [20.0, 30.0]],
        }
    )


def test_ttree_writer_gives_each_jagged_branch_its_own_counter_by_default(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "default_counters.root"

    run_root_tree_write(
        _collection_payload(), path=str(output_path), tree="Events", format="ttree"
    )

    with uproot.open(output_path) as root_file:
        keys = set(root_file["Events"].keys())
    assert {"nSR_pt", "nSR_eta", "nJet_pt"} <= keys
    assert "nSR" not in keys


def test_shared_counters_write_one_counter_per_collection(tmp_path: Path) -> None:
    output_path = tmp_path / "shared_counters.root"

    result = run_root_tree_write(
        _collection_payload(),
        path=str(output_path),
        tree="Events",
        format="ttree",
        shared_counters=True,
    )

    with uproot.open(output_path) as root_file:
        tree = root_file["Events"]
        keys = set(tree.keys())
        assert {"event", "nSR", "SR_pt", "SR_eta", "SR_selection", "nJet", "Jet_pt"} == keys
        assert tree["nSR"].array(library="np").dtype == np.dtype("int32")
        assert tree["nSR"].array(library="np").tolist() == [2, 1, 0]
        assert tree["nJet"].array(library="np").tolist() == [1, 0, 2]
        assert tree["SR_pt"].array(library="ak").to_list() == [[1.0, 2.0], [3.0], []]
        assert tree["SR_selection"].array(library="ak").to_list() == [
            [True, False],
            [True],
            [],
        ]
        assert tree["Jet_pt"].array(library="ak").to_list() == [[10.0], [], [20.0, 30.0]]
    assert result.metadata["branches"] == [
        "event",
        "SR_pt",
        "SR_eta",
        "SR_selection",
        "Jet_pt",
    ]


def test_shared_counters_keep_own_counter_when_lengths_differ(tmp_path: Path) -> None:
    payload = ak.Array(
        {
            "SR_pt": [[1.0, 2.0], [3.0]],
            "SR_eta": [[0.1, 0.2], [0.3]],
            "SR_other": [[1.0], [2.0]],
        }
    )
    output_path = tmp_path / "mixed_counters.root"

    run_root_tree_write(
        payload,
        path=str(output_path),
        tree="Events",
        format="ttree",
        shared_counters=True,
    )

    with uproot.open(output_path) as root_file:
        tree = root_file["Events"]
        assert {"nSR", "SR_pt", "SR_eta", "nSR_other", "SR_other"} == set(tree.keys())
        assert tree["SR_other"].array(library="ak").to_list() == [[1.0], [2.0]]


def test_shared_counters_leave_scalar_branches_alone(tmp_path: Path) -> None:
    output_path = tmp_path / "scalars.root"

    run_root_tree_write(
        ak.Array({"weight_pu": [1.0, 2.0], "run_number": [1, 1]}),
        path=str(output_path),
        tree="Events",
        format="ttree",
        shared_counters=True,
    )

    with uproot.open(output_path) as root_file:
        assert set(root_file["Events"].keys()) == {"weight_pu", "run_number"}


def test_shared_counters_require_the_ttree_format(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="shared_counters"):
        run_root_tree_write(
            _collection_payload(),
            path=str(tmp_path / "rntuple.root"),
            tree="Events",
            format="rntuple",
            shared_counters=True,
        )


def test_only_numeric_jagged_branches_are_grouped_under_a_counter() -> None:
    assert root_tree_sink._is_jagged_numeric(ak.Array([[1.0], []])) is True
    assert root_tree_sink._is_jagged_numeric(ak.Array([["a"], ["b"]])) is False
    assert root_tree_sink._is_jagged_numeric(ak.Array([1.0, 2.0])) is False


def test_shared_counters_reuse_an_existing_counter_branch_of_the_same_lengths(
    tmp_path: Path,
) -> None:
    # Compatibility schemas can already carry the legacy counter (nSR) as a field.
    payload = ak.with_field(
        _collection_payload(), ak.Array(np.array([2, 1, 0], dtype=np.int32)), "nSR"
    )
    output_path = tmp_path / "existing_counter.root"

    run_root_tree_write(
        payload,
        path=str(output_path),
        tree="Events",
        format="ttree",
        shared_counters=True,
    )

    with uproot.open(output_path) as root_file:
        tree = root_file["Events"]
        assert {"event", "nSR", "SR_pt", "SR_eta", "SR_selection", "nJet", "Jet_pt"} == set(
            tree.keys()
        )
        assert tree["nSR"].array(library="np").tolist() == [2, 1, 0]


def test_shared_counters_keep_an_existing_branch_that_is_not_the_counter(
    tmp_path: Path,
) -> None:
    payload = ak.with_field(
        _collection_payload(), ak.Array(np.array([7, 7, 7], dtype=np.int32)), "nSR"
    )
    output_path = tmp_path / "unrelated_n_branch.root"

    run_root_tree_write(
        payload,
        path=str(output_path),
        tree="Events",
        format="ttree",
        shared_counters=True,
    )

    with uproot.open(output_path) as root_file:
        tree = root_file["Events"]
        assert tree["nSR"].array(library="np").tolist() == [7, 7, 7]
        assert tree["SR_pt"].array(library="ak").to_list() == [[1.0, 2.0], [3.0], []]
