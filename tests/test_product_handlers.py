from __future__ import annotations

import json
from pathlib import Path

import awkward as ak
from hepflow.model.plan import ExecutionNode
from hepflow.utils import read_pickle

from fasthep_carpenter.products import (
    materialize_cutflow_product,
    materialize_histogram_product,
    merge_cutflow_products,
    merge_event_streams,
)


def test_event_stream_handler_concatenates_dataset_partitions() -> None:
    first = ak.Array({"value": [1, 2]})
    second = ak.Array({"value": [3]})

    merged = merge_event_streams(
        [first, second],
        node=None,
        output_name="stream",
        dataset_name="data",
    )

    assert ak.to_list(merged) == [
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]


def test_event_stream_handler_preserves_cross_dataset_values() -> None:
    values = [ak.Array([1]), ak.Array([2])]

    merged = merge_event_streams(
        values,
        node=None,
        output_name="stream",
        dataset_name=None,
    )

    assert isinstance(merged, list)
    assert merged[0] is values[0]
    assert merged[1] is values[1]


def test_histogram_product_handler_materializes_pickle_and_manifest(
    tmp_path: Path,
) -> None:
    node = ExecutionNode(
        id="stage.NumberMuons",
        graph_node_id="stage.NumberMuons",
        role="transform",
        impl="hep.hist",
        outputs={"hist": "histogram"},
        meta={"stage_id": "NumberMuons"},
    )

    result = materialize_histogram_product(
        {"bins": [1, 2, 3]},
        node=node,
        output_name="hist",
        outdir=tmp_path,
    )

    assert read_pickle(tmp_path / "artifacts" / "histograms" / "NumberMuons.pkl") == {
        "bins": [1, 2, 3]
    }
    assert result["items"] == [
        {
            "id": "NumberMuons",
            "path": "artifacts/histograms/NumberMuons.pkl",
            "producer": "stage.NumberMuons",
        }
    ]
    assert json.loads(
        (tmp_path / "artifacts" / "histograms" / "manifest.json").read_text(
            encoding="utf-8"
        )
    ) == {"histograms": result["items"]}


def test_cutflow_product_handler_merges_and_materializes_canonical_graph(
    tmp_path: Path,
) -> None:
    node = ExecutionNode(
        id="stage.EventSelection",
        graph_node_id="stage.EventSelection",
        role="transform",
        impl="hep.selection.cutflow",
        outputs={"cutflow": "cutflow"},
        params={
            "selection": {
                "All": [
                    "NIsoMuon >= 2",
                    {"reduce": {"op": "any", "over": "Muon_Pt > 25"}},
                ]
            }
        },
        meta={"stage_id": "EventSelection"},
    )
    merged = merge_cutflow_products(
        [
            {
                "cuts": [
                    {
                        "name": "All[0]",
                        "n_in": 10.0,
                        "n_out": 5.0,
                        "n_unweighted_in": 20,
                        "n_unweighted_out": 10,
                        "sumw_in": 10.0,
                        "sumw_out": 5.0,
                        "sumw2_in": 8.0,
                        "sumw2_out": 4.0,
                    }
                ]
            },
            {
                "cuts": [
                    {
                        "name": "All[0]",
                        "n_in": 2.0,
                        "n_out": 1.0,
                        "n_unweighted_in": 4,
                        "n_unweighted_out": 2,
                        "sumw_in": 2.0,
                        "sumw_out": 1.0,
                        "sumw2_in": 1.5,
                        "sumw2_out": 0.5,
                    }
                ]
            },
        ],
        node=node,
        output_name="cutflow",
        dataset_name="data",
    )

    result = materialize_cutflow_product(
        {"cutflows": [merged]},
        node=node,
        output_name="cutflow",
        outdir=tmp_path,
    )

    payload = json.loads(
        (tmp_path / "artifacts" / "cutflows" / "EventSelection.json").read_text(
            encoding="utf-8"
        )
    )
    assert result["value"] == payload
    assert payload["kind"] == "cutflow"
    assert payload["nodes"][0]["label"] == "NIsoMuon >= 2"
    assert payload["nodes"][0]["stats"]["data"] == {
        "n_in": 12.0,
        "n_out": 6.0,
        "n_unweighted_in": 24,
        "n_unweighted_out": 12,
        "sumw_in": 12.0,
        "sumw_out": 6.0,
        "sumw2_in": 9.5,
        "sumw2_out": 4.5,
    }
    assert json.loads(
        (tmp_path / "artifacts" / "cutflows" / "manifest.json").read_text(
            encoding="utf-8"
        )
    ) == {"cutflows": result["items"]}


def test_cutflow_product_handler_preserves_dataset_identity_at_final_merge() -> None:
    node = _cutflow_node()

    merged = merge_cutflow_products(
        [
            {
                "dataset": "data",
                "cuts": [
                    {
                        "name": "All[0]",
                        "n_in": 10.0,
                        "n_out": 5.0,
                        "n_unweighted_in": 10,
                        "n_unweighted_out": 5,
                    }
                ],
            },
            {
                "dataset": "dy",
                "cuts": [
                    {
                        "name": "All[0]",
                        "n_in": 20.0,
                        "n_out": 12.0,
                        "n_unweighted_in": 40,
                        "n_unweighted_out": 24,
                    }
                ],
            },
        ],
        node=node,
        output_name="cutflow",
    )

    assert merged["datasets"] == ["data", "dy"]
    assert "default" not in merged["datasets"]
    assert merged["nodes"][0]["stats"]["data"]["n_out"] == 5.0
    assert merged["nodes"][0]["stats"]["dy"]["n_out"] == 12.0


def test_cutflow_product_handler_merges_canonical_graphs_by_dataset() -> None:
    node = _cutflow_node()

    merged = merge_cutflow_products(
        [
            _canonical_cutflow(
                dataset="data",
                n_in=10.0,
                n_out=5.0,
                n_unweighted_in=10,
                n_unweighted_out=5,
            ),
            _canonical_cutflow(
                dataset="data",
                n_in=2.0,
                n_out=1.5,
                n_unweighted_in=4,
                n_unweighted_out=3,
            ),
            _canonical_cutflow(
                dataset="dy",
                n_in=20.0,
                n_out=12.0,
                n_unweighted_in=40,
                n_unweighted_out=24,
            ),
        ],
        node=node,
        output_name="cutflow",
    )

    assert merged["version"] == "1.0"
    assert merged["kind"] == "cutflow"
    assert merged["producer"] == "stage.EventSelection"
    assert merged["datasets"] == ["data", "dy"]
    assert "default" not in merged["datasets"]
    assert merged["edges"] == [
        {"source": "All[0]", "target": "All[1]", "kind": "sequence"}
    ]
    stats = merged["nodes"][0]["stats"]
    assert stats["data"] == {
        "n_in": 12.0,
        "n_out": 6.5,
        "n_unweighted_in": 14,
        "n_unweighted_out": 8,
        "sumw_in": 12.0,
        "sumw_out": 6.5,
        "sumw2_in": 14.0,
        "sumw2_out": 8.0,
    }
    assert stats["dy"]["n_out"] == 12.0


def _cutflow_node() -> ExecutionNode:
    return ExecutionNode(
        id="stage.EventSelection",
        graph_node_id="stage.EventSelection",
        role="transform",
        impl="hep.selection.cutflow",
        outputs={"cutflow": "cutflow"},
        params={"selection": {"All": ["NIsoMuon >= 2", "Muon_Pt > 25"]}},
        meta={"stage_id": "EventSelection"},
    )


def _canonical_cutflow(
    *,
    dataset: str,
    n_in: float,
    n_out: float,
    n_unweighted_in: int,
    n_unweighted_out: int,
) -> dict[str, object]:
    return {
        "version": "1.0",
        "kind": "cutflow",
        "producer": "stage.EventSelection",
        "datasets": [dataset],
        "nodes": [
            {
                "id": "All[0]",
                "selection": "All",
                "index": 0,
                "label": "NIsoMuon >= 2",
                "expr": "NIsoMuon >= 2",
                "kind": "expression",
                "parents": [],
                "stats": {
                    dataset: {
                        "n_in": n_in,
                        "n_out": n_out,
                        "n_unweighted_in": n_unweighted_in,
                        "n_unweighted_out": n_unweighted_out,
                        "sumw_in": n_in,
                        "sumw_out": n_out,
                        "sumw2_in": float(n_unweighted_in),
                        "sumw2_out": float(n_unweighted_out),
                    }
                },
            },
            {
                "id": "All[1]",
                "selection": "All",
                "index": 1,
                "label": "Muon_Pt > 25",
                "expr": "Muon_Pt > 25",
                "kind": "expression",
                "parents": ["All[0]"],
                "stats": {
                    dataset: {
                        "n_in": n_out,
                        "n_out": n_out,
                        "n_unweighted_in": n_unweighted_out,
                        "n_unweighted_out": n_unweighted_out,
                        "sumw_in": n_out,
                        "sumw_out": n_out,
                        "sumw2_in": float(n_unweighted_out),
                        "sumw2_out": float(n_unweighted_out),
                    }
                },
            },
        ],
        "edges": [{"source": "All[0]", "target": "All[1]", "kind": "sequence"}],
    }
