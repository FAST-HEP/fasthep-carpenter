from __future__ import annotations

import json
from pathlib import Path

from hepflow.model.plan import ExecutionNode
from hepflow.utils import read_pickle

from fasthep_carpenter.products import (
    materialize_cutflow_product,
    materialize_histogram_product,
    merge_cutflow_products,
)


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
