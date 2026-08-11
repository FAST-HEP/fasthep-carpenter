from __future__ import annotations

from pathlib import Path

import awkward as ak
import yaml
from hepflow.api import compile_workflow_file
from hepflow.registry.loaders import load_object
from hepflow.utils import read_yaml

from fasthep_carpenter.operations.weights.lookup_csv import apply_lookup_csv
from fasthep_carpenter.operations.weights.pdf_envelope import apply_pdf_envelope


def test_weight_operations_resolve_from_registry() -> None:
    lookup_spec = load_object(
        "fasthep_carpenter.operations.weights.lookup_csv:LOOKUP_CSV_SPEC"
    )
    lookup_impl = load_object(
        "fasthep_carpenter.operations.weights.lookup_csv:run_lookup_csv_transform"
    )
    pdf_spec = load_object(
        "fasthep_carpenter.operations.weights.pdf_envelope:PDF_ENVELOPE_SPEC"
    )
    pdf_impl = load_object(
        "fasthep_carpenter.operations.weights.pdf_envelope:run_pdf_envelope_transform"
    )

    assert lookup_spec["name"] == "hep.weights.lookup_csv"
    assert callable(lookup_impl)
    assert pdf_spec["name"] == "hep.weights.pdf_envelope"
    assert callable(pdf_impl)


def test_lookup_csv_produces_nominal_up_down_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "trigger_efficiency.csv"
    csv_path.write_text(
        "\n".join(
            [
                "pt_min,pt_max,trigger_eff,trigger_eff_up,trigger_eff_down",
                "0,25,0.90,0.95,0.85",
                "25,100,0.80,0.88,0.72",
            ]
        ),
        encoding="utf-8",
    )
    events = ak.Array({"Muon_Pt": [12.0, 30.0]})

    out = apply_lookup_csv(
        events,
        path=str(csv_path),
        variable="Muon_Pt",
        bins={"column": "pt"},
        values={
            "nominal": "trigger_eff",
            "up": "trigger_eff_up",
            "down": "trigger_eff_down",
        },
        outputs={
            "nominal": "TriggerEffWeight",
            "up": "TriggerEffWeight_up",
            "down": "TriggerEffWeight_down",
        },
    )

    assert ak.to_list(out.TriggerEffWeight) == [0.9, 0.8]
    assert ak.to_list(out.TriggerEffWeight_up) == [0.95, 0.88]
    assert ak.to_list(out.TriggerEffWeight_down) == [0.85, 0.72]


def test_pdf_envelope_produces_up_down_fields() -> None:
    events = ak.Array({"LHEPdfWeight": [[0.98, 1.04, 1.01], [0.91, 1.07, 1.02]]})

    out = apply_pdf_envelope(
        events,
        inputs="LHEPdfWeight",
        outputs={"up": "ttbar_pdf_weight_up", "down": "ttbar_pdf_weight_down"},
    )

    assert ak.to_list(out.ttbar_pdf_weight_up) == [1.04, 1.07]
    assert ak.to_list(out.ttbar_pdf_weight_down) == [0.98, 0.91]


def test_weight_outputs_can_be_used_by_systematic_weight_multiply(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "trigger_efficiency.csv"
    csv_path.write_text(
        "\n".join(
            [
                "pt_min,pt_max,trigger_eff,trigger_eff_up,trigger_eff_down",
                "0,100,0.90,0.95,0.85",
            ]
        ),
        encoding="utf-8",
    )
    workflow_path = tmp_path / "workflow.yaml"
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
                    "id": "TriggerEfficiencyWeights",
                    "op": "hep.weights.lookup_csv",
                    "params": {
                        "path": str(csv_path),
                        "variable": "Muon_Pt",
                        "bins": {"column": "pt"},
                        "values": {
                            "nominal": "trigger_eff",
                            "up": "trigger_eff_up",
                            "down": "trigger_eff_down",
                        },
                        "outputs": {
                            "nominal": "TriggerEffWeight",
                            "up": "TriggerEffWeight_up",
                            "down": "TriggerEffWeight_down",
                        },
                    },
                },
                {
                    "id": "WeightedHist",
                    "op": "hep.hist",
                    "params": {
                        "axes": [
                            {
                                "name": "pt",
                                "type": "regular",
                                "source": "Muon_Pt",
                                "bins": {"nbins": 10, "low": 0, "high": 100},
                            }
                        ],
                        "weight_expr": "TriggerEffWeight",
                    },
                },
            ]
        },
        "systematics": {
            "include_nominal": True,
            "variations": [
                {
                    "name": "trigger_eff_up",
                    "group": "trigger_eff",
                    "direction": "up",
                    "requires": ["stage.TriggerEfficiencyWeights"],
                    "weight": {"multiply": "TriggerEffWeight_up"},
                }
            ],
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    plan = read_yaml(tmp_path / "build" / "compile" / "trigger_eff_up" / "plan.yaml")
    hist_node = next(node for node in plan["nodes"] if node["id"] == "stage.WeightedHist")
    assert hist_node["params"]["weight_expr"] == (
        "(TriggerEffWeight) * (TriggerEffWeight_up)"
    )


def test_minimal_workflow_compiles_with_pdf_envelope(tmp_path: Path) -> None:
    workflow_path = tmp_path / "workflow.yaml"
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
                    "id": "PDFWeights",
                    "op": "hep.weights.pdf_envelope",
                    "params": {
                        "inputs": "LHEPdfWeight",
                        "outputs": {
                            "up": "ttbar_pdf_weight_up",
                            "down": "ttbar_pdf_weight_down",
                        },
                    },
                },
            ]
        },
    }
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert plan.get_node("stage.PDFWeights").impl == "hep.weights.pdf_envelope"
