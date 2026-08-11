from __future__ import annotations

from importlib import import_module
from pathlib import Path

import awkward as ak
import pytest
import yaml
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)
from hepflow.model.data_flow import DataDependencyResult
from hepflow.runtime import ComponentContext

from fasthep_carpenter.operations.select_objects import (
    SELECT_OBJECTS_SPEC,
    resolve_collection_expression,
    run_select_objects,
)


def test_dependencies_include_selection_keep_default_sort_and_derived() -> None:
    deps = _dependencies(
        {
            "collection": "Electron",
            "output": "selected_tight_Electron",
            "selection": ["pt >= 40", "abs(eta) <= 2.5"],
            "keep": ["eta", "etaSC", "charge"],
            "derived": {"etaSC": "eta + deltaEtaSC"},
        }
    )

    assert deps == DataDependencyResult(
        consumes={
            "Electron_pt",
            "Electron_eta",
            "Electron_deltaEtaSC",
            "Electron_charge",
        },
        produces={
            "nselected_tight_Electron",
            "selected_tight_Electron_eta",
            "selected_tight_Electron_etaSC",
            "selected_tight_Electron_charge",
        },
    )


def test_dependencies_treat_cosh_as_function_for_phase_space_guards() -> None:
    deps = _dependencies(
        {
            "collection": "Jet",
            "output": "selected_Jet",
            "selection": ["Nominal_pt * cosh(eta) < 6800"],
            "keep": ["pt", "eta"],
            "derived": {"pt": "Nominal_pt"},
        }
    )

    assert "Jet_cosh" not in deps.consumes
    assert deps.consumes == {"Jet_Nominal_pt", "Jet_eta"}


def test_sort_field_is_not_implicitly_provided() -> None:
    deps = _dependencies(
        {
            "collection": "Muon",
            "output": "selected_Muon",
            "selection": ["eta > 0"],
            "keep": ["eta"],
        }
    )

    assert "Muon_pt" in deps.consumes
    assert deps.produces == {"nselected_Muon", "selected_Muon_eta"}


def test_collection_relative_expression_resolution_allows_region_cuts() -> None:
    assert resolve_collection_expression(
        "((isEB == 1) & (abs(sieie) <= 0.011)) | ((isEB == 0) & (abs(sieie) <= 0.030))",
        collection="Electron",
    ) == (
        "(Electron_isEB == 1) & (abs(Electron_sieie) <= 0.011) | "
        "(Electron_isEB == 0) & (abs(Electron_sieie) <= 0.03)"
    )


def test_multiple_expressions_are_combined_with_and() -> None:
    out = run_select_objects(
        stream=_muon_events(),
        collection="Muon",
        output="selected_Muon",
        selection=["pt >= 20", "abs(eta) <= 2.4", "tight == 1"],
        keep=["pt", "eta", "tight"],
    )

    assert ak.to_list(out.nselected_Muon) == [1, 0, 1]
    assert ak.to_list(out.selected_Muon_pt) == [[25.0], [], [22.0]]


def test_jagged_empty_events_and_empty_collections_are_preserved() -> None:
    out = run_select_objects(
        stream=ak.Array(
            {
                "Jet_pt": [[30.0], [], [10.0, 40.0]],
                "Jet_eta": [[0.1], [], [0.2, 0.3]],
            }
        ),
        collection="Jet",
        output="selected_Jet",
        selection=["pt >= 20"],
        keep=["eta"],
    )

    assert ak.to_list(out.nselected_Jet) == [1, 0, 1]
    assert ak.to_list(out.selected_Jet_eta) == [[0.1], [], [0.3]]
    assert "selected_Jet_pt" not in out.fields


def test_retains_arbitrary_configured_fields_and_count() -> None:
    out = run_select_objects(
        stream=_muon_events(),
        collection="Muon",
        output="selected_loose_Muon",
        selection=["pt >= 5"],
        keep=["pt", "eta", "isGlobal"],
    )

    assert ak.to_list(out.nselected_loose_Muon) == [2, 1, 2]
    assert ak.to_list(out.selected_loose_Muon_isGlobal) == [
        [True, False],
        [True],
        [True, False],
    ]


def test_default_descending_pt_sorting() -> None:
    out = run_select_objects(
        stream=_muon_events(),
        collection="Muon",
        output="selected_Muon",
        selection=["pt >= 5"],
        keep=["eta"],
    )

    assert ak.to_list(out.selected_Muon_eta) == [[0.1, -2.3], [0.1], [0.3, 0.2]]


def test_explicit_sorting_override() -> None:
    out = run_select_objects(
        stream=_muon_events(),
        collection="Muon",
        output="selected_Muon",
        selection=["pt >= 5"],
        keep=["pt", "eta"],
        sort={"by": "eta", "order": "ascending"},
    )

    assert ak.to_list(out.selected_Muon_eta) == [[-2.3, 0.1], [0.1], [0.2, 0.3]]


def test_sorting_can_be_disabled_explicitly() -> None:
    out = run_select_objects(
        stream=_muon_events(),
        collection="Muon",
        output="selected_Muon",
        selection=["pt >= 5"],
        keep=["pt", "eta"],
        sort=False,
    )

    assert ak.to_list(out.selected_Muon_pt) == [[25.0, 6.0], [30.0], [21.0, 22.0]]


def test_missing_required_field_fails_clearly() -> None:
    events = ak.without_field(_muon_events(), "Muon_promptMVA")

    with pytest.raises(KeyError, match="Muon_promptMVA"):
        run_select_objects(
            stream=events,
            collection="Muon",
            output="selected_tight_Muon",
            selection=["promptMVA >= 0.64"],
            keep=["pt"],
        )


def test_no_overlap_cleaning_behaviour_occurs() -> None:
    out = run_select_objects(
        stream=ak.Array(
            {
                "Muon_pt": [[25.0]],
                "Muon_eta": [[0.0]],
                "Muon_phi": [[0.0]],
                "Electron_eta": [[0.0]],
                "Electron_phi": [[0.0]],
            }
        ),
        collection="Muon",
        output="selected_Muon",
        selection=["pt >= 5"],
        keep=["pt", "eta", "phi"],
    )

    assert ak.to_list(out.selected_Muon_pt) == [[25.0]]
    assert "cleaned_Muon_pt" not in out.fields
    assert "nremoved_selected_Muon" not in out.fields


def test_records_operation_provenance() -> None:
    ctx = ComponentContext({})
    with ctx.provenance.operation_context(
        node_id="stage.SelectMuons",
        impl="hep.select_objects",
        role="transform",
        dataset="dy",
        partition={"id": "events__dy__0"},
    ):
        run_select_objects(
            stream=_muon_events(),
            collection="Muon",
            output="selected_Muon",
            selection=["pt >= 20", "promptMVA >= 0.64"],
            keep=["promptMVA"],
            ctx=ctx,
        )

    assert ctx.provenance.serialise_executions()["stage.SelectMuons::events__dy__0"][
        "operations"
    ] == [
        {
            "inputs": {
                "symbols": ["Muon_promptMVA", "Muon_pt"],
            },
            "outputs": {
                "symbols": [
                    "nselected_Muon",
                    "selected_Muon_promptMVA",
                ]
            },
        }
    ]


def test_normalized_and_compiled_config_materialize_default_sort(
    tmp_path: Path,
) -> None:
    workflow = {
        "version": "1.0",
        "registry": {
            "transforms": {
                "hep.select_objects": {
                    "spec": (
                        "fasthep_carpenter.operations.select_objects:"
                        "SELECT_OBJECTS_SPEC"
                    ),
                    "impl": (
                        "fasthep_carpenter.operations.select_objects:"
                        "run_select_objects"
                    ),
                }
            }
        },
        "data": {"datasets": [], "defaults": {}},
        "analysis": {
            "stages": [
                {
                    "id": "SelectMuons",
                    "op": "hep.select_objects",
                    "params": {
                        "collection": "Muon",
                        "output": "selected_Muon",
                        "selection": ["eta > 0"],
                        "keep": ["eta"],
                    },
                }
            ]
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    try:
        api_module = import_module("hepflow.api")
    except ModuleNotFoundError as exc:
        pytest.skip(f"installed hepflow integration dependencies are unavailable: {exc}")
    normalise_workflow_file = api_module.normalise_workflow_file

    plan_module = import_module("hepflow.compiler.plan")
    build_plan_from_normalized = getattr(plan_module, "build_plan_from_normalized", None)
    if build_plan_from_normalized is None:
        pytest.skip("installed hepflow does not expose build_plan_from_normalized")

    normalized = normalise_workflow_file(workflow_path, outdir=tmp_path / "build")
    _graph, plan = build_plan_from_normalized(normalized)
    params = normalized["analysis"]["stages"][0]["params"]

    assert params["sort"] == {"by": "pt", "order": "descending"}
    assert plan.get_node("stage.SelectMuons").params["sort"] == {
        "by": "pt",
        "order": "descending",
    }


def _dependencies(params: dict[str, object]) -> DataDependencyResult:
    return parse_component_data_dependencies(
        spec=SELECT_OBJECTS_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions={"abs", "cosh", "exp", "log", "log10", "sqrt", "where"},
            known_constants=set(),
            context_symbols=set(),
        ),
    )


def _muon_events() -> ak.Array:
    return ak.Array(
        {
            "Muon_pt": [[25.0, 6.0, 4.0], [30.0], [21.0, 22.0]],
            "Muon_eta": [[0.1, -2.3, 0.1], [0.1], [0.2, 0.3]],
            "Muon_phi": [[0.0, 1.0, 2.0], [0.0], [0.0, 1.0]],
            "Muon_tight": [[1, 0, 1], [0], [0, 1]],
            "Muon_promptMVA": [[0.9, 0.1, 0.9], [0.9], [0.64, 0.7]],
            "Muon_isGlobal": [[True, False, True], [True], [False, True]],
        }
    )
