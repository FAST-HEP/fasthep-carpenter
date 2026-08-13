from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import awkward as ak
import numpy as np
import pytest
import yaml
from hepflow.api import normalise_workflow_file
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.runtime import ComponentContext

from fasthep_carpenter.operations.align_schema import (
    ALIGN_SCHEMA_SPEC,
    parse_align_schema_column_dependencies,
    run_align_schema,
)


def test_align_schema_keeps_unchanged_field_without_dtype() -> None:
    stream = ak.Array({"pt": np.array([1.0, 2.0], dtype=np.float32)})

    out = run_align_schema(stream=stream, schema={"fields": {"pt": {}}}, extra="drop")

    assert ak.fields(out) == ["pt"]
    assert ak.to_numpy(out.pt).dtype == np.dtype("float32")
    assert ak.to_list(out.pt) == [1.0, 2.0]


def test_align_schema_renames_field() -> None:
    stream = ak.Array({"fasthep_name": [1.0, 2.0]})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"legacy_name": {"source": "fasthep_name"}}},
        extra="drop",
    )

    assert ak.fields(out) == ["legacy_name"]
    assert ak.to_list(out.legacy_name) == [1.0, 2.0]


def test_align_schema_casts_float_dtype() -> None:
    stream = ak.Array({"weight": np.array([1.0, 2.0], dtype=np.float64)})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"weight": {"dtype": "float32"}}},
        extra="drop",
    )

    assert ak.to_numpy(out.weight).dtype == np.dtype("float32")


def test_align_schema_casts_integer_dtype() -> None:
    stream = ak.Array({"count": np.array([1, 2], dtype=np.int64)})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"count": {"dtype": "int32"}}},
        extra="drop",
    )

    assert ak.to_numpy(out.count).dtype == np.dtype("int32")
    assert ak.to_list(out.count) == [1, 2]


def test_align_schema_casts_jagged_content_without_changing_shape() -> None:
    stream = ak.Array({"jets": [[1.0, 2.0], [], [3.0]]})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"jets": {"dtype": "float32"}}},
        extra="drop",
    )

    assert ak.to_list(out.jets) == [[1.0, 2.0], [], [3.0]]
    assert ak.to_numpy(ak.flatten(out.jets, axis=None)).dtype == np.dtype("float32")


def test_align_schema_field_order_follows_schema_then_extras() -> None:
    stream = ak.Array({"extra_a": [1], "source_b": [2], "source_a": [3]})

    out = run_align_schema(
        stream=stream,
        schema={
            "fields": {
                "target_a": {"source": "source_a"},
                "target_b": {"source": "source_b"},
            }
        },
        extra="keep",
    )

    assert ak.fields(out) == ["target_a", "target_b", "extra_a", "source_b", "source_a"]


def test_align_schema_missing_error_fails_clearly() -> None:
    stream = ak.Array({"present": [1]})

    with pytest.raises(KeyError, match="missing required source fields: absent"):
        run_align_schema(
            stream=stream,
            schema={"fields": {"target": {"source": "absent"}}},
        )


def test_align_schema_missing_ignore_skips_unresolved_target() -> None:
    stream = ak.Array({"present": [1]})

    out = run_align_schema(
        stream=stream,
        schema={
            "fields": {
                "target": {"source": "absent"},
                "present": {},
            }
        },
        missing="ignore",
        extra="drop",
    )

    assert ak.fields(out) == ["present"]


def test_align_schema_extra_keep_keeps_unmentioned_fields() -> None:
    stream = ak.Array({"a": [1], "b": [2]})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"a": {}}},
        extra="keep",
    )

    assert ak.fields(out) == ["a", "b"]


def test_align_schema_extra_drop_returns_only_schema_fields() -> None:
    stream = ak.Array({"a": [1], "b": [2]})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"a": {}}},
        extra="drop",
    )

    assert ak.fields(out) == ["a"]


def test_align_schema_drop_removes_explicit_scalar_and_jagged_fields() -> None:
    stream = ak.Array(
        {
            "legacy": [1, 2],
            "raw_scalar": [3, 4],
            "raw_jagged": [[1.0, 2.0], []],
            "other": [5, 6],
        }
    )

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"legacy": {}}},
        extra="keep",
        drop=["raw_scalar", "raw_jagged"],
    )

    assert ak.fields(out) == ["legacy", "other"]
    assert ak.to_list(out.legacy) == [1, 2]
    assert ak.to_list(out.other) == [5, 6]


def test_align_schema_keep_retains_explicit_scalar_and_jagged_fields() -> None:
    stream = ak.Array(
        {
            "target": [1, 2],
            "raw_scalar": [3, 4],
            "raw_jagged": [[1.0, 2.0], []],
            "other": [5, 6],
        }
    )

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"target": {}}},
        extra="keep",
        keep=["target", "raw_jagged"],
    )

    assert ak.fields(out) == ["target", "raw_jagged"]
    assert ak.to_list(out.raw_jagged) == [[1.0, 2.0], []]


def test_align_schema_keep_and_drop_are_mutually_exclusive() -> None:
    stream = ak.Array({"x": [1]})

    with pytest.raises(ValueError, match="mutually exclusive"):
        run_align_schema(
            stream=stream,
            schema={"fields": {"x": {}}},
            keep=["x"],
            drop=["y"],
        )


def test_align_schema_runtime_rejects_unexpanded_wildcards() -> None:
    stream = ak.Array({"Electron_pt": [1]})

    with pytest.raises(ValueError, match="unexpanded field patterns"):
        run_align_schema(
            stream=stream,
            schema={"fields": {"Electron_pt": {}}},
            drop=["Electron_*"],
        )


def test_align_schema_source_defaults_to_target_name() -> None:
    stream = ak.Array({"same": [1]})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"same": {}}},
        extra="drop",
    )

    assert ak.to_list(out.same) == [1]


def test_align_schema_rename_and_cast_together() -> None:
    stream = ak.Array({"fasthep": np.array([1, 2], dtype=np.int64)})

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"legacy": {"source": "fasthep", "dtype": "int32"}}},
        extra="drop",
    )

    assert ak.fields(out) == ["legacy"]
    assert ak.to_numpy(out.legacy).dtype == np.dtype("int32")
    assert ak.to_list(out.legacy) == [1, 2]


def test_align_schema_invalid_dtype_fails_clearly() -> None:
    stream = ak.Array({"x": [1]})

    with pytest.raises(ValueError, match="unsupported dtype"):
        run_align_schema(
            stream=stream,
            schema={"fields": {"x": {"dtype": "complex64"}}},
        )


@pytest.mark.parametrize(
    "schema",
    [
        [],
        {"version": 2, "fields": {"x": {}}},
        {"fields": []},
        {"fields": {"x": "not-a-mapping"}},
        {"fields": {"": {}}},
    ],
)
def test_align_schema_malformed_schema_fails_clearly(schema: Any) -> None:
    stream = ak.Array({"x": [1]})

    with pytest.raises(ValueError, match="align_schema"):
        run_align_schema(stream=stream, schema=schema)


def test_align_schema_external_yaml_schema_normalizes_to_mapping(tmp_path: Path) -> None:
    path = tmp_path / "schema.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "fields": {"legacy": {"source": "fasthep", "dtype": "float32"}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    normalized = _normalized_align_params(tmp_path, path)

    assert normalized["schema"] == {
        "version": 1,
        "fields": {"legacy": {"source": "fasthep", "dtype": "float32"}},
    }


def test_align_schema_external_json_schema_normalizes_to_mapping(tmp_path: Path) -> None:
    path = tmp_path / "schema.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "fields": {"legacy": {"source": "fasthep", "dtype": "int32"}},
            }
        ),
        encoding="utf-8",
    )
    normalized = _normalized_align_params(tmp_path, path)

    assert normalized["schema"] == {
        "version": 1,
        "fields": {"legacy": {"source": "fasthep", "dtype": "int32"}},
    }


def test_align_schema_external_schema_is_loaded_during_workflow_normalization(
    tmp_path: Path,
) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_path = schema_dir / "schema.yaml"
    schema_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "fields": {"legacy": {"source": "fasthep", "dtype": "int32"}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    workflow_path = tmp_path / "workflow.yaml"
    registry_path = (
        Path(__file__).parents[1]
        / "src"
        / "fasthep_carpenter"
        / "profiles"
        / "registry.yaml"
    )
    workflow_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "use": {"profiles": ["hep_debug", str(registry_path)]},
                "analysis": {
                    "stages": [
                        {
                            "id": "Align",
                            "op": "hep.align_schema",
                            "params": {"schema": "schemas/schema.yaml"},
                        }
                    ]
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    normalized = normalise_workflow_file(workflow_path, outdir=tmp_path / "build")
    params = normalized["analysis"]["stages"][0]["params"]

    assert params["schema"] == {
        "version": 1,
        "fields": {"legacy": {"source": "fasthep", "dtype": "int32"}},
    }
    deps = parse_align_schema_column_dependencies(params)
    assert deps.consumes == {"fasthep"}
    assert deps.produces == {"legacy"}


def test_align_schema_runtime_receives_normalized_external_yaml_schema(
    tmp_path: Path,
) -> None:
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "fields": {"legacy": {"source": "fasthep", "dtype": "float32"}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    params = _normalized_align_params(tmp_path, schema_path)
    stream = ak.Array({"fasthep": np.array([1.0, 2.0], dtype=np.float64)})

    out = run_align_schema(stream=stream, extra="drop", **params)

    assert ak.fields(out) == ["legacy"]
    assert ak.to_numpy(out.legacy).dtype == np.dtype("float32")


def test_align_schema_runtime_receives_normalized_external_json_schema(
    tmp_path: Path,
) -> None:
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "version": 1,
                "fields": {"legacy": {"source": "fasthep", "dtype": "int32"}},
            }
        ),
        encoding="utf-8",
    )
    params = _normalized_align_params(tmp_path, schema_path)
    stream = ak.Array({"fasthep": np.array([1, 2], dtype=np.int64)})

    out = run_align_schema(stream=stream, extra="drop", **params)

    assert ak.fields(out) == ["legacy"]
    assert ak.to_numpy(out.legacy).dtype == np.dtype("int32")


def test_align_schema_dependency_parser_exposes_sources_and_targets() -> None:
    deps = parse_align_schema_column_dependencies(
        {
            "schema": {
                "fields": {
                    "target_a": {"source": "source_a"},
                    "target_b": {},
                }
            }
        }
    )

    assert deps.consumes == {"source_a", "target_b"}
    assert deps.produces == {"target_a", "target_b"}


def test_align_schema_spec_derived_dependencies() -> None:
    deps = parse_component_data_dependencies(
        spec=ALIGN_SCHEMA_SPEC,
        params={
            "schema": {
                "fields": {
                    "target_a": {"source": "source_a"},
                    "target_b": {},
                }
            }
        },
        dep_ctx=DependencyContext(
            known_functions=set(),
            known_constants=set(),
            context_symbols=set(),
        ),
    )

    assert deps.consumes == {"source_a", "target_b"}
    assert deps.produces == {"target_a", "target_b"}


def test_align_schema_dependency_parser_consumes_explicit_keep_fields() -> None:
    deps = parse_align_schema_column_dependencies(
        {
            "schema": {"fields": {"target": {}}},
            "keep": ["target", "extra"],
        }
    )

    assert deps.consumes == {"target", "extra"}
    assert deps.produces == {"target"}


def test_align_schema_spec_declares_field_glob_expansion() -> None:
    params = cast(dict[str, Any], ALIGN_SCHEMA_SPEC["params"])
    drop = cast(dict[str, Any], params["drop"])
    keep = cast(dict[str, Any], params["keep"])

    assert drop["expand"] == {
        "kind": "field_glob",
        "against": "input.stream",
    }
    assert keep["expand"] == {
        "kind": "field_glob",
        "against": "input.stream",
    }


def test_align_schema_drop_wildcard_compiles_to_explicit_fields(
    tmp_path: Path,
) -> None:
    params, meta = _compiled_align_params(
        tmp_path,
        params={
            "schema": {"version": 1, "fields": {"legacy": {"source": "kept"}}},
            "missing": "ignore",
            "extra": "keep",
            "drop": ["Electron_*", "Muon_pt"],
        },
        branches=["kept", "Electron_pt", "Electron_eta", "Muon_pt", "Muon_eta"],
    )

    assert params["drop"] == ["Electron_pt", "Electron_eta", "Muon_pt"]
    assert meta["param_expansions"]["drop"]["expanded"] == [
        "Electron_pt",
        "Electron_eta",
        "Muon_pt",
    ]


def test_align_schema_keep_wildcard_compiles_to_explicit_fields(
    tmp_path: Path,
) -> None:
    params, meta = _compiled_align_params(
        tmp_path,
        params={
            "schema": {"version": 1, "fields": {"legacy": {"source": "kept"}}},
            "missing": "ignore",
            "extra": "keep",
            "keep": ["Muon_*"],
        },
        branches=["kept", "Muon_pt", "Muon_eta", "Electron_pt"],
    )

    assert params["keep"] == ["Muon_pt", "Muon_eta"]
    assert meta["param_expansions"]["keep"]["patterns"] == ["Muon_*"]


def test_align_schema_records_runtime_provenance() -> None:
    stream = ak.Array({"fasthep": np.array([1, 2], dtype=np.int64)})
    ctx = ComponentContext({})

    with ctx.provenance.operation_context(
        node_id="stage.Align",
        impl="hep.align_schema",
        role="transform",
        dataset="dy",
        partition={"id": "events__dy__0"},
    ):
        run_align_schema(
            stream=stream,
            schema={"fields": {"legacy": {"source": "fasthep", "dtype": "int32"}}},
            extra="drop",
            ctx=ctx,
        )

    executions = ctx.provenance.serialise_executions()
    assert executions["stage.Align::events__dy__0"]["operations"] == [
        {
            "inputs": {"symbols": ["fasthep"]},
            "outputs": {"symbols": ["legacy"]},
        }
    ]


def test_align_schema_does_not_mutate_input_stream() -> None:
    stream = ak.Array({"fasthep": np.array([1, 2], dtype=np.int64), "extra": [3, 4]})
    before_fields = ak.fields(stream)

    out = run_align_schema(
        stream=stream,
        schema={"fields": {"legacy": {"source": "fasthep", "dtype": "int32"}}},
        extra="drop",
    )

    assert ak.fields(stream) == before_fields
    assert ak.fields(out) == ["legacy"]
    assert "legacy" not in stream.fields


def _normalized_align_params(tmp_path: Path, schema_path: Path) -> dict[str, Any]:
    workflow_path = tmp_path / "workflow.yaml"
    registry_path = (
        Path(__file__).parents[1]
        / "src"
        / "fasthep_carpenter"
        / "profiles"
        / "registry.yaml"
    )
    workflow_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "use": {"profiles": ["hep_debug", str(registry_path)]},
                "analysis": {
                    "stages": [
                        {
                            "id": "Align",
                            "op": "hep.align_schema",
                            "params": {"schema": schema_path.name},
                        }
                    ]
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    normalized = normalise_workflow_file(workflow_path, outdir=tmp_path / "build")
    return dict(normalized["analysis"]["stages"][0]["params"])


def _compiled_align_params(
    tmp_path: Path,
    *,
    params: dict[str, Any],
    branches: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    workflow_path = tmp_path / "workflow.yaml"
    registry_path = (
        Path(__file__).parents[1]
        / "src"
        / "fasthep_carpenter"
        / "profiles"
        / "registry.yaml"
    )
    workflow_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "use": {"profiles": ["hep_debug", str(registry_path)]},
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
                        "branches": branches,
                    }
                },
                "analysis": {
                    "stages": [
                        {
                            "id": "Align",
                            "op": "hep.align_schema",
                            "params": params,
                        }
                    ]
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    normalized = normalise_workflow_file(workflow_path, outdir=tmp_path / "build")
    _, plan = build_plan_from_normalized(normalized)
    node = plan.get_node("stage.Align")
    return dict(node.params), dict(node.meta)
