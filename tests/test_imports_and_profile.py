from __future__ import annotations

import importlib.resources as resources
from typing import Any, cast

import yaml
from hepflow.registry.loaders import load_object, resolve_runtime_registry

import fasthep_carpenter


def test_import_package() -> None:
    assert fasthep_carpenter is not None


def test_load_registry_profile_resource() -> None:
    text = (
        resources.files("fasthep_carpenter.profiles")
        .joinpath("registry.yaml")
        .read_text(encoding="utf-8")
    )

    assert "root_tree" in text
    assert "fasthep_carpenter.sources.root_tree:run_root_tree_source" in text


def test_load_one_spec_and_impl_object() -> None:
    spec = load_object("fasthep_carpenter.operations.define:DEFINE_SPEC")
    impl = load_object("fasthep_carpenter.operations.define:run_define_transform")

    assert spec["name"] == "hep.define"
    assert callable(impl)


def test_root_tree_writer_declares_keep_requirements() -> None:
    spec = load_object("fasthep_carpenter.sinks.root_tree:ROOT_TREE_WRITE_SPEC")

    assert spec["params"]["format"] == {
        "type": "string",
        "required": False,
        "default": "rntuple",
        "allowed": ["rntuple", "ttree"],
    }
    assert spec["requires"] == {
        "symbols": [
            {
                "from": "params.keep",
                "kind": "field_list",
            }
        ]
    }


def test_histogram_declares_expression_requirements() -> None:
    spec = load_object("fasthep_carpenter.operations.hist:HIST_SPEC")

    assert spec["requires"] == {
        "symbols": [
            {
                "from": "params.axes.*.source",
                "kind": "expr_or_field",
            },
            {
                "from": "params.weight_expr",
                "kind": "expr",
            },
            {
                "from": "params.variations.weights.*",
                "kind": "expr",
            },
        ]
    }


def test_registry_objects_resolve_from_new_layout() -> None:
    text = (
        resources.files("fasthep_carpenter.profiles")
        .joinpath("registry.yaml")
        .read_text(encoding="utf-8")
    )
    registry = yaml.safe_load(text)["registry"]

    for section in ("sources", "transforms", "sinks"):
        for entry in registry[section].values():
            assert load_object(entry["spec"]) is not None
            assert callable(load_object(entry["impl"]))

    for entry in registry["product_handlers"].values():
        if "combine" in entry:
            assert callable(load_object(entry["combine"]))
        assert callable(load_object(entry["merge"]))
        if "materialize" in entry:
            assert callable(load_object(entry["materialize"]))

    runtime_registry = resolve_runtime_registry(registry)
    assert _boundary_policy(runtime_registry, "event_stream") == (False, "value")
    assert _boundary_policy(runtime_registry, "histogram") == (True, "value")
    assert _boundary_policy(runtime_registry, "cutflow") == (True, "value")
    product_handlers = cast(Any, runtime_registry.product_handlers)
    assert product_handlers["event_stream"].combine is None
    assert callable(product_handlers["histogram"].combine)
    assert callable(product_handlers["cutflow"].combine)


def _boundary_policy(runtime_registry: object, name: str) -> tuple[bool, str]:
    registry = cast(Any, runtime_registry)
    policy = registry.product_handlers[name].boundary
    return bool(policy.retain), str(policy.representation)
