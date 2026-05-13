from __future__ import annotations

import importlib.resources as resources

from hepflow.registry.loaders import load_object

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
    assert "fasthep_carpenter.impl.read.root_tree:run_root_tree_source" in text


def test_load_one_spec_and_impl_object() -> None:
    spec = load_object(
        "fasthep_carpenter.spec.define_transform:DEFINE_TRANSFORM_SPEC"
    )
    impl = load_object("fasthep_carpenter.impl.define:run_define_transform")

    assert spec["name"] == "hep.define"
    assert callable(impl)
