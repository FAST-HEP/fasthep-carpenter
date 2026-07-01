from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from re import escape
from typing import Any

import numpy as np
import pytest
import yaml
from hepflow.registry.loaders import load_object, load_runtime_entry

from fasthep_carpenter.runtime.modifiers.gpu_preload import (
    MISSING_CUPY_MESSAGE,
    GPUPreloadModifier,
)


class FakeCupyArray:
    def __init__(self, value: Any) -> None:
        self.value = value


class FakeCupy(types.ModuleType):
    ndarray = FakeCupyArray

    def __init__(self) -> None:
        super().__init__("cupy")
        self.asarray_calls: list[Any] = []

    def asarray(self, value: Any) -> FakeCupyArray:
        self.asarray_calls.append(value)
        return FakeCupyArray(value)


def test_gpu_preload_imports_cupy_lazily(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "cupy", raising=False)

    module = importlib.import_module(
        "fasthep_carpenter.runtime.modifiers.gpu_preload"
    )

    assert module.GPUPreloadModifier is GPUPreloadModifier
    assert "cupy" not in sys.modules


def test_gpu_preload_converts_selected_numpy_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cupy = FakeCupy()
    monkeypatch.setitem(sys.modules, "cupy", cupy)
    stream = {
        "Muon_Pt": np.asarray([1.0, 2.0]),
        "Jet_Pt": np.asarray([10.0, 20.0]),
    }
    inputs = {"stream": stream}
    ctx: dict[str, Any] = {}

    GPUPreloadModifier(fields=["Muon_Pt"]).before_node(
        node=_node(),
        inputs=inputs,
        ctx=ctx,
    )

    assert inputs["stream"] is stream
    assert isinstance(stream["Muon_Pt"], FakeCupyArray)
    assert stream["Jet_Pt"].tolist() == [10.0, 20.0]
    assert len(cupy.asarray_calls) == 1
    assert np.array_equal(cupy.asarray_calls[0], np.asarray([1.0, 2.0]))
    assert ctx["execution_modifier_metadata"]["gpu.preload"] == [
        {"node": "stage.HeavyInference", "fields": ["Muon_Pt"], "backend": "cupy"}
    ]


def test_gpu_preload_missing_field_errors_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "cupy", FakeCupy())

    with pytest.raises(KeyError, match="missing field 'Missing'"):
        GPUPreloadModifier(fields=["Missing"]).before_node(
            node=_node(),
            inputs={"stream": {"Muon_Pt": np.asarray([1.0])}},
            ctx={},
        )


def test_gpu_preload_missing_field_can_be_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cupy = FakeCupy()
    monkeypatch.setitem(sys.modules, "cupy", cupy)
    ctx: dict[str, Any] = {}

    GPUPreloadModifier(fields=["Missing"], on_missing="ignore").before_node(
        node=_node(),
        inputs={"stream": {"Muon_Pt": np.asarray([1.0])}},
        ctx=ctx,
    )

    assert cupy.asarray_calls == []
    assert ctx["execution_modifier_metadata"]["gpu.preload"][0]["fields"] == []


def test_gpu_preload_requires_explicit_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "cupy", FakeCupy())

    with pytest.raises(ValueError, match="requires explicit fields"):
        GPUPreloadModifier().before_node(
            node=_node(),
            inputs={"stream": {"Muon_Pt": np.asarray([1.0])}},
            ctx={},
        )


def test_gpu_preload_unsupported_backend_errors() -> None:
    with pytest.raises(ValueError, match="unsupported backend 'numba'"):
        GPUPreloadModifier(fields=["Muon_Pt"], backend="numba")


def test_gpu_preload_missing_cupy_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None) -> Any:
        if name == "cupy":
            raise ModuleNotFoundError("No module named 'cupy'")
        return real_import_module(name, package)

    monkeypatch.setattr(
        "fasthep_carpenter.runtime.modifiers.gpu_preload.importlib.import_module",
        fake_import_module,
    )

    with pytest.raises(RuntimeError, match=escape(MISSING_CUPY_MESSAGE)):
        GPUPreloadModifier(fields=["Muon_Pt"]).before_node(
            node=_node(),
            inputs={"stream": {"Muon_Pt": np.asarray([1.0])}},
            ctx={},
        )


def test_gpu_preload_leaves_existing_cupy_array_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cupy = FakeCupy()
    monkeypatch.setitem(sys.modules, "cupy", cupy)
    gpu_array = FakeCupyArray([1.0])
    stream = {"Muon_Pt": gpu_array}

    GPUPreloadModifier(fields=["Muon_Pt"]).before_node(
        node=_node(),
        inputs={"stream": stream},
        ctx={},
    )

    assert stream["Muon_Pt"] is gpu_array
    assert cupy.asarray_calls == []


def test_gpu_preload_registry_entry_resolves() -> None:
    registry_path = (
        Path(__file__).parents[1]
        / "src"
        / "fasthep_carpenter"
        / "profiles"
        / "registry.yaml"
    )
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))["registry"]

    entry = load_runtime_entry(registry, "execution_modifiers", "gpu.preload")

    assert load_object(entry["impl"]) is GPUPreloadModifier


def _node() -> Any:
    return type("Node", (), {"id": "stage.HeavyInference"})()
