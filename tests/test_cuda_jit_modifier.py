from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from re import escape
from typing import Any

import pytest
import yaml
from hepflow.registry.loaders import load_object, load_runtime_entry

from fasthep_carpenter.runtime.modifiers.cuda_jit import (
    MISSING_NUMBA_CUDA_MESSAGE,
    CUDAJitModifier,
)


class FakeCuda:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.jit_calls: list[Any] = []

    def is_available(self) -> bool:
        return self.available

    def jit(self, func: Any) -> str:
        self.jit_calls.append(func)
        return f"compiled:{func.__name__}"


class FakeNumba(types.ModuleType):
    def __init__(self, cuda: FakeCuda) -> None:
        super().__init__("numba")
        self.cuda = cuda


def test_cuda_jit_imports_numba_lazily(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "numba", raising=False)

    module = importlib.import_module("fasthep_carpenter.runtime.modifiers.cuda_jit")

    assert module.CUDAJitModifier is CUDAJitModifier
    assert "numba" not in sys.modules


def test_cuda_jit_requires_explicit_functions() -> None:
    with pytest.raises(ValueError, match="requires explicit functions"):
        CUDAJitModifier()


def test_cuda_jit_missing_numba_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None) -> Any:
        if name == "numba":
            raise ModuleNotFoundError("No module named 'numba'")
        return real_import_module(name, package)

    monkeypatch.setattr(
        "fasthep_carpenter.runtime.modifiers.cuda_jit.importlib.import_module",
        fake_import_module,
    )

    with pytest.raises(RuntimeError, match=escape(MISSING_NUMBA_CUDA_MESSAGE)):
        CUDAJitModifier(functions=["my_kernel"]).before_node(
            node=_node(),
            inputs={},
            ctx={"cuda_jit_functions": {"my_kernel": _kernel}},
        )


def test_cuda_jit_unavailable_cuda_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(FakeCuda(available=False)))

    with pytest.raises(RuntimeError, match="CUDA is not available"):
        CUDAJitModifier(functions=["my_kernel"]).before_node(
            node=_node(),
            inputs={},
            ctx={"cuda_jit_functions": {"my_kernel": _kernel}},
        )


def test_cuda_jit_missing_function_in_context_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(FakeCuda()))

    with pytest.raises(KeyError, match="missing function 'missing'"):
        CUDAJitModifier(functions=["missing"]).before_node(
            node=_node(),
            inputs={},
            ctx={"cuda_jit_functions": {"my_kernel": _kernel}},
        )


def test_cuda_jit_compiles_named_functions_into_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cuda = FakeCuda()
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(cuda))
    ctx: dict[str, Any] = {"cuda_jit_functions": {"my_kernel": _kernel}}

    CUDAJitModifier(functions=["my_kernel"]).before_node(
        node=_node(),
        inputs={},
        ctx=ctx,
    )

    assert cuda.jit_calls == [_kernel]
    assert ctx["cuda_jit_compiled"] == {"my_kernel": "compiled:_kernel"}
    assert ctx["execution_modifier_metadata"]["cuda.jit"] == [
        {
            "node": "stage.HeavyInference",
            "functions": ["my_kernel"],
            "backend": "numba.cuda",
            "compiled_count": 1,
            "cache_hits": [],
            "cache_misses": ["my_kernel"],
        }
    ]


def test_cuda_jit_reuses_cached_function_for_same_node_and_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cuda = FakeCuda()
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(cuda))
    ctx: dict[str, Any] = {"cuda_jit_functions": {"my_kernel": _kernel}}
    modifier = CUDAJitModifier(functions=["my_kernel"])

    modifier.before_node(node=_node(), inputs={}, ctx=ctx)
    modifier.before_node(node=_node(), inputs={}, ctx=ctx)

    assert cuda.jit_calls == [_kernel]
    assert ctx["cuda_jit_compiled"] == {"my_kernel": "compiled:_kernel"}
    assert ctx["execution_modifier_metadata"]["cuda.jit"][-1] == {
        "node": "stage.HeavyInference",
        "functions": ["my_kernel"],
        "backend": "numba.cuda",
        "compiled_count": 0,
        "cache_hits": ["my_kernel"],
        "cache_misses": [],
    }


def test_cuda_jit_compiles_same_function_for_different_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cuda = FakeCuda()
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(cuda))
    ctx: dict[str, Any] = {"cuda_jit_functions": {"my_kernel": _kernel}}
    modifier = CUDAJitModifier(functions=["my_kernel"])

    modifier.before_node(node=_node("stage.A"), inputs={}, ctx=ctx)
    modifier.before_node(node=_node("stage.B"), inputs={}, ctx=ctx)

    assert cuda.jit_calls == [_kernel, _kernel]
    assert sorted(ctx["cuda_jit_cache"]) == [
        ("numba.cuda", "stage.A", "my_kernel"),
        ("numba.cuda", "stage.B", "my_kernel"),
    ]


def test_cuda_jit_compiles_different_function_name_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cuda = FakeCuda()
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(cuda))
    ctx: dict[str, Any] = {
        "cuda_jit_functions": {
            "my_kernel": _kernel,
            "other_kernel": _other_kernel,
        }
    }

    CUDAJitModifier(functions=["my_kernel", "other_kernel"]).before_node(
        node=_node(),
        inputs={},
        ctx=ctx,
    )

    assert cuda.jit_calls == [_kernel, _other_kernel]
    assert ctx["cuda_jit_compiled"] == {
        "my_kernel": "compiled:_kernel",
        "other_kernel": "compiled:_other_kernel",
    }
    assert sorted(ctx["cuda_jit_cache"]) == [
        ("numba.cuda", "stage.HeavyInference", "my_kernel"),
        ("numba.cuda", "stage.HeavyInference", "other_kernel"),
    ]


def test_cuda_jit_invalid_cache_type_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(FakeCuda()))

    with pytest.raises(
        ValueError,
        match=escape("ctx['cuda_jit_cache']"),
    ):
        CUDAJitModifier(functions=["my_kernel"]).before_node(
            node=_node(),
            inputs={},
            ctx={
                "cuda_jit_functions": {"my_kernel": _kernel},
                "cuda_jit_cache": [],
            },
        )


def test_cuda_jit_non_callable_function_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "numba", FakeNumba(FakeCuda()))

    with pytest.raises(TypeError, match="is not callable"):
        CUDAJitModifier(functions=["my_kernel"]).before_node(
            node=_node(),
            inputs={},
            ctx={"cuda_jit_functions": {"my_kernel": object()}},
        )


def test_cuda_jit_rejects_unsupported_mode() -> None:
    with pytest.raises(ValueError, match="unsupported mode 'auto'"):
        CUDAJitModifier(functions=["my_kernel"], mode="auto")


def test_cuda_jit_registry_entry_resolves() -> None:
    registry_path = (
        Path(__file__).parents[1]
        / "src"
        / "fasthep_carpenter"
        / "profiles"
        / "registry.yaml"
    )
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))["registry"]

    entry = load_runtime_entry(registry, "execution_modifiers", "cuda.jit")

    assert load_object(entry["impl"]) is CUDAJitModifier


def _kernel(x: Any) -> Any:
    return x


def _other_kernel(x: Any) -> Any:
    return x


def _node(node_id: str = "stage.HeavyInference") -> Any:
    return type("Node", (), {"id": node_id})()
