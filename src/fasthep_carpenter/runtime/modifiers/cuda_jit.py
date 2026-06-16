from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from typing import Any

MISSING_NUMBA_CUDA_MESSAGE = "cuda.jit requires numba with CUDA support."


class CUDAJitModifier:
    """
    Experimental execution modifier that JIT-compiles explicit CUDA callables.

    This is intentionally narrow: it only looks up named callables from
    ``ctx["cuda_jit_functions"]`` and stores compiled versions in
    ``ctx["cuda_jit_compiled"]``. It does not automatically accelerate arbitrary
    FAST-HEP stages, derive kernels from plan nodes, or inject kernels into
    operation internals. Compilation is cached per runtime context, which maps
    naturally to one worker process in distributed execution.
    """

    def __init__(
        self,
        *,
        functions: list[str] | None = None,
        mode: str = "explicit",
    ) -> None:
        self.functions = _normalise_functions(functions)
        self.mode = _normalise_mode(mode)

    def before_node(
        self,
        *,
        node: Any,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> None:
        del inputs
        cuda = _load_cuda()
        if not _cuda_is_available(cuda):
            raise RuntimeError(
                f"cuda.jit requested for node {node.id}, but CUDA is not "
                "available on this worker."
            )

        available_functions = ctx.get("cuda_jit_functions")
        if not isinstance(available_functions, Mapping):
            raise ValueError(
                "cuda.jit expected ctx['cuda_jit_functions'] to be a mapping "
                f"for node {node.id}"
            )

        compiled_functions = ctx.setdefault("cuda_jit_compiled", {})
        if not isinstance(compiled_functions, dict):
            raise ValueError("cuda.jit expected ctx['cuda_jit_compiled'] to be a dict")

        cache = ctx.setdefault("cuda_jit_cache", {})
        if not isinstance(cache, dict):
            raise ValueError("cuda.jit expected ctx['cuda_jit_cache'] to be a dict")

        compiled_names: list[str] = []
        cache_hits: list[str] = []
        cache_misses: list[str] = []
        node_id = str(node.id)
        for name in self.functions:
            if name not in available_functions:
                raise KeyError(
                    f"cuda.jit missing function {name!r} in "
                    f"ctx['cuda_jit_functions'] for node {node.id}"
                )
            func = available_functions[name]
            if not callable(func):
                raise TypeError(f"cuda.jit function {name!r} is not callable")

            key = _cache_key(node_id=node_id, name=name)
            if key in cache:
                compiled = cache[key]
                cache_hits.append(name)
            else:
                compiled = _compile_function(
                    cuda,
                    name=name,
                    node_id=node_id,
                    func=func,
                )
                cache[key] = compiled
                cache_misses.append(name)

            compiled_functions[name] = compiled
            compiled_names.append(name)

        _record_metadata(
            ctx,
            node_id=node_id,
            functions=compiled_names,
            compiled_count=len(cache_misses),
            cache_hits=cache_hits,
            cache_misses=cache_misses,
        )


def _normalise_functions(functions: list[str] | None) -> list[str]:
    if functions is None or not functions:
        raise ValueError("cuda.jit requires explicit functions in params.functions for now.")
    if not isinstance(functions, list) or not all(
        isinstance(name, str) and name.strip() for name in functions
    ):
        raise ValueError("cuda.jit functions must be a list of non-empty strings")
    seen: set[str] = set()
    names: list[str] = []
    for raw in functions:
        name = raw.strip()
        if name in seen:
            raise ValueError(f"cuda.jit duplicate function name {name!r}")
        seen.add(name)
        names.append(name)
    return names


def _normalise_mode(mode: str) -> str:
    if mode != "explicit":
        raise ValueError(f"cuda.jit unsupported mode {mode!r}; expected 'explicit'")
    return mode


def _load_cuda() -> Any:
    try:
        numba = importlib.import_module("numba")
    except ModuleNotFoundError as exc:
        raise RuntimeError(MISSING_NUMBA_CUDA_MESSAGE) from exc
    cuda = getattr(numba, "cuda", None)
    if cuda is None:
        try:
            cuda = importlib.import_module("numba.cuda")
        except ModuleNotFoundError as exc:
            raise RuntimeError(MISSING_NUMBA_CUDA_MESSAGE) from exc
    return cuda


def _cuda_is_available(cuda: Any) -> bool:
    is_available = getattr(cuda, "is_available", None)
    if not callable(is_available):
        return False
    try:
        return bool(is_available())
    except Exception:
        return False


def _compile_function(
    cuda: Any,
    *,
    name: str,
    node_id: str,
    func: Callable[..., Any],
) -> Any:
    try:
        return cuda.jit(func)
    except Exception as exc:
        raise RuntimeError(
            f"cuda.jit failed to compile function {name!r} for node {node_id}"
        ) from exc


def _cache_key(*, node_id: str, name: str) -> tuple[str, str, str]:
    return ("numba.cuda", node_id, name)


def _record_metadata(
    ctx: dict[str, Any],
    *,
    node_id: str,
    functions: list[str],
    compiled_count: int,
    cache_hits: list[str],
    cache_misses: list[str],
) -> None:
    metadata = ctx.setdefault("execution_modifier_metadata", {})
    if not isinstance(metadata, dict):
        return
    entries = metadata.setdefault("cuda.jit", [])
    if isinstance(entries, list):
        entries.append(
            {
                "node": node_id,
                "functions": list(functions),
                "backend": "numba.cuda",
                "compiled_count": compiled_count,
                "cache_hits": list(cache_hits),
                "cache_misses": list(cache_misses),
            }
        )
