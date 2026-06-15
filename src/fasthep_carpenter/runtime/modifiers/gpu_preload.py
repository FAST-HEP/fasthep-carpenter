from __future__ import annotations

import importlib
from collections.abc import MutableMapping
from typing import Any

MISSING_CUPY_MESSAGE = (
    "gpu.preload requires CuPy. Install fasthep-carpenter[gpu] or ensure CuPy "
    "is available."
)


class GPUPreloadModifier:
    """
    Move selected top-level event-stream fields to a GPU backend before a node runs.

    This is an execution modifier and intentionally mutates selected input fields
    in place where the stream container supports mutation. For immutable stream
    containers such as Awkward arrays, it replaces ``inputs["stream"]`` with an
    equivalent stream carrying converted fields.
    """

    def __init__(
        self,
        *,
        fields: list[str] | None = None,
        backend: str = "cupy",
        on_missing: str = "error",
    ) -> None:
        self.fields = _normalise_fields(fields)
        self.backend = _normalise_backend(backend)
        self.on_missing = _normalise_on_missing(on_missing)

    def before_node(
        self,
        *,
        node: Any,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> None:
        if not self.fields:
            raise ValueError(
                "gpu.preload requires explicit fields for this prototype. "
                "Set execution.modifiers[].params.fields."
            )
        if "stream" not in inputs:
            raise ValueError(f"gpu.preload expected inputs['stream'] for node {node.id}")

        cupy = _load_cupy()
        stream = inputs["stream"]
        moved: list[str] = []
        for field in self.fields:
            if not _has_field(stream, field):
                if self.on_missing == "ignore":
                    continue
                raise KeyError(f"gpu.preload missing field {field!r} for node {node.id}")
            converted = _to_gpu_array(_get_field(stream, field), field=field, cupy=cupy)
            stream = _set_field(stream, field, converted)
            moved.append(field)

        inputs["stream"] = stream
        _record_metadata(
            ctx,
            node_id=str(node.id),
            fields=moved,
            backend=self.backend,
        )


def _normalise_fields(fields: list[str] | None) -> list[str]:
    if fields is None:
        return []
    if not isinstance(fields, list) or not all(
        isinstance(field, str) and field.strip() for field in fields
    ):
        raise ValueError("gpu.preload fields must be a list of non-empty strings")
    return [field.strip() for field in fields]


def _normalise_backend(backend: str) -> str:
    if backend != "cupy":
        raise ValueError(f"gpu.preload unsupported backend {backend!r}; expected 'cupy'")
    return backend


def _normalise_on_missing(on_missing: str) -> str:
    if on_missing not in {"error", "ignore"}:
        raise ValueError("gpu.preload on_missing must be 'error' or 'ignore'")
    return on_missing


def _load_cupy() -> Any:
    try:
        return importlib.import_module("cupy")
    except ModuleNotFoundError as exc:
        raise RuntimeError(MISSING_CUPY_MESSAGE) from exc


def _has_field(stream: Any, field: str) -> bool:
    if isinstance(stream, dict):
        return field in stream
    fields = getattr(stream, "fields", None)
    if isinstance(fields, list):
        return field in fields
    try:
        stream[field]
    except Exception:
        return False
    return True


def _get_field(stream: Any, field: str) -> Any:
    return stream[field]


def _set_field(stream: Any, field: str, value: Any) -> Any:
    if isinstance(stream, MutableMapping):
        stream[field] = value
        return stream

    if _is_awkward_array(stream):
        awkward = importlib.import_module("awkward")
        return awkward.with_field(stream, value, field)

    try:
        stream[field] = value
    except Exception as exc:
        raise TypeError(
            f"gpu.preload cannot mutate stream field {field!r} on "
            f"{type(stream).__name__}"
        ) from exc
    return stream


def _to_gpu_array(value: Any, *, field: str, cupy: Any) -> Any:
    if _is_cupy_array(value, cupy):
        return value
    if _is_awkward_array(value):
        return _awkward_to_gpu(value, field=field)
    try:
        return cupy.asarray(value)
    except Exception as exc:
        raise TypeError(
            f"gpu.preload cannot convert field {field!r} of type "
            f"{type(value).__name__} with CuPy"
        ) from exc


def _awkward_to_gpu(value: Any, *, field: str) -> Any:
    awkward = importlib.import_module("awkward")
    try:
        return awkward.to_backend(value, "cuda")
    except Exception as exc:
        raise TypeError(
            f"gpu.preload cannot convert Awkward field {field!r} to CUDA backend"
        ) from exc


def _is_awkward_array(value: Any) -> bool:
    return type(value).__module__.startswith("awkward.") and type(value).__name__ == "Array"


def _is_cupy_array(value: Any, cupy: Any) -> bool:
    cupy_array_type = getattr(cupy, "ndarray", None)
    if cupy_array_type is not None and isinstance(value, cupy_array_type):
        return True
    return type(value).__module__.startswith("cupy")


def _record_metadata(
    ctx: dict[str, Any],
    *,
    node_id: str,
    fields: list[str],
    backend: str,
) -> None:
    metadata = ctx.setdefault("execution_modifier_metadata", {})
    if not isinstance(metadata, dict):
        return
    entries = metadata.setdefault("gpu.preload", [])
    if isinstance(entries, list):
        entries.append(
            {
                "node": node_id,
                "fields": list(fields),
                "backend": backend,
            }
        )
