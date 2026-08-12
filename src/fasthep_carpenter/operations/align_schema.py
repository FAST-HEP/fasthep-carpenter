from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from hepflow.model.data_flow import DataDependencyResult

ALIGN_SCHEMA_SPEC = {
    "name": "hep.align_schema",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "schema": {
            "type": "mapping",
            "required": True,
            "load": {"formats": ["yaml", "json"]},
        },
        "missing": {
            "type": "string",
            "required": False,
            "default": "error",
            "allowed": ["error", "ignore"],
        },
        "extra": {
            "type": "string",
            "required": False,
            "default": "keep",
            "allowed": ["keep", "drop"],
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream aligned to a logical target schema.",
    },
    "dependency_parser": (
        "fasthep_carpenter.operations.align_schema:"
        "parse_align_schema_column_dependencies"
    ),
}

SUPPORTED_DTYPES = {
    name: np.dtype(name)
    for name in (
        "float32",
        "float64",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "bool",
    )
}


def parse_align_schema_column_dependencies(
    params: dict[str, Any],
    *,
    context_symbols: set[str] | None = None,
    **_: Any,
) -> DataDependencyResult:
    del context_symbols
    result = DataDependencyResult()
    try:
        schema = normalize_align_schema(params.get("schema"))
    except (FileNotFoundError, ValueError, TypeError):
        return result
    for entry in schema["fields"]:
        result.consumes.add(entry["source"])
        result.produces.add(entry["target"])
    return result


def run_align_schema(
    *,
    stream: Any,
    schema: Any,
    missing: str = "error",
    extra: str = "keep",
    ctx: Any | None = None,
) -> ak.Array:
    stream = _normalise_stream(stream)
    missing_policy = _policy(missing, "missing", {"error", "ignore"})
    extra_policy = _policy(extra, "extra", {"keep", "drop"})
    normalized = normalize_align_schema(schema)

    out_fields: dict[str, Any] = {}
    consumed: list[str] = []
    produced: list[str] = []
    missing_fields: list[str] = []

    for entry in normalized["fields"]:
        target = entry["target"]
        source = entry["source"]
        dtype = entry.get("dtype")
        if source not in stream.fields:
            if missing_policy == "ignore":
                continue
            missing_fields.append(source)
            continue

        value = stream[source]
        if dtype is not None:
            value = ak.values_astype(value, _dtype(dtype))
        out_fields[target] = value
        consumed.append(source)
        produced.append(target)

    if missing_fields:
        raise KeyError(
            "align_schema missing required source fields: "
            + ", ".join(sorted(missing_fields))
        )

    if extra_policy == "keep":
        for field in stream.fields:
            if field not in out_fields:
                out_fields[field] = stream[field]

    if ctx is not None and hasattr(ctx, "provenance"):
        ctx.provenance.record_operation(
            inputs={"symbols": consumed},
            outputs={"symbols": produced},
        )

    return ak.zip(out_fields, depth_limit=1)


def normalize_align_schema(schema: Any) -> dict[str, Any]:
    if not isinstance(schema, dict):
        raise ValueError("align_schema schema must be a mapping")
    if schema.get("version", 1) != 1:
        raise ValueError("align_schema schema version must be 1")
    fields = schema.get("fields")
    if not isinstance(fields, dict) or not fields:
        raise ValueError("align_schema schema.fields must be a non-empty mapping")

    normalized_fields: list[dict[str, str]] = []
    for target, spec in fields.items():
        target_name = _field_name(target, "target field")
        if spec is None:
            item: dict[str, Any] = {}
        elif isinstance(spec, dict):
            item = dict(spec)
        else:
            raise ValueError(
                f"align_schema field {target_name!r} must be a mapping or null"
            )

        source = _field_name(item.get("source", target_name), f"{target_name}.source")
        normalized: dict[str, str] = {
            "target": target_name,
            "source": source,
        }
        if "dtype" in item and item["dtype"] is not None:
            dtype_name = str(item["dtype"])
            _dtype(dtype_name)
            normalized["dtype"] = dtype_name
        normalized_fields.append(normalized)

    return {"version": 1, "fields": normalized_fields}


def _dtype(value: str) -> np.dtype[Any]:
    dtype = SUPPORTED_DTYPES.get(str(value))
    if dtype is None:
        raise ValueError(
            f"align_schema unsupported dtype {value!r}; supported dtypes are "
            + ", ".join(sorted(SUPPORTED_DTYPES))
        )
    return dtype


def _policy(value: str, label: str, allowed: set[str]) -> str:
    out = str(value)
    if out not in allowed:
        raise ValueError(
            f"align_schema {label} policy must be one of {sorted(allowed)}, got {out!r}"
        )
    return out


def _field_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"align_schema {label} must be a non-empty string")
    return value


def _normalise_stream(stream: Any) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream
    if isinstance(stream, dict):
        return ak.Array(stream)
    raise TypeError("align_schema expects an awkward.Array or dict[str, array-like]")


__all__ = [
    "ALIGN_SCHEMA_SPEC",
    "SUPPORTED_DTYPES",
    "normalize_align_schema",
    "parse_align_schema_column_dependencies",
    "run_align_schema",
]
