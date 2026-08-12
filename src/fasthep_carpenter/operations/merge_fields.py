from __future__ import annotations

from typing import Any

import awkward as ak

MERGE_FIELDS_SPEC = {
    "name": "hep.merge_fields",
    "kind": "transform",
    "version": "1.0",
    "params": {
        "on_conflict": {
            "type": "string",
            "required": False,
            "default": "keep_first",
            "allowed": ["keep_first", "keep_last", "error"],
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with top-level fields merged from inputs.",
    },
}


def run_merge_fields(
    *,
    on_conflict: str = "keep_first",
    ctx: Any | None = None,
    **streams: Any,
) -> ak.Array:
    """Merge top-level fields from equal-length event streams."""
    del ctx
    if not streams:
        raise ValueError("merge_fields requires at least one input stream")
    if on_conflict not in {"keep_first", "keep_last", "error"}:
        raise ValueError(f"Unsupported merge_fields on_conflict mode: {on_conflict}")

    merged: dict[str, Any] = {}
    lengths: list[int] = []
    for name, stream in streams.items():
        arr = _normalise_stream(stream, name=name)
        lengths.append(len(arr))
        for field in ak.fields(arr):
            if field in merged:
                if on_conflict == "error":
                    raise ValueError(f"merge_fields duplicate field: {field}")
                if on_conflict == "keep_first":
                    continue
            merged[field] = arr[field]

    if len(set(lengths)) != 1:
        raise ValueError(f"merge_fields length mismatch: {lengths}")

    return ak.zip(merged, depth_limit=1)


def _normalise_stream(stream: Any, *, name: str) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream
    if isinstance(stream, dict):
        return ak.Array(stream)
    raise TypeError(
        f"merge_fields input stream '{name}' expects awkward.Array or dict[str, array-like]"
    )
