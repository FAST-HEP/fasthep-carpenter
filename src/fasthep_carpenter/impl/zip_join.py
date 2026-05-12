from __future__ import annotations

from typing import Any

import awkward as ak


def run_zip_join(
    *,
    on_mismatch: str = "error",
    inputs: list[dict[str, str]],
    **streams: Any,
) -> ak.Array:
    """
    Zip multiple event streams into one.

    Parameters
    ----------
    streams:
        Named inputs from graph edges (e.g. l1=..., reco=...)
    inputs:
        [{name: ..., prefix: ...}]
    """
    parts: dict[str, ak.Array] = {}
    lens: list[int] = []

    for inp in inputs:
        name = inp["name"]
        prefix = inp["prefix"]

        if name not in streams:
            raise ValueError(f"Missing input stream '{name}'")

        arr = streams[name]
        arr = _normalise_stream(arr)

        parts[prefix] = arr
        lens.append(len(arr))

    if len(set(lens)) != 1:
        if on_mismatch == "error":
            raise ValueError(f"zip_join length mismatch: {lens}")
        raise ValueError(f"Unsupported on_mismatch mode: {on_mismatch}")

    return ak.zip(parts, depth_limit=1)


def _normalise_stream(stream: Any) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream
    if isinstance(stream, dict):
        return ak.Array(stream)
    raise TypeError("zip_join expects awkward.Array or dict[str, array-like]")
