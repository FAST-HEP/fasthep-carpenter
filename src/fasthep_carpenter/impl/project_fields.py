from __future__ import annotations

from typing import Any

import awkward as ak

from hepflow.runtime.records import get_field_by_branch


def run_project_fields(
    stream: Any,
    *,
    stream_id: str,
    aliases: dict[str, str],
    ctx: dict[str, Any] | None = None,
) -> ak.Array:
    """
    Project logical alias fields onto a stream.

    Parameters
    ----------
    stream:
        awkward record array
    stream_id:
        Logical stream id, used only for diagnostics.
    aliases:
        Mapping of alias name -> physical branch path
    """
    stream = _normalise_stream(stream)

    if not aliases:
        return stream

    base_cols = {name: stream[name] for name in stream.fields}

    alias_cols: dict[str, Any] = {}
    missing: list[dict[str, str]] = []

    for alias, branch in aliases.items():
        try:
            alias_cols[alias] = get_field_by_branch(stream, branch)
        except KeyError:
            missing.append({"alias": alias, "branch": branch})

    if missing:
        raise ValueError(
            f"Missing branches while projecting aliases for stream '{stream_id}'. "
            f"Missing: {missing[:20]}{' ...' if len(missing) > 20 else ''}"
        )

    overlap = set(base_cols) & set(alias_cols)
    if overlap:
        raise ValueError(
            f"Alias projection would overwrite existing fields in stream "
            f"'{stream_id}': {sorted(overlap)}"
        )

    return ak.zip({**base_cols, **alias_cols}, depth_limit=1)


def _normalise_stream(stream: Any) -> ak.Array:
    if isinstance(stream, ak.Array):
        return stream

    if isinstance(stream, dict):
        return ak.Array(stream)

    raise TypeError("project_fields expects an awkward.Array or dict[str, array-like]")
