from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.model.data_flow import DataDependencyResult
from hepflow.runtime.records import get_field_by_branch

PROJECT_FIELDS_SPEC = {
    "name": "core.project_fields",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.operations.project_fields:parse_project_fields_column_dependencies",
    },
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "stream_id": {"type": "string", "required": True},
        "aliases": {"type": "mapping", "required": True},
    },
    "result": {
        "kind": "event_stream",
        "description": "Stream with projected alias fields added.",
    },
    "requires": {
        "symbols": [
            {"from": "params.aliases.*", "kind": "field_list"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.aliases", "kind": "field_list"},
        ]
    },
}


def parse_project_fields_column_dependencies(
    params: dict[str, Any],
    *,
    context_symbols: set[str] | None = None,
    **_: Any,
) -> DataDependencyResult:
    del context_symbols
    result = DataDependencyResult()
    aliases = params.get("aliases") or {}
    if not isinstance(aliases, dict):
        return result

    for alias, branch in aliases.items():
        if isinstance(alias, str) and alias:
            result.produces.add(alias)
        if isinstance(branch, str) and branch:
            result.consumes.add(branch)

    return result


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
    del ctx
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
