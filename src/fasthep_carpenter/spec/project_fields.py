from __future__ import annotations

from typing import Any

from hepflow.model.data_flow import DataDependencyResult


PROJECT_FIELDS_SPEC = {
    "name": "core.project_fields",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.spec.project_fields:parse_project_fields_column_dependencies",
    },
    "input": {
        "name": "stream",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "stream_id": {
            "type": "string",
            "required": True,
            "description": "Logical stream id whose aliases should be projected.",
        },
        "aliases": {
            "type": "mapping",
            "required": True,
            "description": (
                "Alias mapping of logical field name -> physical branch path "
                "for this stream."
            ),
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Stream with projected alias fields added.",
    },
}


def parse_project_fields_column_dependencies(
    params: dict[str, Any],
    *,
    context_symbols: set[str] | None = None,
    **_: Any,
) -> DataDependencyResult:
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
