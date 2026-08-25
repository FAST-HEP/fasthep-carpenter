from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.runtime.records import branch_to_segments


def materialize_selection_flag(
    events: ak.Array,
    flag: Any,
    output: str,
    *,
    op_name: str,
) -> ak.Array:
    output = _required_output(output)
    path = _field_path(output)
    _reject_existing_field(events, output, path, op_name=op_name)
    return _with_field_path(events, flag, path)


def _required_output(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "hep.selection.flag output must be resolved during workflow normalization"
        )
    return value.strip()


def _field_path(output: str) -> list[str]:
    return branch_to_segments(output) or [output]


def _reject_existing_field(
    events: Any,
    output: str,
    path: list[str],
    *,
    op_name: str,
) -> None:
    if not hasattr(events, "fields"):
        return
    if output in events.fields:
        raise ValueError(
            f"{op_name} output field {output!r} already exists; refusing to overwrite"
        )

    current = events
    for index, segment in enumerate(path):
        fields = list(getattr(current, "fields", []))
        if segment not in fields:
            return
        if index == len(path) - 1:
            raise ValueError(
                f"{op_name} output field {output!r} already exists; "
                "refusing to overwrite"
            )
        current = current[segment]
        if not _has_record_fields(current):
            prefix = ".".join(path[: index + 1])
            raise ValueError(
                f"{op_name} output field {output!r} conflicts with existing "
                f"non-record field {prefix!r}"
            )


def _with_field_path(events: Any, value: Any, path: list[str]) -> Any:
    if len(path) == 1:
        return ak.with_field(events, value, path[0])

    head, tail = path[0], path[1:]
    if hasattr(events, "fields") and head in events.fields:
        nested = _with_field_path(events[head], value, tail)
    else:
        nested = _nested_record(value, tail)
    return ak.with_field(events, nested, head)


def _nested_record(value: Any, path: list[str]) -> Any:
    if len(path) == 1:
        return ak.zip({path[0]: value})
    return ak.zip({path[0]: _nested_record(value, path[1:])})


def _has_record_fields(value: Any) -> bool:
    return bool(list(getattr(value, "fields", [])))
