# fasthep_carpenter/impl/compat.py

from __future__ import annotations

from typing import Any

from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID


def legacy_data_envelope(stream: Any) -> dict[str, Any]:
    if (
        isinstance(stream, dict)
        and DEFAULT_PRIMARY_STREAM_ID in stream
        and len(stream) == 1
    ):
        return stream

    return {DEFAULT_PRIMARY_STREAM_ID: stream}


def unwrap_legacy_data_envelope(value: Any) -> Any:
    if (
        isinstance(value, dict)
        and DEFAULT_PRIMARY_STREAM_ID in value
        and len(value) == 1
    ):
        return value[DEFAULT_PRIMARY_STREAM_ID]

    return value
