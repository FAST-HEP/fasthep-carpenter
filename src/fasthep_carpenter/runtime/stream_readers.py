from __future__ import annotations

from typing import Any

from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID


def get_stream_array(data: dict[str, Any], stream_name: str) -> Any:
    """Return a named stream, falling back to the primary stream envelope."""
    if stream_name in data:
        return data[stream_name]
    if DEFAULT_PRIMARY_STREAM_ID in data:
        return data[DEFAULT_PRIMARY_STREAM_ID]
    if data:
        return next(iter(data.values()))
    raise KeyError(
        f"Stream {stream_name!r} not found in data, and no default primary "
        "stream is available"
    )
