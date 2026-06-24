from __future__ import annotations

import pytest

from fasthep_carpenter.runtime.stream_readers import get_stream_array


def test_get_stream_array_prefers_requested_stream() -> None:
    assert get_stream_array({"events": 1, "selected": 2}, "selected") == 2


def test_get_stream_array_falls_back_to_primary_stream() -> None:
    assert get_stream_array({"events": 1}, "selected") == 1


def test_get_stream_array_falls_back_to_only_available_stream() -> None:
    assert get_stream_array({"other": 3}, "selected") == 3


def test_get_stream_array_rejects_empty_data() -> None:
    with pytest.raises(KeyError, match="no default primary stream is available"):
        get_stream_array({}, "selected")
