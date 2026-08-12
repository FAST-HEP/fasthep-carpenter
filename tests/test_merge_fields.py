from __future__ import annotations

import awkward as ak
import pytest

from fasthep_carpenter.operations.merge_fields import run_merge_fields


def test_merge_fields_combines_top_level_fields() -> None:
    left = ak.Array({"event": [1, 2], "a": [10, 20]})
    right = ak.Array({"event": [1, 2], "b": [30, 40]})

    out = run_merge_fields(left=left, right=right)

    assert ak.fields(out) == ["event", "a", "b"]
    assert out.a.to_list() == [10, 20]
    assert out.b.to_list() == [30, 40]


def test_merge_fields_ignores_runtime_context() -> None:
    left = ak.Array({"event": [1, 2], "a": [10, 20]})
    right = ak.Array({"event": [1, 2], "b": [30, 40]})

    out = run_merge_fields(left=left, right=right, ctx={"dataset": {"name": "sample"}})

    assert ak.fields(out) == ["event", "a", "b"]


def test_merge_fields_keeps_first_duplicate_field_by_default() -> None:
    left = ak.Array({"event": [1, 2], "value": [10, 20]})
    right = ak.Array({"event": [1, 2], "value": [30, 40]})

    out = run_merge_fields(left=left, right=right)

    assert out.value.to_list() == [10, 20]


def test_merge_fields_can_keep_last_duplicate_field() -> None:
    left = ak.Array({"value": [10, 20]})
    right = ak.Array({"value": [30, 40]})

    out = run_merge_fields(left=left, right=right, on_conflict="keep_last")

    assert out.value.to_list() == [30, 40]


def test_merge_fields_can_reject_duplicate_fields() -> None:
    left = ak.Array({"value": [10, 20]})
    right = ak.Array({"value": [30, 40]})

    with pytest.raises(ValueError, match="duplicate field: value"):
        run_merge_fields(left=left, right=right, on_conflict="error")


def test_merge_fields_rejects_length_mismatch() -> None:
    left = ak.Array({"a": [1]})
    right = ak.Array({"b": [1, 2]})

    with pytest.raises(ValueError, match="length mismatch"):
        run_merge_fields(left=left, right=right)
