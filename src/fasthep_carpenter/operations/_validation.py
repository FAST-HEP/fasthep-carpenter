from __future__ import annotations

from typing import Any

import awkward as ak
import numpy as np
from awkward.types import ArrayType, NumpyType, OptionType


def validate_event_mask(
    mask: Any,
    *,
    n_events: int,
    expression: Any,
    context: str,
    guidance: str,
) -> Any:
    mask_type = ak.type(mask)
    try:
        outer_length = len(mask)
    except TypeError as exc:
        raise ValueError(
            f"{context} expression {expression!r} produced a non-event-level "
            f"mask with type {str(mask_type)!r}.\n\n{guidance}"
        ) from exc

    content = mask_type.content if isinstance(mask_type, ArrayType) else None
    if (
        outer_length == n_events
        and isinstance(content, NumpyType)
        and content.primitive == "bool"
    ):
        return mask

    raise ValueError(
        f"{context} expression {expression!r} produced a non-event-level "
        f"mask with type {str(mask_type)!r}.\n\n{guidance}"
    )


def validate_event_value(
    value: Any,
    *,
    n_events: int,
    context: str,
) -> Any:
    if _is_scalar(value):
        return ak.Array(np.full(n_events, value))

    value_type = ak.type(value)
    try:
        outer_length = len(value)
    except TypeError as exc:
        raise ValueError(
            f"{context} produced {str(value_type)!r}; expected one value per event."
        ) from exc

    content = value_type.content if isinstance(value_type, ArrayType) else None
    if outer_length == n_events and _is_scalar_content(content):
        return value

    raise ValueError(
        f"{context} produced {str(value_type)!r}; expected one value per event."
    )


def _is_scalar(value: Any) -> bool:
    return isinstance(value, str | bytes | bool | int | float | complex | np.generic)


def _is_scalar_content(content: Any) -> bool:
    if isinstance(content, NumpyType):
        return True
    if isinstance(content, OptionType):
        return isinstance(content.content, NumpyType)
    return False
