from __future__ import annotations

import awkward as ak
from typing import Any


def nth(values: Any, index: int, default: Any = 0.0) -> Any:
    """
    Return the n-th element of a variable-length collection per event.

    This is a convenience wrapper around awkward operations to safely access
    elements of jagged arrays (e.g. Jet_pt, Muon_eta) without raising errors
    when fewer elements are present.

    Parameters
    ----------
    values : array-like
        Per-event collection (e.g. awkward Array of variable-length lists).
    index : int
        Zero-based index of the element to extract:
        - 0 -> leading
        - 1 -> subleading
        - etc.
    default : Any, optional
        Value to use when the event contains fewer than (index + 1) elements.
        Default is 0.0.

    Returns
    -------
    array-like
        Per-event array with one value per event.

    Notes
    -----
    - Assumes the collection is already sorted (e.g. by pT descending).
    - Internally uses:
        ak.pad_none(values, index+1, clip=True)[:, index]
        ak.fill_none(..., default)

    Examples
    --------
    >>> nth(Jet_pt, 0)
    >>> nth(Jet_eta, 1, default=0.0)
    """
    return ak.fill_none(ak.pad_none(values, index + 1, clip=True)[:, index], default)


def leading(values: Any, default: Any = 0.0) -> Any:
    """
    Return the leading (first) element of a collection per event.

    Equivalent to:
        nth(values, 0, default)

    See Also
    --------
    nth : General helper for extracting the n-th element.
    """
    return nth(values, 0, default=default)


def subleading(values: Any, default: Any = 0.0) -> Any:
    """
    Return the subleading (second) element of a collection per event.

    Equivalent to:
        nth(values, 1, default)

    See Also
    --------
    nth : General helper for extracting the n-th element.
    """
    return nth(values, 1, default=default)
