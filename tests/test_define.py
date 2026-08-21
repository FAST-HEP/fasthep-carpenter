from __future__ import annotations

import awkward as ak
import numpy as np

from fasthep_carpenter.operations.define import run_define_transform


def test_define_broadcasts_literal_scalars_with_requested_dtype() -> None:
    stream = ak.Array({"event": [1, 2, 3]})

    out = run_define_transform(
        stream=stream,
        variables=[
            {"name": "one_float", "expr": "1.0", "dtype": "float32"},
            {"name": "one_int", "expr": "1", "dtype": "int32"},
            {"name": "flag", "expr": "True"},
        ],
    )

    assert ak.to_list(out.one_float) == [1.0, 1.0, 1.0]
    assert ak.to_numpy(out.one_float).dtype == np.dtype("float32")
    assert ak.to_list(out.one_int) == [1, 1, 1]
    assert ak.to_numpy(out.one_int).dtype == np.dtype("int32")
    assert ak.to_list(out.flag) == [True, True, True]
    assert ak.to_numpy(out.flag).dtype == np.dtype("bool")
