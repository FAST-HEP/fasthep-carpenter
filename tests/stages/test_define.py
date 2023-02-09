from tkinter import E
import awkward as ak
import numpy as np

from fasthep_carpenter.stages import Define


def test_define_simple_var(data_mapping_with_tree):
    source_var = "Muon_Py"
    var_name = "simple_var"
    stage = Define("test_define", {var_name: f"{source_var}**2"})
    result = stage(data_mapping_with_tree)

    assert result is not None
    assert result.data is not None
    assert result.data[var_name] is not None
    assert data_mapping_with_tree[var_name] is not None
    assert len(result.data[var_name]) == len(result.data[source_var])
    np.testing.assert_array_almost_equal(
        ak.flatten(result.data[var_name]).to_numpy(),
        ak.flatten(data_mapping_with_tree[source_var] ** 2).to_numpy(),
        decimal=4,

    )
