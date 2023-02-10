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


def test_define_multi_var(data_mapping_with_tree):
    vars = {
        "Muon_Pt": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)",
        "IsoMuon_Idx": "(Muon_Iso / Muon_Pt) < 0.10",
    }
    stage = Define("test_define", vars)
    result = stage(data_mapping_with_tree)
    source_var = "Muon_Py"

    assert result is not None
    assert result.data is not None
    for var in vars:
        assert result.data[var] is not None
        assert data_mapping_with_tree[var] is not None
        assert len(result.data[var]) == len(result.data[source_var])



def test_define_with_function(data_mapping_with_tree):
    var = "num_muons"
    source_var = "Muon_Py"
    vars = {
        var: "count_nonzero(Muon_Py)",
    }
    stage = Define("test_define", vars)
    result = stage(data_mapping_with_tree)

    assert result is not None
    assert result.data is not None
    assert result.data[var] is not None
    assert data_mapping_with_tree[var] is not None
    # this is a reduction
    assert len(result.data[var]) != len(result.data[source_var])
    assert result.data[var] == result.data["NMuon"]


# def test_define_with_slice(data_mapping_with_tree):
#     vars = {
#         "first_muon_py": "Muon_Py[:, 0]",
#     }
