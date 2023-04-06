from tkinter import E
import awkward as ak
import numpy as np
import pytest

from fasthep_carpenter.stages import Define
from fasthep_carpenter.testing import execute_tasks
from fasthep_carpenter.expressions import register_function


@pytest.fixture
def simple_definition():
    return [{"simple_var": "Muon_Py**2"}]


@pytest.fixture
def multi_definition():
    return [
        {"Muon_Pt": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"},
        {"IsoMuon_Idx": "(Muon_Iso / Muon_Pt) < 0.10"},
    ]


@pytest.fixture
def definition_with_function():
    return [
        {"Muon_Pt": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"},
        # {"IsoMuon_Idx_2": "(Muon_Iso / Muon_Pt_2) < 0.10"},
        {"IsoMuon_Idx": "find_isolated_muons(Muon_Iso, Muon_Pt)"},
    ]


@pytest.fixture
def definition_with_slice():
    return [{
        "first_muon_py": "Muon_Py[:, 0]",
    }]


def test_define_simple_var(simple_definition, data_mapping_with_tree):
    stage = Define("test_simple_var", simple_definition)
    tasks = stage.tasks(data_mapping_with_tree)
    assert len(tasks) == 2
    assert tasks.last_task == "define-test_simple_var-0-simple_var-0"

    result = execute_tasks(tasks)
    assert "simple_var" in data_mapping_with_tree
    assert ak.almost_equal(data_mapping_with_tree["simple_var"], (data_mapping_with_tree["Muon_Py"] ** 2), rtol=1e-4)


def test_define_multi_var(multi_definition, data_mapping_with_tree):
    stage = Define("test_define", multi_definition)
    tasks = stage.tasks(data_mapping_with_tree)
    # two additions, two function calls, one evaluation
    assert len(tasks) == 5
    assert tasks.last_task == "define-test_define-1-IsoMuon_Idx-0"

    result = execute_tasks(tasks)
    assert "Muon_Pt" in data_mapping_with_tree
    assert "IsoMuon_Idx" in data_mapping_with_tree
    assert ak.almost_equal(data_mapping_with_tree["Muon_Pt"], np.sqrt(
        data_mapping_with_tree["Muon_Px"] ** 2 + data_mapping_with_tree["Muon_Py"] ** 2), rtol=1e-4)
    assert ak.almost_equal(
        data_mapping_with_tree["IsoMuon_Idx"],
        (data_mapping_with_tree["Muon_Iso"] / data_mapping_with_tree["Muon_Pt"]) < 0.10, rtol=1e-4)


def test_define_with_function(definition_with_function, data_mapping_with_tree):
    stage = Define("test_define", definition_with_function)
    register_function("find_isolated_muons", lambda iso, pt: (iso / pt) < 0.10)
    tasks = stage.tasks(data_mapping_with_tree)

    # two additions, two function calls, three evaluations
    assert len(tasks) == 7
    assert tasks.last_task == "define-test_define-1-IsoMuon_Idx-1"
    result = execute_tasks(tasks)

    assert "Muon_Pt" in data_mapping_with_tree
    assert "IsoMuon_Idx" in data_mapping_with_tree
    assert ak.almost_equal(data_mapping_with_tree["Muon_Pt"], np.sqrt(
        data_mapping_with_tree["Muon_Px"] ** 2 + data_mapping_with_tree["Muon_Py"] ** 2), rtol=1e-4)
    assert ak.almost_equal(
        data_mapping_with_tree["IsoMuon_Idx"],
        (data_mapping_with_tree["Muon_Iso"] / data_mapping_with_tree["Muon_Pt"]) < 0.10, rtol=1e-4)


@pytest.mark.xfail
def test_define_with_slice(definition_with_slice, data_mapping_with_tree):
    stage = Define("test_define", definition_with_slice)
    tasks = stage.tasks(data_mapping_with_tree)
    # one addition, one evaluation
    assert len(tasks) == 2
    assert tasks.last_task == "define-test_define-0-first_muon_py-0"
    result = execute_tasks(tasks)

    assert "first_muon_py" in data_mapping_with_tree
    assert ak.almost_equal(data_mapping_with_tree["first_muon_py"], data_mapping_with_tree["Muon_Py"][:, 0], rtol=1e-4)
