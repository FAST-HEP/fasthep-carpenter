from collections import defaultdict
from importlib import reload
import pytest

from fasthep_carpenter.workflow import get_task_number, reset_task_counters


@pytest.fixture(autouse=True)
def clear_task_counters():
    reset_task_counters()


def test_single_task():
    task_name = "task1"
    assert get_task_number(task_name) == 0
    assert get_task_number(task_name) == 1


def test_multiple_tasks():
    task_name1 = "task1"
    task_name2 = "task2"
    assert get_task_number(task_name1) == 0
    assert get_task_number(task_name2) == 0
    assert get_task_number(task_name1) == 1
    assert get_task_number(task_name2) == 1
