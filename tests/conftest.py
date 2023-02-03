import pytest

from fasthep_carpenter.protocols import EventRange


@pytest.fixture
def test_input_file():
    return "tests/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def test_multi_tree_input_file():
    return "tests/data/2kmu.root"


@pytest.fixture
def event_range():
    return EventRange(
        start=100,
        stop=200,
        block_size=100,
    )


@pytest.fixture
def full_event_range():
    return EventRange(
        start=0,
        stop=-1,
        block_size=0,
    )
