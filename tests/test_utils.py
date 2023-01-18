from pathlib import Path
import pytest


from fasthep_carpenter.utils import mkdir_p


@pytest.fixture
def tmp_path():
    return Path("tmp")


def test_mkdir_p(tmp_path):
    path = tmp_path / "test"
    mkdir_p(path)
    assert path.exists()

    # Should not raise an error
    mkdir_p(path)
    assert path.exists()
