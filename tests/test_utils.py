from pathlib import Path
import pytest
import re


from fasthep_carpenter.utils import list_python_packages_with_versions, mkdir_p


@pytest.fixture
def tmp_path():
    return Path("tmp")


def test_list_python_packages_with_versions():
    packages = list_python_packages_with_versions()
    assert isinstance(packages, dict)
    assert "fasthep-carpenter" in packages
    assert re.fullmatch(r"\d+\.\d+\.(\d+|\d+[a-z]\d+)", packages["fasthep-carpenter"])


def test_mkdir_p(tmp_path):
    path = tmp_path / "test"
    mkdir_p(path)
    assert path.exists()

    # Should not raise an error
    mkdir_p(path)
    assert path.exists()
