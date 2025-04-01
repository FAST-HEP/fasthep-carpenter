from __future__ import annotations

import importlib.metadata

import fasthep_carpenter as m


def test_version():
    assert importlib.metadata.version("fasthep_carpenter") == m.__version__
