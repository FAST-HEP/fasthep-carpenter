from collections import namedtuple
import numpy as np
from typing import Any

from ..workflow import Task

FakeEventRange = namedtuple("FakeEventRange", "start_entry stop_entry entries_in_block")


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree: Any, eventtype: str):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))

    def __len__(self) -> int:
        return len(self.tree)

    def count_nonzero(self) -> int:
        return self.tree.count_nonzero()

    def arrays(self, array_names: list[str], library: str = "np", outputtype: Any = list) -> list[Any]:
        return [self.tree[name] for name in array_names]


class FakeTree(dict):
    length: int = 101

    def __init__(self, length: int = 101):
        super(FakeTree, self).__init__(
            NMuon=np.linspace(0, 5., length),
            NElectron=np.linspace(0, 10, length),
            NJet=np.linspace(2, -18, length),
        )
        self.length = length

    def __len__(self) -> int:
        return self.length

    def arrays(self, array_names: list[str], library: str = "np", outputtype=list) -> list[Any]:
        return [self[name] for name in array_names]


class DummyMapping(dict):
    pass


def execute_task(task):
    return task[0](*task[1:])


def execute_tasks(tasks: dict[Task], last_task: str) -> Any:
    from dask.threaded import get
    return get(tasks, last_task)
