from collections import defaultdict

from dask import visualize
from dask.delayed import Delayed
from typing import Any, Callable

# very general task definition
Task = tuple[Callable[[Any], Any], Any]

# a task graph is a dict of tasks
TaskGraph = dict[str, Task]

_task_counters: defaultdict[int] = defaultdict(int)

# note, we are trying to create a HighLevelGraph here
# https://docs.dask.org/en/stable/high-level-graphs.html


def visualize_tasks(tasks: TaskGraph, filename: str = "tasks.png") -> None:
    """Visualize a task graph using dask.visualize.

    Args:
        tasks (TaskGraph): The task graph to visualize.
        filename (str, optional): The filename to save the visualization to. Defaults to "tasks.png".
    """
    delayed_dsk = Delayed("w", tasks)
    visualize(tasks, filename=filename)


def get_task_number(task_name: str) -> int:
    """Get the number of a task.

    Args:
        task_name (str): The name of the task.

    Returns:
        int: The number of the task taken from a global task counter.
    """
    global _task_counters
    task_id = _task_counters[task_name]
    _task_counters[task_name] += 1
    return task_id


def reset_task_counters():
    """Reset the global task counters."""
    global _task_counters
    _task_counters = defaultdict(int)


class TaskCollection:
    _tasks: TaskGraph
    first_task: str
    last_task: str

    def __init__(self):
        self._tasks = {}
        self.first_task = None
        self.last_task = None

    def add_task(self, task_name: str, *args: Any):
        """Add a task to the collection.

        Args:
            task_name (str): The name of the task.
            task (Task): The task to add.
        """
        self._tasks[task_name] = (*args,)
        if self.first_task is None:
            self.first_task = task_name
        self.last_task = task_name

    def __repr__(self) -> str:
        return f"TaskCollection({self._tasks}, first={self.first_task}, last={self.last_task})"

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self):
        return iter(self._tasks)

    def __contains__(self, task_name: str) -> bool:
        return task_name in self._tasks

    def __getitem__(self, task_name: str) -> Task:
        return self._tasks[task_name]

    def update(self, other: Any):
        """Update the collection with another collection.

        Args:
            other (TaskCollection): The other collection.
        """
        self._tasks.update(other._tasks)
        if self.first_task is None:
            self.first_task = other.first_task
        self.last_task = other.last_task

    def append_to_task(self, task_name: str, *args: Any):
        """Append to the arguments of a task.

        Args:
            task_name (str): The name of the task.
            *args (Any): The arguments to append.
        """
        self._tasks[task_name] = (*self._tasks[task_name], *args)

    @property
    def graph(self) -> TaskGraph:
        """Get the tasks in the collection.

        Returns:
            TaskGraph: The tasks in the collection.
        """
        return self._tasks


class Workflow:
    layers: dict[str, TaskGraph]
    dependencies: dict[str, set[str]]
    """Creates layers of tasks to be executed in a workflow.
    e.g. read-csv -> add -> filter -> ...
    {
 'read-csv': {('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
              ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
              ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
              ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv')},
 'add': {('add', 0): (operator.add, ('read-csv', 0), 100),
         ('add', 1): (operator.add, ('read-csv', 1), 100),
         ('add', 2): (operator.add, ('read-csv', 2), 100),
         ('add', 3): (operator.add, ('read-csv', 3), 100)}
 'filter': {('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
            ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
            ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
            ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3))}
}

    These layers can then be used in Dask HighLevelGraphs;
    https://docs.dask.org/en/stable/high-level-graphs.html

    """

    def __init__(self, sequence: list[str], datasets: dict[str, Any]):
        self.layers = {}
        self.dependencies = {}
