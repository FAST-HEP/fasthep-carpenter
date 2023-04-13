"""Stages for defining new variables"""
from functools import partial

from fasthep_logging import get_logger
from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult
from ..expressions import expression_to_ast
from ..workflow import Task, get_task_number, TaskCollection

log = get_logger("FASTHEP::Carpenter")

VariableDefinition = dict[str, str]


class Define(ProcessingStep):
    """Creates new variables using a string-based expression.

    Example:
      ::

        variables:
          - Muon_pt: "sqrt(Muon_px**2 + Muon_py**2)"
          - Muon_is_good: (Muon_iso > 0.3) & (Muon_pt > 10)
          - NGoodMuons: "count_nonzero(Muon_is_good)"
    """

    _name: str
    _variable_definitions = list[VariableDefinition]
    _variables: list[tuple[str]]
    _tasks: TaskCollection

    def __init__(self, name: str, variables: list[VariableDefinition], **kwargs) -> None:
        self._name = name
        self._variable_definitions = variables
        self._variables = []
        for variable in variables:
            for name, expression in variable.items():
                self._variables.append((name, expression))
        self._tasks = None

    def __call__(self, data: DataMapping) -> ProcessingStepResult:
        results = {}
        for name, expression in self._variables:
            log.trace(f"Processing {name=} {expression=}")
            # result = process_expression(data, expression)
            # results[name] = result
            # data.add_variable(name, result)

        # add something like result.dask?
        return ProcessingStepResult(
            data=data,
            error_code=0,
            error_message="",
            result=results,
            bookkeeping={self._name: self._variables},
            # reducer=None,
        )

    def __dask_tokenize__(self):
        return (Define, self._name, self._variable_definitions)

    @property
    def name(self) -> str:
        return self._name

    # TODO: tasks should be able to created without using a data mapping
    def tasks(self, data: DataMapping) -> TaskCollection:
        """
        Uses the variable definitions to create a task graph.

        _variables = [
          ("Muon_pt", "sqrt(Muon_px**2 + Muon_py**2)"),
          ("Muon_is_good": "(Muon_iso > 0.3) & (Muon_pt > 10)"),
          ("NGoodMuons": "count_nonzero(Muon_is_good)"),
        ]
        Should create at least 3 tasks to evaluate the expressions, and then
        3 tasks to add the variables to the data mapping.
        Each definition should depend on the previous one.

        Returns:
            dict[str, Task]: A dictionary of tasks to be executed.
        e.g.
        {"define-Muon_pt": (data.add_variable, "Muon_pt", ...),
            "define-Muon_is_good": (data.add_variable, "Muon_is_good", ..., "define-Muon_pt"),
        """
        if self._tasks:
            return self._tasks

        self._tasks = TaskCollection()

        previous_definition = None
        previous_variable = None
        for i, (name, expression) in enumerate(self._variables):
            # TODO: move to task builder
            task_name = f"define-{self.name}-{i}-{name}"
            task_id = get_task_number(task_name)
            task_name = f"{task_name}-{task_id}"
            ast_wrapper = expression_to_ast(expression)
            tasks = ast_wrapper.to_tasks(data)
            if previous_definition:
                # check each task to see if it depends on the previous definition
                # if it does, add the previous definition as dependency
                dependend_tasks = tasks.find_tasks_with_dependency(previous_variable)
                for dependent in dependend_tasks:
                    tasks.append_to_task(dependent, previous_definition)
            self._tasks.update(tasks)
            partial_add = partial(data.add_variable, name)
            self._tasks.add_task(task_name, partial_add, tasks.last_task)
            previous_definition = task_name
            previous_variable = name


        return self._tasks
