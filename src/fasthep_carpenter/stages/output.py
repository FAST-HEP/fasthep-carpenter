"""Stages that produce output"""


from fasthep_logging import get_logger
from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult
from ..expressions import expression_to_ast
from ..workflow import Task, get_task_number, TaskCollection

log = get_logger("FASTHEP::Carpenter")

VariableDefinition = dict[str, str]


class FileOutput(ProcessingStep):
    """Creates output based on configured output variables.
    File output is relative to the output directory.

    example:

    file_output:
      output_type: "ROOT|Parquet|CSV|text"
      path: "output.root|output.parquet|output.csv|output.txt"
      content:
        - NGoodMuons
        - Muon_pt

    """

    _name: str
    _output_type: str
    _path: str
    _content: list[str]
    _tasks: TaskCollection

    def __init__(self, name: str, output_type: str, path: str, content: list[str], **kwargs) -> None:
        self._name = name
        self._output_type = output_type
        self._path = path
        self._content = content

    def __call__(self, data: DataMapping) -> ProcessingStepResult:
        return None

    @property
    def name(self) -> str:
        return self._name

    def tasks(self, data: DataMapping) -> TaskCollection:
        if self._tasks:
            return self._tasks

        self._tasks = TaskCollection()

        task_name = f"file_output_{self._name}"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"
        self._tasks[task_name] = (self._write_file, data)

        return self._tasks

    def _write_file(self, data: DataMapping) -> str:
        with open(self._path, "w") as f:
            content = data.array_dict(self._content)
            f.write(str(content))
        return self._path


class ConsoleOutput(ProcessingStep):
    """Creates output based on configured output variables.
    File output is relative to the output directory.

    example:

    console_output:
      path: "stdout|log"
      content:
        - NGoodMuons
        - Muon_pt
    """

    _name: str
    _path: str
    _content: list[str]

    def __init__(self, name: str, output_type: str, path: str, content: list[str], **kwargs) -> None:
        self._name = name
        self._output_type = output_type
        self._path = path
        self._content = content
