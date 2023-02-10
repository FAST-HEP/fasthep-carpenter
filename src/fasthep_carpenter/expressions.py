import awkward as ak
import numpy as np
from functools import partial
import re

from .protocols import DataMapping
from .utils import register_in_collection, unregister_from_collection

CONSTANTS = {
    "nan": np.nan,
    "inf": np.inf,
    "pi": np.pi,
    "e": np.e,
}

ATTR_REGEX = re.compile(r"([a-zA-Z]\w*)\s*(\.\s*(\w+))+")
FUNC_REGEX = re.compile(r"([a-zA-Z]\w*)\s*\(")

SUPPORTED_FUNCTIONS = {
    "count_nonzero": ak.num,
}

register_function = partial(register_in_collection, SUPPORTED_FUNCTIONS, "Supported Functions")
unregister_function = partial(unregister_from_collection, SUPPORTED_FUNCTIONS, "Supported Functions")

# separate the parameters from a function call by matching the pattern of the function call and
# capturing the arguments in separate groups
# e.g. "count_nonzero(Muon_Py)" -> "count_nonzero", "Muon_Py"
# or "count_nonzero(Muon_Py, Muon_Px)" -> "count_nonzero", "Muon_Py", "Muon_Px"
FUNC_CALL_REGEX = re.compile(r"([a-zA-Z]\w*)\s*\((.*)\)")


def _replace_attributes(expression: str):
    """Replace attributes in an expression with the correct variable names.

    Args:
        expression (str): The expression to replace attributes in.

    Returns:
        The expression with attributes replaced.
    """
    def _replace(match):
        """Replace a match with the correct variable name."""
        # match.group(1) is the first group, which is the first variable name
        # match.group(3) is the third group, which is the attribute name
        return f"{match.group(1)}__DOT__{match.group(3)}"

    return ATTR_REGEX.sub(_replace, expression)


def extract_functions_and_parameters(expression: str) -> tuple[str]:
    """Extract the functions and their parameters from an expression.
        e.g. "count_nonzero(Muon_Py)" -> [("count_nonzero", "Muon_Py")]
        or "count_nonzero(Muon_Py, Muon_Px)" -> [("count_nonzero", ("Muon_Py", "Muon_Px"))]
        or "count(count_nonzero(Muon_Py, Muon_Px))" -> [("count_nonzero", ("Muon_Py", "Muon_Px")), ("count", "count_nonzero")
    """
    eval_function = "eval"
    # find the function call and its parameters
    match = FUNC_CALL_REGEX.search(expression)
    if not match:
        return [(eval_function, expression)]

    # match.group(1) is the first group, which is the function name
    # match.group(2) is the second group, which is the arguments
    function = match.group(1)
    arguments = match.group(2)
    # recursively find the functions and parameters in the arguments
    inside_function, inside_arguments = extract_functions_and_parameters(arguments)[0]

    return [(inside_function, inside_arguments), (function, inside_function)]


def tasks_from_expression(expression: str, task_prefix: str = "task-"):
    """Creates a dictionary of tasks from an expression."""
    expression = _replace_attributes(expression)
    task_tuple = extract_functions_and_parameters(expression)
    tasks = {}
    for i, (function, arguments) in enumerate(task_tuple):
        if i == 0:
            tasks[f"{task_prefix}{i}"] = (function, arguments)
            continue

        tasks[f"{task_prefix}{i}"] =(function, f"{task_prefix}{i - 1}")
    return tasks



def count_nonzero(array: ak.Array):
    """Count the number of nonzero elements in an array.

    Args:
        array (ak.Array): The array to count the nonzero elements of.

    Returns:
        The number of nonzero elements in the array.
    """
    return 1


def _build_global_dict():
    """Build the global dictionary for the expression."""
    global_dict = {}
    global_dict.update(CONSTANTS)
    global_dict.update(SUPPORTED_FUNCTIONS)
    return global_dict


def process_expression(data: DataMapping, expression: str):
    """Process an expression using the data.

    Args:
        data (DataMapping): The data to use in the expression.
        expression (str): The expression to process.

    Returns:
        The result of the expression.
    """
    global_dict = _build_global_dict()
    # register_function("eval", partial(data.evaluate, global_dict=global_dict))
    # from dask.threaded import get
    # get(dsk, '<last task>')  # executes in parallel

    return data.evaluate(expression, global_dict=global_dict)
