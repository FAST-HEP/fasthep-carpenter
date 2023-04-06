import ast
import awkward as ak
import pytest

from fasthep_carpenter.expressions import (
    register_function,
    SUPPORTED_FUNCTIONS,
    expression_to_ast,
    ASTWrapper,
    SymbolNode,
    FunctionNode,
)

from fasthep_carpenter.testing import DummyMapping, execute_task, execute_tasks
from fasthep_carpenter.workflow import reset_task_counters


@pytest.fixture(autouse=True)
def clear_task_counters():
    reset_task_counters()


@pytest.fixture
def dummy_mapping():
    # dummy mapping with awkward arrays
    return DummyMapping(
        {
            "a": ak.Array([[1, 2, 3], [5, 6]]),
            "b": ak.Array([[2, 3, 4], [6, 7]]),
            "c": ak.Array([[3, 4, 5], [7, 8]]),
        }
    )


@pytest.fixture(scope="session")
def register_functions():
    register_function("count", lambda x: len(x))


def test_constant(dummy_mapping):
    expression = "1"
    ast_wrapper = expression_to_ast(expression)
    assert isinstance(ast_wrapper, ASTWrapper)
    ast_tree = ast_wrapper.ast
    assert isinstance(ast_tree, ast.Constant)
    assert ast_tree.value == 1
    tasks = ast_wrapper.to_tasks(dummy_mapping)
    assert len(tasks) == 1
    assert "constant-0" in tasks
    assert tasks["constant-0"][0] == SUPPORTED_FUNCTIONS["constant"]
    assert tasks["constant-0"][1] == 1
    assert execute_task(tasks["constant-0"]) == 1


def test_expression(dummy_mapping):
    expression = "a + b"
    ast_wrapper = expression_to_ast(expression)
    assert isinstance(ast_wrapper, ASTWrapper)
    ast_tree = ast_wrapper.ast
    # first node is a binary operation node
    assert isinstance(ast_tree, ast.BinOp)
    assert isinstance(ast_tree.op, ast.Add)
    assert isinstance(ast_tree.left, SymbolNode)
    assert isinstance(ast_tree.right, SymbolNode)


def test_expression_with_constants(dummy_mapping):
    expression = "a*2 + 1"
    ast_wrapper = expression_to_ast(expression)
    tasks = ast_wrapper.to_tasks(dummy_mapping)
    # the above is a "pure" expression, so it should only have one task
    assert len(tasks) == 1
    assert "eval-0" in tasks
    result = execute_tasks(tasks, "eval-0")
    assert ak.all(result == (dummy_mapping["a"] * 2 + 1))


def test_expression_with_func(dummy_mapping, register_functions):
    expression = "count(a + b)"
    ast_wrapper = expression_to_ast(expression)
    assert isinstance(ast_wrapper, ASTWrapper)
    ast_tree = ast_wrapper.ast
    # first node is the function node
    assert isinstance(ast_tree, FunctionNode)
    assert ast_tree.name == "count"
    assert isinstance(ast_tree.arguments[0], ast.BinOp)
    arguments = ast_tree.arguments[0]
    assert isinstance(arguments.op, ast.Add)
    assert isinstance(arguments.left, SymbolNode)
    assert isinstance(arguments.right, SymbolNode)

    tasks = ast_wrapper.to_tasks(dummy_mapping)
    assert len(tasks) == 2
    assert "eval-0" in tasks
    assert "func-count-0" in tasks
    assert tasks["eval-0"][0].func == SUPPORTED_FUNCTIONS["eval"]
    assert tasks["eval-0"][0].keywords["global_dict"] == dummy_mapping

    result = execute_tasks(tasks, "func-count-0")
    assert result == 2


def test_expression_with_func_and_slice(dummy_mapping, register_functions, clear_task_counters):
    expression = "count(c[1:])"
    ast_wrapper = expression_to_ast(expression)
    assert isinstance(ast_wrapper, ASTWrapper)
    ast_tree = ast_wrapper.ast
    # first node is the function node
    assert isinstance(ast_tree, FunctionNode)
    assert ast_tree.name == "count"
    assert isinstance(ast_tree.arguments[0], FunctionNode)

    tasks = ast_wrapper.to_tasks(dummy_mapping)
    assert len(tasks) == 4
    assert "eval-0" in tasks  # evaluate c
    assert "constant-0" in tasks  # slice start
    assert "func-slice-0" in tasks  # slice c
    assert "func-count-0" in tasks  # count slice

    from dask.threaded import get
    result = get(tasks, "func-count-0")
    assert result == 1
