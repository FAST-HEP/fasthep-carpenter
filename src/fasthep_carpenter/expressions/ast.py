import ast
import numpy as np
from typing import Any
from collections import defaultdict
from functools import partial
from astor import to_source

from ..protocols import DataMapping, Task
from .custom import SUPPORTED_FUNCTIONS
from .symbols import symbol_to_str


class SymbolNode(ast.AST):
    """A node representing a symbol in the expression."""
    _fields = ("value", "slice")

    def __init__(self, value, slice=None):
        self.value = value
        self.slice = slice

    def __repr__(self):
        if self.slice is not None:
            return f"SymbolNode({self.value}, {self.slice})"
        else:
            return f"SymbolNode({self.value})"


class FunctionNode(ast.AST):
    """A node representing a function in the expression."""

    _fields = ("name", "arguments", "slice")

    def __init__(self, name, arguments=None, slice=None):
        self.name = name
        self.arguments = arguments or []
        self.slice = slice

    def __repr__(self):
        if self.slice is not None:
            return f"FunctionNode({self.name}, {self.arguments}, {self.slice})"
        else:
            return f"FunctionNode({self.name}, {self.arguments})"


class Transformer(ast.NodeTransformer):
    def visit_Name(self, node):
        slice = None
        if isinstance(node.ctx, ast.Load) and node.id.endswith(']'):
            slice_node = ast.parse(f"[{node.id.split('[')[1]}", mode='eval').body
            slice = self.visit(slice_node)
            node = ast.Name(id=node.id.split('[')[0], ctx=node.ctx)
        return SymbolNode(node.id, slice=slice)

    def visit_Subscript(self, node):
        if isinstance(node.slice, ast.Slice):
            lower = self.visit(node.slice.lower) if node.slice.lower else None
            upper = self.visit(node.slice.upper) if node.slice.upper else None
            step = self.visit(node.slice.step) if node.slice.step else None
            return FunctionNode('slice', [self.visit(node.value)], slice=(lower, upper, step))
        else:
            return self.generic_visit(node)

    def visit_Call(self, node):
        name = node.func.id
        arguments = [self.visit(arg) for arg in node.args]
        slice = None
        if isinstance(node.func.ctx, ast.Load) and name.endswith(']'):
            slice_node = ast.parse(f"[{name.split('[')[1]}", mode='eval').body
            slice = self.visit(slice_node)
            name = name.split('[')[0]
        return FunctionNode(name, arguments, slice=slice)

    def visit_Index(self, node):
        return self.visit(node.value)

    def visit_Slice(self, node):
        lower = self.visit(node.lower) if node.lower else None
        upper = self.visit(node.upper) if node.upper else None
        step = self.visit(node.step) if node.step else None
        return (lower, upper, step)


def get_symbol(data: DataMapping, symbol: SymbolNode):
    if symbol.slice is not None:
        return data[symbol.value][symbol.slice]
    else:
        return data[symbol.value]


class NodeToStringConverter(ast.NodeVisitor):
    def __init__(self):
        self.result = ''

    def visit_Add(self, node):
        self.result = '+'

    def convert(self, node):
        self.visit(node)
        return self.result


def ast_to_expression(node: ast.AST) -> str:
    converter = NodeToStringConverter()
    if isinstance(node, SymbolNode):
        if node.slice is not None:
            return f"{node.value}{node.slice}"
        else:
            return node.value
    elif isinstance(node, ast.BinOp):
        return f"{ast_to_expression(node.left)} {symbol_to_str(node.op)} {ast_to_expression(node.right)}"
    elif isinstance(node, ast.UnaryOp):
        return f"{symbol_to_str(node.op)}{ast_to_expression(node.operand)}"


class ASTWrapper:

    task_counters: dict[str, int]
    tasks: dict[str, Task]
    last_task: str

    def __init__(self, abstrac_syntax_tree) -> None:
        self.ast = abstrac_syntax_tree
        self.tasks = {}
        self.task_counters = defaultdict(int)
        self.last_task = None

    def __repr__(self) -> str:
        return f"ASTWrapper({self.ast})"

    def _get_task_name(self, task_type: str) -> str:
        task_id = self.task_counters[task_type]
        self.task_counters[task_type] += 1
        return f"{task_type}-{task_id}"

    def _is_pure(self, node: Any) -> bool:
        for node in ast.walk(node):
            if isinstance(node, SymbolNode) and node.slice is not None:
                yield False
                continue
            yield not isinstance(node, (FunctionNode, ))

    def _build(self, node: Any, data: DataMapping) -> str:

        # if node only contains symbols or binary operators, create eval task
        is_pure = all(self._is_pure(node))
        if isinstance(node, ast.Constant):
            task_name = self._get_task_name("constant")
            self.tasks[task_name] = (SUPPORTED_FUNCTIONS["constant"], node.value)
            return task_name

        if is_pure:
            task_name = self._get_task_name("eval")
            partial_eval = partial(SUPPORTED_FUNCTIONS["eval"], global_dict=data)
            self.tasks[task_name] = (partial_eval, ast_to_expression(node))
            return task_name

        if isinstance(node, FunctionNode):
            var_name = node.name
            previous_tasks = []
            for arg in node.arguments:
                previous_tasks.append(self._build(arg, data))
            task_name = self._get_task_name(f"func-{var_name}")

            slice_args = []
            if node.slice is not None:
                # convert slice to tuple
                for item in node.slice:
                    if item is None:
                        slice_args.append(None)
                        continue
                    slice_args.append(self._build(item, data))

                self.tasks[task_name] = (SUPPORTED_FUNCTIONS[var_name], previous_tasks[0], slice_args)
            else:
                self.tasks[task_name] = (SUPPORTED_FUNCTIONS[var_name], *previous_tasks)
            return task_name

    def build(self, data: DataMapping) -> None:
        self.tasks = {}
        self.task_counters = defaultdict(int)
        self.last_task = self._build(self.ast, data)

    def to_tasks(self, data: DataMapping) -> dict[str, Task]:
        if not self.tasks:
            self.build(data)
        return self.tasks


def expression_to_ast(expression):
    tree = ast.parse(expression, mode='eval')
    transformer = Transformer()
    new_tree = transformer.visit(tree.body)
    return ASTWrapper(new_tree)
