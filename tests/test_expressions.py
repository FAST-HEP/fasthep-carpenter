import awkward as ak

from fasthep_carpenter.expressions import process_expression, register_function, extract_functions_and_parameters, _replace_attributes,\
    tasks_from_expression


class DummyMapping:
    def __init__(self, data):
        self.data = data

    def evaluate(self, expression, **kwargs):
        return ak.numexpr.evaluate(expression, self.data, **kwargs)

def test_replace_attributes():
    assert _replace_attributes("a + b") == "a + b"
    assert _replace_attributes("a.some + b.two + c") == "a__DOT__some + b__DOT__two + c"

def test_extract_functions_and_parameters():
    assert extract_functions_and_parameters("a + b") == [("eval", "a + b")]
    assert extract_functions_and_parameters("count(a + b + c)") == [("eval", "a + b + c"), ("count", "eval")]
    # assert extract_functions_and_parameters("count(a, b)") == [("eval", "a"), ("eval", "b"), ("count", "eval")]
    # assert extract_functions_and_parameters("count(a + b + c) + some(a, b)") == [("eval", "a + b + c"), ("count", "eval")]

def test_tasks_from_expression():
    expression = "count(a + b + c)"
    tasks = tasks_from_expression(expression)
    assert len(tasks) == 2
    assert tasks["task-0"] == ("eval", "a + b + c")
    assert tasks["task-1"] == ("count", "task-0")

def test_process_expression():
    data = DummyMapping({"a": [1], "b": [2]})
    assert process_expression(data, "a + b") == [3]


# def test_process_expression_with_function():
#     data = DummyMapping({"a": [1], "b": [2]})

#     def double_it(array):
#         return array * 2

#     register_function("double_it", double_it)
#     assert process_expression(data, "double_it(a + b)") == [6]
