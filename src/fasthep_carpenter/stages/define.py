"""Stages for defining new variables"""

from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult
from ..expressions import process_expression

VariableDefinition = dict[str, str]


class Define(ProcessingStep):
    """Creates new variables using a string-based expression.

    Example:
      ::

        variables:
          - Muon_pt: "sqrt(Muon_px**2 + Muon_py**2)"
          - Muon_is_good: (Muon_iso > 0.3) & (Muon_pt > 10)
          - NGoodMuons: {reduce: count_nonzero, formula: Muon_is_good}
    """

    _name: str
    _variables = list[VariableDefinition]

    def __init__(self, name: str, variables: list[VariableDefinition]) -> None:
        self._name = name
        self._variables = variables

    def __call__(self, data: DataMapping) -> ProcessingStepResult:
        results = {}
        for name, expression in self._variables.items():
            result = process_expression(data, expression)
            results[name] = result
            data.add_variable(name, result)

        return ProcessingStepResult(
            data=data,
            error_code=0,
            error_message="",
            result=results,
            bookkeeping={self._name: self._variables},
            # reducer=None,
        )
