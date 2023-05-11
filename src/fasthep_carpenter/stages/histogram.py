
from typing import Any

from fasthep_logging import get_logger

from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult

log = get_logger("FASTHEP::Carpenter")


class Histogram(ProcessingStep):

    _name: str
    _prefix: str = "__histogram__"
    _binning: dict[str, tuple[float, float, int]]
    _weights: dict[str, str]
    multiplex: bool = True

    def __init__(self, name: str, binning: dict[str, tuple[Any]], weights: dict[str, str] = None, **kwargs) -> None:
        self._name = name
        self._binning = binning
        self._weights = weights

    def __call__(self, data: ProcessingStepResult) -> ProcessingStepResult:
        import awkward as ak
        log.trace(f"Processing stage {self._name}")
        data.bookkeeping[(self.__class__.__name__, self._name)] = self.__dask_tokenize__()

        import hist
        axes = []
        for name, (bins, low, high) in self._binning.items():
            axes.append(hist.axis.Regular(bins, low, high, name=name))
        histogram = hist.Hist(
            *axes,
            hist.storage.Weight(),
        )
        arrays = data.data.arrays(list(self._binning.keys()), how=dict)

        # extend weights to the same length as the data
        weight = data.data[self._weights["weighted"]]
        weights = {}
        for name, value in arrays.items():
            weights[name] = ak.flatten(ak.broadcast_arrays(weight, value)[0])
        arrays = {k: ak.flatten(v, axis=1) for k, v in arrays.items()}
        # remove empty entries
        arrays = {k: v[~ak.is_none(v)] for k, v in arrays.items()}
        weights = {k: v[~ak.is_none(v)] for k, v in weights.items()}

        weight_values = weights.popitem()[1]

        histogram.fill(**arrays, weight=weight_values)
        if data.result is None:
            data.result = {}
        data.result[self.name] = histogram

        return data

    def __dask_tokenize__(self):
        return (Histogram, self._name, self._binning, self._weights)

    @property
    def name(self) -> str:
        return self._name
