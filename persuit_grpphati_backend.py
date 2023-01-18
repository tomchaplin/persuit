from grpphati.backends.abstract import Backend
from grpphati.utils.column import SparseConstructor
from grpphati.results import Result
from persuit import std_persuit, std_persuit_serial
from pprint import pprint


class PersuitBackend(Backend):
    def __init__(self, in_parallel=True):
        self.in_parallel = in_parallel

    def compute_ph(self, cols) -> Result:
        cols.sort(key=lambda col: (col.entrance_time, col.dimension()))
        # Extract rows, ignore dimension
        sparse_cols = map(lambda x: x[1], SparseConstructor(iter(cols)))
        if self.in_parallel:
            pairs = std_persuit(sparse_cols)
        else:
            pairs = std_persuit_serial(sparse_cols)
        pairs.sort()
        result = Result.empty()
        result.add_paired(pairs, cols)
        result.add_unpaired(pairs, cols)
        return result
