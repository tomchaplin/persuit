from grpphati.backends.abstract import Backend
from grpphati.sparsifiers import GeneratorSparsifier
from grpphati.results import Result
from persuit import (
    std_persuit,
    std_persuit_serial,
    std_persuit_serial_bs,
    std_persuit_serial_bts,
    unsafe_persuit,
    unsafe_persuit_v2,
)
from pprint import pprint


class PersuitBackend(Backend):
    def __init__(self, in_parallel=True, internal="vec"):
        self.in_parallel = in_parallel
        self.internal = internal
        self.sparsifier = GeneratorSparsifier(return_dimension=False)

    def compute_ph(self, cols) -> Result:
        cols.sort(key=lambda col: (col.entrance_time, col.dimension()))
        # Extract rows, ignore dimension
        sparse_cols = self.sparsifier(iter(cols))
        if self.in_parallel:
            if self.internal == "unsafe":
                pairs = unsafe_persuit(sparse_cols, len(cols))
            elif self.internal == "unsafe_v2":
                pairs = unsafe_persuit_v2(sparse_cols, len(cols))
            else:
                pairs = std_persuit(sparse_cols)
        else:
            if self.internal == "bitset":
                pairs = std_persuit_serial_bs(sparse_cols)
            elif self.internal == "btreeset":
                pairs = std_persuit_serial_bts(sparse_cols)
            else:
                pairs = std_persuit_serial(sparse_cols)
        pairs.sort()
        result = Result.empty()
        result.add_paired(pairs, cols)
        result.add_unpaired(pairs, cols)
        return result
