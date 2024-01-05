import networkx as nx
import numpy as np
import time
import gc
from grpphati.pipelines.grounded import make_grounded_pipeline
from grpphati.homologies import RegularPathHomology
from grpphati.filtrations import ShortestPathFiltration
from persuit_grpphati_backend import PersuitBackend


def timed(f):
    def timed_f(*args, **kwargs):
        tic = time.time()
        output = f(*args, **kwargs)
        toc = time.time()
        elapsed = toc - tic
        gc.collect()
        return (output, elapsed)

    return timed_f


pipeline = make_grounded_pipeline(
    ShortestPathFiltration,
    RegularPathHomology,
    backend=PersuitBackend(in_parallel=True),
    optimisation_strat=None,
)

pipeline_unsafe = make_grounded_pipeline(
    ShortestPathFiltration,
    RegularPathHomology,
    backend=PersuitBackend(internal="unsafe"),
    optimisation_strat=None,
)

pipeline_serial = make_grounded_pipeline(
    ShortestPathFiltration,
    RegularPathHomology,
    backend=PersuitBackend(in_parallel=False),
    optimisation_strat=None,
)

pipeline_serial_bs = make_grounded_pipeline(
    ShortestPathFiltration,
    RegularPathHomology,
    backend=PersuitBackend(in_parallel=False, internal="bitset"),
    optimisation_strat=None,
)

pipeline_serial_bts = make_grounded_pipeline(
    ShortestPathFiltration,
    RegularPathHomology,
    backend=PersuitBackend(in_parallel=False, internal="btreeset"),
    optimisation_strat=None,
)


N = 200
G6 = nx.complete_graph(N, create_using=nx.DiGraph)


def run(p, repeats=100):
    times = np.array([timed(p)(G6)[1] for _ in range(repeats)])
    median = np.median(times)
    std = np.std(times)
    return times, median, std


results = []
print("Default")
results.append(run(pipeline))
print("Unsafe")
results.append(run(pipeline_unsafe))
print("Serial")
results.append(run(pipeline_serial))

for times, mean, std in results:
    print(f"{mean:.3f} ± {std:.3f}")

for times, mean, std in results:
    print("[", end="")
    for idx, t in enumerate(times):
        if idx != 0:
            print(" ", end="")
        print(f"{t:.3f}", end="")
    print("]", end="")
    print("\n", end="")
