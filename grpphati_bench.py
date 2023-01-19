import networkx as nx
import time
from grpphati.pipelines.grounded import make_grounded_pipeline
from grpphati.homologies import RegularPathHomology
from grpphati.backends import PHATBackend
from grpphati.filtrations import ShortestPathFiltration
from persuit_grpphati_backend import PersuitBackend
from phat import reductions

def timed(f):
    tic = time.time()
    output = f()
    toc = time.time()
    elapsed = f"{toc - tic}s"
    return (output, elapsed)


phat_pipeline = make_grounded_pipeline(
        ShortestPathFiltration,
        RegularPathHomology,
        backend = PHATBackend(reduction=reductions.standard_reduction),
        optimisation_strat = None
        )

phat_twist_pipeline = make_grounded_pipeline(
        ShortestPathFiltration,
        RegularPathHomology,
        backend = PHATBackend(reduction=reductions.twist_reduction),
        optimisation_strat = None
        )


pipeline = make_grounded_pipeline(
        ShortestPathFiltration,
        RegularPathHomology,
        backend = PersuitBackend(in_parallel=True),
        optimisation_strat = None
        )

pipeline_serial = make_grounded_pipeline(
        ShortestPathFiltration,
        RegularPathHomology,
        backend = PersuitBackend(in_parallel=False),
        optimisation_strat = None
        )

N= 200
G6 = nx.complete_graph(N, create_using=nx.DiGraph)
#print(G6.number_of_nodes())
#(out, elap) = timed(lambda: GrPPH_par_wedge(G6))
#print(len(out.barcode))
#print(elap)
#(out, elap) = timed(lambda: pipeline(G6))
#print(len(out.barcode))
#print(elap)
#(out, elap) = timed(lambda: pipeline_serial(G6))
#print(len(out.barcode))
#print(elap)

#phat_pipeline(G6)
tic1 = time.time()

print("Start")
pipeline(G6)
print("End")

tic2 = time.time()

print("Start")
pipeline_serial(G6)
print("End")

tic3 = time.time()

print("Start")
phat_pipeline(G6)
print("End")

tic4 = time.time()

print("Start")
phat_twist_pipeline(G6)
print("End")

tic5 = time.time()
print(tic2 - tic1)
print(tic3 - tic2)
print(tic4 - tic3)
print(tic5 - tic4)