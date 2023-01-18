from persuit import std_persuit
from pprint import pprint
import time
#from data.wedge_of_two_k20 import test_sparse_mat

test_sparse_mat = [
   [],
   [],
   [],
   [],
   [0, 1],
   [1, 2],
   [2, 3],
   [0, 3],
   [0, 2],
   [6, 7, 8],
   [4, 5, 8],
]


class SlowIterator:
    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        time.sleep(0.0001)
        return self.iterator.__next__()


# This represents an iterator that builds our sparse matrix and is slow
def get_slow_iterator(sparse_mat):
    return SlowIterator(iter(sparse_mat))


# Option 1: Collect the entire matrix into memory, then do persistence
tic = time.time()
sparse_mat_collected = list(get_slow_iterator(test_sparse_mat))
print("Starting persistence computation")
pairings1 = std_persuit(iter(sparse_mat_collected))
print("Finished persistence computation")
toc = time.time()
elapsed1 = toc - tic

time.sleep(1)

# Option 2: Do persistence in parallel with the sparse matrix construction
tic = time.time()
print("Starting persistence computation")
pairings2 = std_persuit(get_slow_iterator(test_sparse_mat))
print("Finished persistence computation")
toc = time.time()
elapsed2 = toc - tic

print(len(pairings1))
print(len(pairings2))
print(f"Method 1: {elapsed1}s")
print(f"Method 2: {elapsed2}s")

assert pairings1 == pairings2
