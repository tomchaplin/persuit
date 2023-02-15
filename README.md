# persuit

Persuit is an experimental persistent homology (PH) library, which aims to speed up your PH pipeline by reducing the boundary matrix **whilst** it is being constructed.
The library is written in Rust and Python bindings are provided via PyO3.

> **Warning**
> Persuit is currently in alpha.
> The implementation of the standard algorithm is far from optimised, the API is not fixed and no tests have been written.
> Use at your own risk.

## Motivation

Here is the rough outline of many PH pipelines:

1. Read in data.
2. Build up a basis for the columns of your boundary matrix.
3. Sort this basis by the filtration time and dimension.
4. Compute a sparse representation of the boundary matrix.
5. Pass the sparse matrix to a PH library (e.g. Eirene or PHAT) to reduce the boundary matrix and hence compute pairings.

In many (although not all) pipelines, steps 1-4 takes significantly longer than step 5.
Indeed, often step 4 alone can take longer than step 5.
However, step 5 often still takes a significant time to complete.
In such circumstances, Persuit might offer a speed up.

Consider the "standard algorithm" for persistence computation:
```
for each column j:
    while ∃i < j with low(i) = low(j):
        add col[i] to col[j]
    endwhile
endfor
```
Note that in order to reduce column `j` we only need the previous columns.
Therefore, we can start reducing `j` as soon as it is available!
That is, we can compute PH **while** we compute the sparse boundary matrix and potentially hide some of the cost of the standard algorithm.

## Main Idea

The two core functions provided by Persuit are `std_persuit` and `std_persuit_collected`.
Let's focus on `std_persuit`.

```rust
pub fn std_persuit<C, T>(col_iterator: T) -> StandardAlgo<C, flume::IntoIter<(usize, C)>>
where
    T: Iterator<Item = C> + Send + 'static,
    C: Column + 'static,
{
    let (tx, rx) = flume::unbounded();
    thread::spawn(move || {
        let mut enumerated_cols = col_iterator.enumerate();
        while let Some(enum_col) = enumerated_cols.next() {
            tx.send(enum_col).unwrap();
        }
    });
    StandardAlgo::new(rx.into_iter())
}
```

Note that the output of this function is an iterator over the pairings computed by the standard algorithm.
Despite all of the annotations, the signature of `std_persuit` is essentially
```
iterator over columns -> iterator over parings
```

The way this is achieved is:

1. Spawn a new thread and setup a channel for communication.
2. The new thread consumes columns from the input iterator and puts them on the channel.
3. The main thread consumes columns off the channel and performs the standard algorithm on these columns, unaware they are coming from a different thread.

Since the new thread is constantly consuming the iterator, we construct the sparse matrix at the usual rate.
Meanwhile, the main thread is reducing columns of this sparse matrix as soon as it can.

Currently, due to the overhead of inter-thread communication this is usually slower than the serial algorithm.

## Usage

The main Python bindings are `std_persuit` and `std_persuit_serial`.
These functions take one argument: an [iterator](https://docs.python.org/3/c-api/iterator.html) over sparse columns.
Currently, `persuit` only supports ℤ₂ homology and hence your matrix should also have enties in ℤ₂.
The ith iterate of the provided iterator should be an increasing list of indices of the non-zero rows in the ith column of your boundary matrix.
See [`test.py`](https://github.com/tomchaplin/persuit/blob/main/test.py) for an illustrative example.

## TODO

- [ ] Try out `FnvHasMap`, `IndexMap` and `BTreeMap` for storing `low_inverse`
- [ ] Try out linked lists and `bitvec` as Column types
- [ ] Return (Lex-optimal) representatives
- [x] Python bindings
- [ ] Implement other PH algorithms, e.g. clear and compress?
- [ ] Implement parallel over dimensions?
- [x] Write a basic README
- [ ] Find example where this actually does speed up PH (need PH computation to be a significant portion of the time)
- [ ] Write tests (unit, integration + property)
- [ ] Benchmark
