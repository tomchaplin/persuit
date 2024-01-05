use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::thread;

use crate::{Column, Pairing};

fn col_with_same_low<'a, C: Column>(low_inverse: &HashMap<usize, &'a C>, col: &C) -> Option<&'a C> {
    let pivot = col.pivot()?;
    low_inverse.get(&pivot).copied()
}

pub fn unsafe_persuit<C, Iter>(cols: Iter, len: usize) -> Vec<Pairing>
where
    Iter: Iterator<Item = C>,
    C: Send + Column,
{
    let mut storage = Vec::with_capacity(len);
    for _ in 0..len {
        storage.push(CachePadded::new(Mutex::new(MaybeUninit::uninit())))
    }

    // Obtain a write lock on every Mutex
    let locks: Vec<_> = storage.iter().map(|mtx| mtx.lock()).collect();

    thread::scope(|s| {
        let reducer_thread = s.spawn(|| {
            let mut pairings = vec![];
            let mut low_inverse: HashMap<usize, &C> = HashMap::new();
            for (idx, mtx) in storage.iter().enumerate() {
                // This will block until thread 1 sends
                let mut write_handle = mtx.lock();
                // SAFETY: This column is initialised before the previous lock is dropped
                let write_handle = unsafe { write_handle.assume_init_mut() };

                // Reduce
                while let Some(lower_col) = col_with_same_low(&low_inverse, write_handle) {
                    write_handle.add_col(lower_col);
                }

                // Store pivot
                if let Some(pivot) = write_handle.pivot() {
                    // SAFETY:
                    // 1. Once we drop write_handle, we will never lock or mutate this column again
                    // 2. storage is kept at least until the end of the for loop which is also when low_inverse is dropped
                    // 3. We have already assumed that the data behind mtx is initialised
                    // 4. Hence it is safe to dereference the raw pointer and store a reference in low_inverse
                    low_inverse.insert(pivot, unsafe { (*mtx.data_ptr()).assume_init_ref() });
                    pairings.push((pivot, idx));
                }
            }
            // Return the pairings
            pairings
        });

        // Send each column
        for (col, mut lock) in cols.zip(locks.into_iter()) {
            lock.write(col);
            drop(lock);
        }

        let pairings = reducer_thread.join().unwrap();

        // Drop each column - we have to do this explicility
        // because MaybeUninit will not run the destructor,
        // leaving our columns unfreed
        for mtx in storage.iter() {
            unsafe { (*mtx.data_ptr()).assume_init_drop() }
        }

        pairings
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    use super::*;
    use crate::VecColumn;

    #[test]
    fn it_works() {
        let file = File::open("test.mat").unwrap();
        let columns: Vec<VecColumn> = BufReader::new(file)
            .lines()
            .map(|l| {
                let l = l.unwrap();
                if l.is_empty() {
                    vec![]
                } else {
                    l.split(",").map(|c| c.parse().unwrap()).collect()
                }
            })
            .map(|l| VecColumn { col: l })
            .collect();
        let len = columns.len();
        let pairings = unsafe_persuit(columns.into_iter(), len);
        let correct = vec![(1, 4), (2, 5), (3, 6), (8, 9), (7, 10)];
        assert_eq!(pairings, correct)
    }
}
