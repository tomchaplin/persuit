use bit_set::BitSet;
use bitvec::prelude::*;
use crossbeam_channel;
use pyo3::prelude::*;
use pyo3::types::PyIterator;
use std::cmp::Ordering;
use std::collections::btree_set::BTreeSet;
use std::iter::Enumerate;
use std::marker::Send;
use std::ops::{BitXor, BitXorAssign};
use std::{collections::HashMap, thread};

pub trait Column: Send {
    fn pivot(&self) -> Option<usize>;
    fn add_col(&mut self, other: &Self);
}

#[derive(Default)]
struct VecColumn {
    col: Vec<usize>,
}

impl VecColumn {
    // Returns the index where we should try to insert next entry
    fn add_entry(&mut self, entry: usize, starting_idx: usize) -> usize {
        let mut working_idx = starting_idx;
        while let Some(value_at_idx) = self.col.iter().nth(working_idx) {
            match value_at_idx.cmp(&entry) {
                Ordering::Less => {
                    working_idx += 1;
                    continue;
                }
                Ordering::Equal => {
                    self.col.remove(working_idx);
                    return working_idx;
                }
                Ordering::Greater => {
                    self.col.insert(working_idx, entry);
                    return working_idx + 1;
                }
            }
        }
        // Bigger than all idxs in col - add to end
        self.col.push(entry);
        return self.col.len() - 1;
    }
}

impl Column for VecColumn {
    fn pivot(&self) -> Option<usize> {
        Some(*(self.col.iter().last()?))
    }

    fn add_col(&mut self, other: &Self) {
        let mut working_idx = 0;
        for entry in other.col.iter() {
            working_idx = self.add_entry(*entry, working_idx);
        }
    }
}

#[derive(Debug, PartialEq)]
struct BitVecColumn {
    col: BitVec<usize, Lsb0>,
}

impl BitVecColumn {
    fn from_sparse_col(sparse_col: Vec<usize>) -> Self {
        let col = if let Some(&max_elem) = sparse_col.last() {
            let mut bv = bitvec![usize, Lsb0; 0; max_elem+1];
            for index in sparse_col.into_iter() {
                bv.set(index, true);
            }
            bv
        } else {
            BitVec::<usize, Lsb0>::new()
        };
        BitVecColumn { col }
    }
}

impl Column for BitVecColumn {
    fn pivot(&self) -> Option<usize> {
        self.col.last_one()
    }

    fn add_col(&mut self, other: &Self) {
        // TODO: Write tests - what happens if other is longer?
        if other.col.len() > self.col.len() {
            let storage = self.col.clone();
            self.col = other.col.clone().bitxor(storage);
        } else {
            self.col.bitxor_assign(&other.col);
        }
    }
}

#[derive(Debug, PartialEq)]
struct BitSetColumn {
    col: BitSet,
}

impl BitSetColumn {
    fn from_sparse_col(sparse_col: Vec<usize>) -> Self {
        BitSetColumn {
            col: sparse_col.into_iter().collect(),
        }
    }
}

impl Column for BitSetColumn {
    fn pivot(&self) -> Option<usize> {
        self.col.iter().max()
    }

    fn add_col(&mut self, other: &Self) {
        self.col.symmetric_difference_with(&other.col);
    }
}

#[derive(Debug, PartialEq)]
struct BTreeSetColumn {
    col: BTreeSet<usize>,
}

impl BTreeSetColumn {
    fn from_sparse_col(sparse_col: Vec<usize>) -> Self {
        BTreeSetColumn {
            col: sparse_col.into_iter().collect(),
        }
    }
}

impl Column for BTreeSetColumn {
    fn pivot(&self) -> Option<usize> {
        Some(*self.col.last()?)
    }

    fn add_col(&mut self, other: &Self) {
        self.col = self.col.symmetric_difference(&other.col).cloned().collect();
    }
}

type Pairing = (usize, usize);

#[derive(Default)]
pub struct StandardAlgo<C: Column, T: Iterator<Item = (usize, C)>> {
    enumerated_cols: T,
    // Takes in a low and returns the column with that low
    low_inverse: HashMap<usize, C>,
}

impl<C: Column, T: Iterator<Item = (usize, C)>> StandardAlgo<C, T> {
    fn new(enumerated_cols: T) -> Self {
        StandardAlgo {
            enumerated_cols,
            low_inverse: HashMap::new(),
        }
    }

    fn col_with_same_low(&self, col: &C) -> Option<&C> {
        let pivot = col.pivot()?;
        self.low_inverse.get(&pivot)
    }

    fn reduce_col(&self, mut col: C) -> C {
        while let Some(lower_col) = self.col_with_same_low(&col) {
            col.add_col(lower_col);
        }
        // Cannot reduce any further
        return col;
    }
}

// A StandardAlgo struct can be viewed as an iterator over pairings
impl<C: Column, T: Iterator<Item = (usize, C)>> Iterator for StandardAlgo<C, T> {
    type Item = Pairing;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(enum_col) = self.enumerated_cols.next() {
            let reduced = self.reduce_col(enum_col.1);
            match reduced.pivot() {
                Some(pivot) => {
                    // Need to record that this column has low=pivot
                    self.low_inverse.insert(pivot, reduced);
                    // This column gets paired with its pivot
                    return Some((pivot, enum_col.0));
                }
                // Empty column, no pairing found
                None => continue,
            }
        }
        // Ran out of columns
        return None;
    }
}

pub fn std_persuit<C, T>(
    col_iterator: T,
) -> StandardAlgo<C, crossbeam_channel::IntoIter<(usize, C)>>
where
    T: Iterator<Item = C> + Send + 'static,
    C: Column + 'static,
{
    //let (tx, rx) = flume::unbounded();
    let (tx, rx) = crossbeam_channel::unbounded();
    thread::spawn(move || {
        let mut enumerated_cols = col_iterator.enumerate();
        while let Some(enum_col) = enumerated_cols.next() {
            tx.send(enum_col).unwrap();
        }
    });
    StandardAlgo::new(rx.into_iter())
}

pub fn std_persuit_collected<C, T>(col_iterator: T) -> Vec<Pairing>
where
    T: Iterator<Item = C>,
    C: Column + 'static,
{
    let (tx, rx) = flume::unbounded();
    let receiver = thread::spawn(move || {
        StandardAlgo::new(
            rx.iter()
                .take_while(|msg: &Option<(usize, C)>| msg.is_some())
                .map(|msg| msg.unwrap()),
        )
        .collect()
    });
    let mut enumerated_cols = col_iterator.enumerate();
    while let Some(enum_col) = enumerated_cols.next() {
        tx.send(Some(enum_col)).unwrap();
    }
    // Tell receiver that we're done
    tx.send(None).unwrap();
    receiver.join().expect("Panic in StandardAlgo")
}

pub fn std_persuit_serial<C, T>(col_iterator: T) -> StandardAlgo<C, Enumerate<T>>
where
    T: Iterator<Item = C>,
    C: Column,
{
    StandardAlgo::new(col_iterator.enumerate())
}

#[pyfunction]
#[pyo3(name = "std_persuit")]
fn std_persuit_py(iterator: &PyIterator) -> Vec<Pairing> {
    let columns = iterator
        .map(|i| {
            i.and_then(PyAny::extract::<Vec<usize>>)
                .expect("Could not parse sparse columns from iterator")
        })
        .map(|col| VecColumn { col });
    println!("Start persistence computation");
    std_persuit_collected(columns)
}

#[pyfunction]
#[pyo3(name = "std_persuit_serial")]
fn std_persuit_serial_py(iterator: &PyIterator) -> Vec<Pairing> {
    let columns = iterator
        .map(|i| {
            i.and_then(PyAny::extract::<Vec<usize>>)
                .expect("Could not parse sparse columns from iterator")
        })
        .map(|col| VecColumn { col });
    println!("Start persistence computation");
    std_persuit_serial(columns).collect()
}

#[pyfunction]
#[pyo3(name = "std_persuit_serial_bs")]
fn std_persuit_serial_bs_py(iterator: &PyIterator) -> Vec<Pairing> {
    let columns = iterator
        .map(|i| {
            i.and_then(PyAny::extract::<Vec<usize>>)
                .expect("Could not parse sparse columns from iterator")
        })
        .map(|col| BitSetColumn::from_sparse_col(col));
    println!("Start persistence computation");
    std_persuit_serial(columns).collect()
}

#[pyfunction]
#[pyo3(name = "std_persuit_serial_bts")]
fn std_persuit_serial_bts_py(iterator: &PyIterator) -> Vec<Pairing> {
    let columns = iterator
        .map(|i| {
            i.and_then(PyAny::extract::<Vec<usize>>)
                .expect("Could not parse sparse columns from iterator")
        })
        .map(|col| BTreeSetColumn::from_sparse_col(col));
    println!("Start persistence computation");
    std_persuit_serial(columns).collect()
}

#[pymodule]
fn persuit(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(std_persuit_py, m)?)?;
    m.add_function(wrap_pyfunction!(std_persuit_serial_py, m)?)?;
    m.add_function(wrap_pyfunction!(std_persuit_serial_bs_py, m)?)?;
    m.add_function(wrap_pyfunction!(std_persuit_serial_bts_py, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

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
        let pairings: Vec<Pairing> = std_persuit(columns.into_iter()).collect();
        let correct = vec![(1, 4), (2, 5), (3, 6), (8, 9), (7, 10)];
        assert_eq!(pairings, correct)
    }

    #[test]
    fn it_works_bv() {
        let file = File::open("test.mat").unwrap();
        let columns: Vec<BitVecColumn> = BufReader::new(file)
            .lines()
            .map(|l| {
                let l = l.unwrap();
                if l.is_empty() {
                    vec![]
                } else {
                    l.split(",").map(|c| c.parse().unwrap()).collect()
                }
            })
            .map(|l| BitVecColumn::from_sparse_col(l))
            .collect();
        let pairings: Vec<Pairing> = std_persuit(columns.into_iter()).collect();
        let correct = vec![(1, 4), (2, 5), (3, 6), (8, 9), (7, 10)];
        assert_eq!(pairings, correct)
    }

    #[test]
    fn bv_pivot() {
        let col = BitVecColumn::from_sparse_col(vec![0, 1, 5]);
        assert_eq!(col.pivot(), Some(5));
    }

    #[test]
    fn bv_add_col() {
        let mut col1 = BitVecColumn::from_sparse_col(vec![0, 1, 5]);
        let col2 = BitVecColumn::from_sparse_col(vec![0, 1, 8]);
        let col3 = BitVecColumn::from_sparse_col(vec![5, 8]);
        col1.add_col(&col2);
        assert_eq!(col1, col3);
    }

    #[test]
    fn bv_add_col2() {
        let col1 = BitVecColumn::from_sparse_col(vec![0, 1, 5]);
        let mut col2 = BitVecColumn::from_sparse_col(vec![0, 1, 8]);
        let col3 = BitVecColumn::from_sparse_col(vec![5, 8]);
        col2.add_col(&col1);
        assert_eq!(col2, col3);
    }

    #[test]
    fn bs_pivot() {
        let col = BitSetColumn::from_sparse_col(vec![0, 1, 5]);
        assert_eq!(col.pivot(), Some(5));
    }

    #[test]
    fn bs_add_col() {
        let mut col1 = BitSetColumn::from_sparse_col(vec![0, 1, 5]);
        let col2 = BitSetColumn::from_sparse_col(vec![0, 1, 8]);
        let col3 = BitSetColumn::from_sparse_col(vec![5, 8]);
        col1.add_col(&col2);
        assert_eq!(col1, col3);
    }

    #[test]
    fn bs_add_col2() {
        let col1 = BitSetColumn::from_sparse_col(vec![0, 1, 5]);
        let mut col2 = BitSetColumn::from_sparse_col(vec![0, 1, 8]);
        let col3 = BitSetColumn::from_sparse_col(vec![5, 8]);
        col2.add_col(&col1);
        assert_eq!(col2, col3);
    }
}
