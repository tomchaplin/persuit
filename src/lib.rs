use std::cmp::Ordering;
use std::iter::Enumerate;
use std::marker::Send;
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

pub fn persuit<C, T>(col_iterator: T) -> StandardAlgo<C, flume::IntoIter<(usize, C)>>
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

pub fn persuit_serial<C, T>(col_iterator: T) -> StandardAlgo<C, Enumerate<T>>
where
    T: Iterator<Item = C> + Send + 'static,
    C: Column + 'static,
{
    StandardAlgo::new(col_iterator.enumerate())
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
        let pairings: Vec<Pairing> = persuit(columns.into_iter()).collect();
        let correct = vec![(1, 4), (2, 5), (3, 6), (8, 9), (7, 10)];
        assert_eq!(pairings, correct)
    }
}
