use std::collections::HashMap;

trait Column {
    fn pivot(&self) -> Option<usize>;
    fn add_col(&mut self, other: &Self);
}

struct VecColumn {
    col: Vec<usize>,
}

impl Column for VecColumn {
    fn pivot(&self) -> Option<usize> {
        Some(*(self.col.iter().last()?))
    }

    fn add_col(&mut self, other: &Self) {
        todo!()
    }
}

struct StandardAlgo<C: Column, T: Iterator<Item = (usize, C)>> {
    enumerated_cols: T,
    // Takes in a low and returns the column with that low
    low_inverse: HashMap<usize, C>,
}

impl<C: Column, T: Iterator<Item = (usize, C)>> StandardAlgo<C, T> {
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

type Pairing = (usize, usize);

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
