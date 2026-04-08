use crate::Row;

pub struct ResultSet {
    pub rows: Vec<Row>,
}

impl ResultSet {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Row> {
        self.rows.iter()
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}
