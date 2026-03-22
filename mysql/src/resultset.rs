use crate::{row::Row, Result};

/// 结果集
pub struct ResultSet<'a> {
    rows: Vec<mysql_async::Row>,
    current: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ResultSet<'a> {
    pub(crate) fn new(rows: Vec<mysql_async::Row>) -> Self {
        Self {
            rows,
            current: 0,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn column_count(&self) -> usize {
        self.rows.first().map(|r| r.columns().len()).unwrap_or(0)
    }

    pub fn fetch(&mut self) -> Option<Row> {
        if self.current < self.rows.len() {
            let row = self.rows[self.current].clone();
            self.current += 1;
            Some(Row::new(row))
        } else {
            None
        }
    }

    pub fn reset(&mut self) {
        self.current = 0;
    }

    pub async fn first_row(&mut self) -> Result<Option<Row>> {
        self.reset();
        Ok(self.fetch())
    }

    pub async fn collect_row(&mut self) -> Result<Vec<Row>> {
        self.reset();
        let mut rows = Vec::with_capacity(self.rows.len());
        while let Some(row) = self.fetch() {
            rows.push(row);
        }
        Ok(rows)
    }

    pub async fn collect(mut self) -> Result<Vec<Row>> {
        self.collect_row().await
    }

    pub async fn scalar_string(&mut self) -> Result<Option<String>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_string(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i64(&mut self) -> Result<Option<i64>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i64(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i32(&mut self) -> Result<Option<i32>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i32(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i16(&mut self) -> Result<Option<i16>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i16(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_u8(&mut self) -> Result<Option<u8>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_u8(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_f64(&mut self) -> Result<Option<f64>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_f64(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_f32(&mut self) -> Result<Option<f32>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_f32(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_bool(&mut self) -> Result<Option<bool>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_bool(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_decimal(&mut self) -> Result<Option<rust_decimal::Decimal>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_decimal(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_datetime(&mut self) -> Result<Option<chrono::NaiveDateTime>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_datetime(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_date(&mut self) -> Result<Option<chrono::NaiveDate>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_date(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_time(&mut self) -> Result<Option<chrono::NaiveTime>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_time(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_uuid(&mut self) -> Result<Option<uuid::Uuid>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_uuid(0)
        } else {
            Ok(None)
        }
    }
}
