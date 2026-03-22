use crate::{Error, Result};
use mysql_async::{consts::ColumnType as MyColumnType, prelude::FromValue, Value};
use std::fmt::{self, Debug, Formatter};

/// 支持索引方式 (usize 或 &str)
pub trait QueryIdx {
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl QueryIdx for &str {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.0.columns().iter().position(|c| c.name_str() == *self)
    }
}

    pub enum ColumnType {
    String,
    Int,
    Float,
    Bool,
    Decimal,
    DateTime,
    Date,
    Time,
    Uuid,
    Unknown,
}

pub enum ColumnData {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Decimal(rust_decimal::Decimal),
    DateTime(chrono::NaiveDateTime),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Uuid(uuid::Uuid),
}

impl Debug for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ColumnData::String(v) => Debug::fmt(v, f),
            ColumnData::Int(v) => Debug::fmt(v, f),
            ColumnData::Float(v) => Debug::fmt(v, f),
            ColumnData::Bool(v) => Debug::fmt(v, f),
            ColumnData::Decimal(v) => Debug::fmt(v, f),
            ColumnData::DateTime(v) => Debug::fmt(v, f),
            ColumnData::Date(v) => Debug::fmt(v, f),
            ColumnData::Time(v) => Debug::fmt(v, f),
            ColumnData::Uuid(v) => Debug::fmt(v, f),
        }
    }
}

/// 行数据
pub struct Row(pub(crate) mysql_async::Row);

impl Row {
    pub(crate) fn new(row: mysql_async::Row) -> Self {
        Row(row)
    }

    pub fn column_count(&self) -> usize {
        self.0.columns().len()
    }

    pub fn column_name(&self, idx: usize) -> Option<String> {
        self.0.columns().get(idx).map(|c| c.name_str().to_string())
    }

    /// 获取列类型
    pub fn column_type(&self, idx: impl QueryIdx) -> Result<ColumnType> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match column.column_type() {
            MyColumnType::MYSQL_TYPE_SHORT
            | MyColumnType::MYSQL_TYPE_LONG
            | MyColumnType::MYSQL_TYPE_LONGLONG => Ok(ColumnType::Int),
            MyColumnType::MYSQL_TYPE_FLOAT | MyColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(ColumnType::Float)
            }
            MyColumnType::MYSQL_TYPE_DECIMAL | MyColumnType::MYSQL_TYPE_NEWDECIMAL => {
                Ok(ColumnType::Decimal)
            }
            MyColumnType::MYSQL_TYPE_STRING
            | MyColumnType::MYSQL_TYPE_VAR_STRING
            | MyColumnType::MYSQL_TYPE_VARCHAR => Ok(ColumnType::String),
            MyColumnType::MYSQL_TYPE_DATETIME | MyColumnType::MYSQL_TYPE_TIMESTAMP => {
                Ok(ColumnType::DateTime)
            }
            MyColumnType::MYSQL_TYPE_DATE => Ok(ColumnType::Date),
            MyColumnType::MYSQL_TYPE_TIME => Ok(ColumnType::Time),
            MyColumnType::MYSQL_TYPE_TINY if column.column_length() == 1 => Ok(ColumnType::Bool),
            _ => Ok(ColumnType::Unknown),
        }
    }

    /// 获取指定列的数据
    pub fn try_get<T>(&self, idx: impl QueryIdx) -> Result<Option<T>>
    where
        T: FromValue,
    {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .get_opt::<T, usize>(idx)
            .transpose()
            .map_err(Error::from)
    }

    pub fn try_get_string(&self, idx: impl QueryIdx) -> Result<Option<String>> {
        self.try_get::<String>(idx)
    }

    pub fn try_get_i64(&self, idx: impl QueryIdx) -> Result<Option<i64>> {
        self.try_get::<i64>(idx)
    }

    pub fn try_get_i32(&self, idx: impl QueryIdx) -> Result<Option<i32>> {
        self.try_get::<i32>(idx)
    }

    pub fn try_get_i16(&self, idx: impl QueryIdx) -> Result<Option<i16>> {
        self.try_get::<i16>(idx)
    }

    pub fn try_get_u8(&self, idx: impl QueryIdx) -> Result<Option<u8>> {
        self.try_get::<u8>(idx)
    }

    pub fn try_get_f64(&self, idx: impl QueryIdx) -> Result<Option<f64>> {
        self.try_get::<f64>(idx)
    }

    pub fn try_get_f32(&self, idx: impl QueryIdx) -> Result<Option<f32>> {
        self.try_get::<f32>(idx)
    }

    pub fn try_get_bool(&self, idx: impl QueryIdx) -> Result<Option<bool>> {
        self.try_get::<bool>(idx)
    }

    pub fn try_get_decimal(&self, idx: impl QueryIdx) -> Result<Option<rust_decimal::Decimal>> {
        self.try_get::<rust_decimal::Decimal>(idx)
    }

    pub fn try_get_datetime(&self, idx: impl QueryIdx) -> Result<Option<chrono::NaiveDateTime>> {
        self.try_get::<chrono::NaiveDateTime>(idx)
    }

    pub fn try_get_date(&self, idx: impl QueryIdx) -> Result<Option<chrono::NaiveDate>> {
        self.try_get::<chrono::NaiveDate>(idx)
    }

    pub fn try_get_time(&self, idx: impl QueryIdx) -> Result<Option<chrono::NaiveTime>> {
        self.try_get::<chrono::NaiveTime>(idx)
    }

    pub fn try_get_uuid(&self, idx: impl QueryIdx) -> Result<Option<uuid::Uuid>> {
        let v = self.try_get::<Value>(idx)?;
        uuid_from_value(v)
    }
}

fn uuid_from_value(v: Option<Value>) -> Result<Option<uuid::Uuid>> {
    let Some(v) = v else {
        return Ok(None);
    };
    match v {
        Value::NULL => Ok(None),
        Value::Bytes(b) => {
            if b.len() == 16 {
                uuid::Uuid::from_slice(&b)
                    .map(Some)
                    .map_err(|e| Error::TypeConversion(e.to_string()))
            } else {
                let s = std::str::from_utf8(&b).map_err(|e| Error::TypeConversion(e.to_string()))?;
                uuid::Uuid::parse_str(s.trim())
                    .map(Some)
                    .map_err(|e| Error::TypeConversion(e.to_string()))
            }
        }
        other => Err(Error::TypeConversion(format!(
            "unsupported uuid value: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_from_bytes_16() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let got = uuid_from_value(Some(Value::Bytes(u.as_bytes().to_vec()))).unwrap();
        assert_eq!(got, Some(u));
    }

    #[test]
    fn uuid_from_string_bytes() {
        let s = "550e8400-e29b-41d4-a716-446655440000";
        let u = uuid::Uuid::parse_str(s).unwrap();
        let got = uuid_from_value(Some(Value::Bytes(s.as_bytes().to_vec()))).unwrap();
        assert_eq!(got, Some(u));
    }

    #[test]
    fn uuid_from_null() {
        let got = uuid_from_value(Some(Value::NULL)).unwrap();
        assert_eq!(got, None);
    }
}
