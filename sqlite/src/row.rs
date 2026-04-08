//!
//! 数据行
//! 字段下标从0开始
//!

use crate::Error;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use std::fmt::{self, Debug, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Blob,
    Null,
    Unknown,
}

pub enum ColumnData {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Decimal(Decimal),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Uuid(uuid::Uuid),
    Blob(Vec<u8>),
    Null,
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
            ColumnData::Blob(v) => write!(f, "Blob({} bytes)", v.len()),
            ColumnData::Null => write!(f, "Null"),
        }
    }
}

/// 行数据
pub struct Row {
    pub(crate) columns: Vec<String>,
    pub(crate) data: Vec<ColumnData>,
}

impl Row {
    /// 获取列数
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// 获取列名称
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 获取指定索引的数据
    pub fn try_get_any(&self, idx: impl QueryIdx) -> Result<Option<ColumnData>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        Ok(self.data.get(idx).cloned().and_then(|d| match d {
            ColumnData::Null => None,
            _ => Some(d),
        }))
    }

    pub fn try_get_string(&self, idx: impl QueryIdx) -> Result<Option<String>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::String(s)) => Ok(Some(s)),
            Some(ColumnData::Int(i)) => Ok(Some(i.to_string())),
            Some(ColumnData::Float(f)) => Ok(Some(f.to_string())),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a string".to_string())),
        }
    }

    pub fn try_get_i64(&self, idx: impl QueryIdx) -> Result<Option<i64>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Int(i)) => Ok(Some(i)),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not an integer".to_string())),
        }
    }

    pub fn try_get_f64(&self, idx: impl QueryIdx) -> Result<Option<f64>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Float(f)) => Ok(Some(f)),
            Some(ColumnData::Int(i)) => Ok(Some(i as f64)),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a float".to_string())),
        }
    }

    pub fn try_get_bool(&self, idx: impl QueryIdx) -> Result<Option<bool>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Bool(b)) => Ok(Some(b)),
            Some(ColumnData::Int(i)) => Ok(Some(i != 0)),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a bool".to_string())),
        }
    }

    pub fn try_get_decimal(&self, idx: impl QueryIdx) -> Result<Option<Decimal>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Decimal(d)) => Ok(Some(d)),
            Some(ColumnData::String(s)) => s
                .parse::<Decimal>()
                .map(Some)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Some(ColumnData::Float(f)) => Ok(Some(Decimal::from_f64_retain(f).unwrap_or_default())),
            Some(ColumnData::Int(i)) => Ok(Some(Decimal::from(i))),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a decimal".to_string())),
        }
    }

    pub fn try_get_datetime(&self, idx: impl QueryIdx) -> Result<Option<NaiveDateTime>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::DateTime(dt)) => Ok(Some(dt)),
            Some(ColumnData::String(s)) => {
                // Try common formats
                NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                    .map(Some)
                    .map_err(|e| Error::TypeConversion(e.to_string()))
            }
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a datetime".to_string())),
        }
    }

    pub fn try_get_uuid(&self, idx: impl QueryIdx) -> Result<Option<uuid::Uuid>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Uuid(u)) => Ok(Some(u)),
            Some(ColumnData::String(s)) => s
                .parse::<uuid::Uuid>()
                .map(Some)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Some(ColumnData::Blob(b)) => uuid::Uuid::from_slice(&b)
                .map(Some)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a uuid".to_string())),
        }
    }

    pub fn try_get_blob(&self, idx: impl QueryIdx) -> Result<Option<Vec<u8>>, Error> {
        match self.try_get_any(idx)? {
            Some(ColumnData::Blob(b)) => Ok(Some(b)),
            Some(ColumnData::Null) | None => Ok(None),
            Some(_) => Err(Error::TypeConversion("Not a blob".to_string())),
        }
    }
}

impl Clone for ColumnData {
    fn clone(&self) -> Self {
        match self {
            ColumnData::String(v) => ColumnData::String(v.clone()),
            ColumnData::Int(v) => ColumnData::Int(*v),
            ColumnData::Float(v) => ColumnData::Float(*v),
            ColumnData::Bool(v) => ColumnData::Bool(*v),
            ColumnData::Decimal(v) => ColumnData::Decimal(*v),
            ColumnData::DateTime(v) => ColumnData::DateTime(*v),
            ColumnData::Date(v) => ColumnData::Date(*v),
            ColumnData::Time(v) => ColumnData::Time(*v),
            ColumnData::Uuid(v) => ColumnData::Uuid(*v),
            ColumnData::Blob(v) => ColumnData::Blob(v.clone()),
            ColumnData::Null => ColumnData::Null,
        }
    }
}

pub trait QueryIdx {
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, row: &Row) -> Option<usize> {
        if *self < row.column_count() {
            Some(*self)
        } else {
            None
        }
    }
}

impl QueryIdx for &str {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c == *self)
    }
}

impl QueryIdx for String {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c == self)
    }
}
