//!
//! 数据行
//! 字段下标从0开始
//!

use crate::{prelude::*, Error};
use std::fmt::{self, Debug, Formatter};
use tokio_postgres::types::Type;

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
    Decimal(Decimal),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
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
#[repr(transparent)]
pub struct Row(pub(crate) tokio_postgres::Row);

impl Row {
    pub(crate) fn new(row: tokio_postgres::Row) -> Self {
        Row(row)
    }

    /// 获取列数
    pub fn column_count(&self) -> usize {
        self.0.len()
    }

    /// 获取列名称
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.0.columns().get(idx).map(|c| c.name())
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 获取列类型
    pub fn column_type(&self, idx: impl QueryIdx) -> Result<ColumnType, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        match self.0.columns()[idx].type_() {
            &Type::BOOL => Ok(ColumnType::Bool),
            &Type::INT2 | &Type::INT4 | &Type::INT8 => Ok(ColumnType::Int),
            &Type::FLOAT4 | &Type::FLOAT8 => Ok(ColumnType::Float),
            &Type::TEXT | &Type::VARCHAR | &Type::CHAR | &Type::NAME => Ok(ColumnType::String),
            &Type::NUMERIC => Ok(ColumnType::Decimal),
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Ok(ColumnType::DateTime),
            &Type::DATE => Ok(ColumnType::Date),
            &Type::TIME | &Type::TIMETZ => Ok(ColumnType::Time),
            &Type::UUID => Ok(ColumnType::Uuid),
            _ => Ok(ColumnType::Unknown),
        }
    }

    /// 获取指定索引的数据
    pub fn try_get_any(&self, idx: impl QueryIdx) -> Result<Option<ColumnData>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match column.type_() {
            &Type::BOOL => self
                .0
                .try_get::<usize, Option<bool>>(idx)
                .map(|v| v.map(ColumnData::Bool))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::INT2 => self
                .0
                .try_get::<usize, Option<i16>>(idx)
                .map(|v| v.map(|v| ColumnData::Int(v as i64)))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::INT4 => self
                .0
                .try_get::<usize, Option<i32>>(idx)
                .map(|v| v.map(|v| ColumnData::Int(v as i64)))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::INT8 => self
                .0
                .try_get::<usize, Option<i64>>(idx)
                .map(|v| v.map(ColumnData::Int))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::FLOAT4 => self
                .0
                .try_get::<usize, Option<f32>>(idx)
                .map(|v| v.map(|v| ColumnData::Float(v as f64)))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::FLOAT8 => self
                .0
                .try_get::<usize, Option<f64>>(idx)
                .map(|v| v.map(ColumnData::Float))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::NUMERIC => self
                .0
                .try_get::<usize, Option<Decimal>>(idx)
                .map(|v| v.map(ColumnData::Decimal))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::TIMESTAMP => self
                .0
                .try_get::<usize, Option<NaiveDateTime>>(idx)
                .map(|v| v.map(ColumnData::DateTime))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::TIMESTAMPTZ => self
                .0
                .try_get::<usize, Option<DateTime<Utc>>>(idx)
                .map(|v| v.map(|v| ColumnData::DateTime(v.naive_utc())))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::DATE => self
                .0
                .try_get::<usize, Option<NaiveDate>>(idx)
                .map(|v| v.map(ColumnData::Date))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::TIME => self
                .0
                .try_get::<usize, Option<NaiveTime>>(idx)
                .map(|v| v.map(ColumnData::Time))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::UUID => self
                .0
                .try_get::<usize, Option<uuid::Uuid>>(idx)
                .map(|v| v.map(ColumnData::Uuid))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            &Type::VARCHAR | &Type::TEXT | &Type::BPCHAR => self
                .0
                .try_get::<usize, Option<String>>(idx)
                .map(|v| v.map(ColumnData::String))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "unsupported column type: {}",
                column.type_()
            ))),
        }
    }

    /// 获取字符串数据
    pub fn try_get_string(&self, idx: impl QueryIdx) -> Result<Option<String>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<String>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }

    /// 获取整数数据
    pub fn try_get_i64(&self, idx: impl QueryIdx) -> Result<Option<i64>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::INT2 => self
                .0
                .try_get::<usize, Option<i16>>(idx)
                .map(|v| v.map(|v| v as i64))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::INT4 => self
                .0
                .try_get::<usize, Option<i32>>(idx)
                .map(|v| v.map(|v| v as i64))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::INT8 => self
                .0
                .try_get::<usize, Option<i64>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not an integer type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取i32数据
    pub fn try_get_i32(&self, idx: impl QueryIdx) -> Result<Option<i32>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::INT2 => self
                .0
                .try_get::<usize, Option<i16>>(idx)
                .map(|v| v.map(|v| v as i32))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::INT4 => self
                .0
                .try_get::<usize, Option<i32>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::INT8 => self
                .0
                .try_get::<usize, Option<i64>>(idx)
                .map(|v| v.map(|v| v as i32))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not an i32 compatible type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取i16数据
    pub fn try_get_i16(&self, idx: impl QueryIdx) -> Result<Option<i16>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::INT2 => self
                .0
                .try_get::<usize, Option<i16>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not an i16 type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取u8数据 (PostgreSQL无u8，从INT2转换)
    pub fn try_get_u8(&self, idx: impl QueryIdx) -> Result<Option<u8>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::INT2 => self
                .0
                .try_get::<usize, Option<i16>>(idx)
                .map(|v| v.map(|v| v as u8))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not a u8 compatible type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取浮点数数据
    pub fn try_get_f64(&self, idx: impl QueryIdx) -> Result<Option<f64>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::FLOAT4 => self
                .0
                .try_get::<usize, Option<f32>>(idx)
                .map(|v| v.map(|v| v as f64))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::FLOAT8 => self
                .0
                .try_get::<usize, Option<f64>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not a float type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取f32数据
    pub fn try_get_f32(&self, idx: impl QueryIdx) -> Result<Option<f32>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::FLOAT4 => self
                .0
                .try_get::<usize, Option<f32>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not an f32 type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取Bool数据
    pub fn try_get_bool(&self, idx: impl QueryIdx) -> Result<Option<bool>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<bool>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }

    /// 获取Decimal数据
    pub fn try_get_decimal(&self, idx: impl QueryIdx) -> Result<Option<Decimal>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<Decimal>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }

    /// 获取DateTime数据
    pub fn try_get_datetime(&self, idx: impl QueryIdx) -> Result<Option<NaiveDateTime>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        let column = &self.0.columns()[idx];
        match *column.type_() {
            Type::TIMESTAMP => self
                .0
                .try_get::<usize, Option<NaiveDateTime>>(idx)
                .map_err(|e| Error::TypeConversion(e.to_string())),
            Type::TIMESTAMPTZ => self
                .0
                .try_get::<usize, Option<DateTime<Utc>>>(idx)
                .map(|v| v.map(|v| v.naive_utc()))
                .map_err(|e| Error::TypeConversion(e.to_string())),
            _ => Err(Error::TypeConversion(format!(
                "column {} is not a datetime type: {}",
                column.name(),
                column.type_()
            ))),
        }
    }

    /// 获取Date数据
    pub fn try_get_date(&self, idx: impl QueryIdx) -> Result<Option<NaiveDate>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<NaiveDate>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }

    /// 获取Time数据
    pub fn try_get_time(&self, idx: impl QueryIdx) -> Result<Option<NaiveTime>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<NaiveTime>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }

    /// 获取Uuid数据
    pub fn try_get_uuid(&self, idx: impl QueryIdx) -> Result<Option<uuid::Uuid>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)?;
        self.0
            .try_get::<usize, Option<uuid::Uuid>>(idx)
            .map_err(|e| Error::TypeConversion(e.to_string()))
    }
}

/// 查询索引
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
        row.0.columns().iter().position(|c| c.name() == *self)
    }
}

impl QueryIdx for String {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.0.columns().iter().position(|c| c.name() == self)
    }
}
