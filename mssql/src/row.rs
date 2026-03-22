//!
//! 数据行
//! 字段下标从0开始
//!

use crate::{Error, prelude::*};
use fmt::Display;
use std::fmt::{self, Debug, Formatter};
use tiberius::Uuid;

pub enum ColumnType {
    String,
    Int,
    Float,
    Bit,
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
    Bit(bool),
    Decimal(Decimal),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Uuid(Uuid),
}

impl Debug for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ColumnData::String(v) => Debug::fmt(v, f),
            ColumnData::Int(v) => Debug::fmt(v, f),
            ColumnData::Float(v) => Debug::fmt(v, f),
            ColumnData::Bit(v) => Debug::fmt(v, f),
            ColumnData::Decimal(v) => Debug::fmt(v, f),
            ColumnData::DateTime(v) => Debug::fmt(v, f),
            ColumnData::Date(v) => Debug::fmt(v, f),
            ColumnData::Time(v) => Debug::fmt(v, f),
            ColumnData::Uuid(v) => Debug::fmt(v, f),
        }
    }
}

impl Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ColumnData::String(v) => Display::fmt(v, f),
            ColumnData::Int(v) => Display::fmt(v, f),
            ColumnData::Float(v) => Display::fmt(v, f),
            ColumnData::Bit(v) => Display::fmt(v, f),
            ColumnData::Decimal(v) => Display::fmt(v, f),
            ColumnData::DateTime(v) => Display::fmt(&v.format("%Y-%m-%d %H:%M:%S"), f),
            ColumnData::Date(v) => Display::fmt(&v.format("%Y-%m-%d"), f),
            ColumnData::Time(v) => Display::fmt(&v.format("%H:%M:%S"), f),
            ColumnData::Uuid(v) => Display::fmt(v, f),
        }
    }
}

pub trait QueryIdx
where
    Self: Display,
{
    fn idx(&self, row: &Row) -> Option<u16>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &Row) -> Option<u16> {
        Some(*self as u16)
    }
}
impl QueryIdx for u16 {
    fn idx(&self, _row: &Row) -> Option<u16> {
        Some(*self)
    }
}
impl QueryIdx for i32 {
    fn idx(&self, _row: &Row) -> Option<u16> {
        Some(*self as u16)
    }
}
impl QueryIdx for &str {
    fn idx(&self, row: &Row) -> Option<u16> {
        row.column_index(self)
    }
}

pub struct Row(pub(crate) tiberius::Row);

impl Row {
    pub(crate) fn new(row: tiberius::Row) -> Row {
        Row(row)
    }
}

impl Row {
    #[inline]
    pub fn column_count(&self) -> u16 {
        self.0.columns().len() as u16
    }
    #[inline]
    pub fn column_index(&self, name: &str) -> Option<u16> {
        self.0.columns().iter().position(|c| name.eq_ignore_ascii_case(c.name())).map(|i| i as u16)
    }
    #[inline]
    pub fn column_name(&self, idx: u16) -> &str {
        self.0.columns()[idx as usize].name()
    }
    #[inline]
    pub fn column_exists(&self, name: &str) -> bool {
        self.column_index(name).is_some()
    }
    #[inline]
    pub fn column_type(&self, idx: impl QueryIdx) -> Result<ColumnType, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        match self.0.columns()[idx].column_type() {
            tiberius::ColumnType::Bit => Ok(ColumnType::Bit),
            tiberius::ColumnType::Int1
            | tiberius::ColumnType::Int2
            | tiberius::ColumnType::Int4
            | tiberius::ColumnType::Int8 => Ok(ColumnType::Int),
            tiberius::ColumnType::Float4 | tiberius::ColumnType::Float8 | tiberius::ColumnType::Floatn => {
                Ok(ColumnType::Float)
            },
            tiberius::ColumnType::Decimaln
            | tiberius::ColumnType::Numericn
            | tiberius::ColumnType::Money
            | tiberius::ColumnType::Money4 => Ok(ColumnType::Decimal),
            tiberius::ColumnType::Datetime
            | tiberius::ColumnType::Datetime2
            | tiberius::ColumnType::Datetime4
            | tiberius::ColumnType::Datetimen
            | tiberius::ColumnType::DatetimeOffsetn => Ok(ColumnType::DateTime),
            tiberius::ColumnType::Daten => Ok(ColumnType::Date),
            tiberius::ColumnType::Timen => Ok(ColumnType::Time),
            tiberius::ColumnType::Guid => Ok(ColumnType::Uuid),
            _ => Ok(ColumnType::Unknown),
        }
    }

    #[inline]
    pub fn try_get_str(&self, idx: impl QueryIdx) -> Result<Option<&str>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get::<&str, _>(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_u8(&self, idx: impl QueryIdx) -> Result<Option<u8>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        match self.0.columns()[idx].column_type() {
            tiberius::ColumnType::Int1 => self.0.try_get(idx).map_err(Error::FetchError),
            tiberius::ColumnType::Int2 => {
                let v: Option<i16> = self.0.try_get(idx).map_err(Error::FetchError)?;
                v.map(u8::try_from)
                    .transpose()
                    .map_err(|_| Error::custom("u8 overflow"))
            }
            tiberius::ColumnType::Int4 => {
                let v: Option<i32> = self.0.try_get(idx).map_err(Error::FetchError)?;
                v.map(u8::try_from)
                    .transpose()
                    .map_err(|_| Error::custom("u8 overflow"))
            }
            tiberius::ColumnType::Int8 => {
                let v: Option<i64> = self.0.try_get(idx).map_err(Error::FetchError)?;
                v.map(u8::try_from)
                    .transpose()
                    .map_err(|_| Error::custom("u8 overflow"))
            }
            tiberius::ColumnType::Intn => {
                if let Ok(v) = self.0.try_get::<u8, _>(idx) {
                    return Ok(v);
                }
                if let Ok(v) = self.0.try_get::<i16, _>(idx) {
                    return v
                        .map(u8::try_from)
                        .transpose()
                        .map_err(|_| Error::custom("u8 overflow"));
                }
                if let Ok(v) = self.0.try_get::<i32, _>(idx) {
                    return v
                        .map(u8::try_from)
                        .transpose()
                        .map_err(|_| Error::custom("u8 overflow"));
                }
                if let Ok(v) = self.0.try_get::<i64, _>(idx) {
                    return v
                        .map(u8::try_from)
                        .transpose()
                        .map_err(|_| Error::custom("u8 overflow"));
                }
                Err(Error::custom("cannot interpret Intn as an u8 value"))
            }
            other => Err(Error::custom(format!(
                "cannot interpret {other:?} as an u8 value"
            ))),
        }
    }
    #[inline]
    pub fn try_get_i16(&self, idx: impl QueryIdx) -> Result<Option<i16>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        match self.0.columns()[idx].column_type() {
            tiberius::ColumnType::Int1 => {
                let v: Option<u8> = self.0.try_get(idx).map_err(Error::FetchError)?;
                Ok(v.map(|v| v as i16))
            }
            tiberius::ColumnType::Int2 => self.0.try_get(idx).map_err(Error::FetchError),
            tiberius::ColumnType::Int4 => {
                let v: Option<i32> = self.0.try_get(idx).map_err(Error::FetchError)?;
                v.map(i16::try_from)
                    .transpose()
                    .map_err(|_| Error::custom("i16 overflow"))
            }
            tiberius::ColumnType::Int8 => {
                let v: Option<i64> = self.0.try_get(idx).map_err(Error::FetchError)?;
                v.map(i16::try_from)
                    .transpose()
                    .map_err(|_| Error::custom("i16 overflow"))
            }
            tiberius::ColumnType::Intn => {
                if let Ok(v) = self.0.try_get::<i16, _>(idx) {
                    return Ok(v);
                }
                if let Ok(v) = self.0.try_get::<u8, _>(idx) {
                    return Ok(v.map(|v| v as i16));
                }
                if let Ok(v) = self.0.try_get::<i32, _>(idx) {
                    return v
                        .map(i16::try_from)
                        .transpose()
                        .map_err(|_| Error::custom("i16 overflow"));
                }
                if let Ok(v) = self.0.try_get::<i64, _>(idx) {
                    return v
                        .map(i16::try_from)
                        .transpose()
                        .map_err(|_| Error::custom("i16 overflow"));
                }
                Err(Error::custom("cannot interpret Intn as an i16 value"))
            }
            other => Err(Error::custom(format!(
                "cannot interpret {other:?} as an i16 value"
            ))),
        }
    }
    #[inline]
    pub fn try_get_i32(&self, idx: impl QueryIdx) -> Result<Option<i32>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_i64(&self, idx: impl QueryIdx) -> Result<Option<i64>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        match self.0.columns()[idx].column_type() {
            tiberius::ColumnType::Int1 => {
                let v: Option<u8> = self.0.try_get(idx).map_err(Error::FetchError)?;
                Ok(v.map(|v| v as i64))
            }
            tiberius::ColumnType::Int2 => {
                let v: Option<i16> = self.0.try_get(idx).map_err(Error::FetchError)?;
                Ok(v.map(|v| v as i64))
            }
            tiberius::ColumnType::Int4 => {
                let v: Option<i32> = self.0.try_get(idx).map_err(Error::FetchError)?;
                Ok(v.map(|v| v as i64))
            }
            tiberius::ColumnType::Int8 => self.0.try_get(idx).map_err(Error::FetchError),
            tiberius::ColumnType::Intn => {
                if let Ok(v) = self.0.try_get::<i64, _>(idx) {
                    return Ok(v);
                }
                if let Ok(v) = self.0.try_get::<i32, _>(idx) {
                    return Ok(v.map(|v| v as i64));
                }
                if let Ok(v) = self.0.try_get::<i16, _>(idx) {
                    return Ok(v.map(|v| v as i64));
                }
                if let Ok(v) = self.0.try_get::<u8, _>(idx) {
                    return Ok(v.map(|v| v as i64));
                }
                Err(Error::custom("cannot interpret Intn as an i64 value"))
            }
            other => Err(Error::custom(format!(
                "cannot interpret {other:?} as an i64 value"
            ))),
        }
    }
    #[inline]
    pub fn try_get_f32(&self, idx: impl QueryIdx) -> Result<Option<f32>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_f64(&self, idx: impl QueryIdx) -> Result<Option<f64>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_dec(&self, idx: impl QueryIdx) -> Result<Option<Decimal>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_datetime(&self, idx: impl QueryIdx) -> Result<Option<NaiveDateTime>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_date(&self, idx: impl QueryIdx) -> Result<Option<NaiveDate>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_time(&self, idx: impl QueryIdx) -> Result<Option<NaiveTime>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    #[inline]
    pub fn try_get_uuid(&self, idx: impl QueryIdx) -> Result<Option<Uuid>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        self.0.try_get(idx).map_err(Error::FetchError)
    }
    pub fn try_get_any(&self, idx: impl QueryIdx) -> Result<Option<ColumnData>, Error> {
        let idx = idx.idx(self).ok_or(Error::ColumnNotExists)? as usize;
        match self.0.columns()[idx].column_type() {
            tiberius::ColumnType::Bit => {
                self.0.try_get::<bool, _>(idx).map(|v| v.map(ColumnData::Bit)).map_err(Error::FetchError)
            },
            tiberius::ColumnType::Int1 => self
                .0
                .try_get::<u8, _>(idx)
                .map(|v| v.map(|v| ColumnData::Int(v as i64)))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Int2 => self
                .0
                .try_get::<i16, _>(idx)
                .map(|v| v.map(|v| ColumnData::Int(v as i64)))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Int4 => self
                .0
                .try_get::<i32, _>(idx)
                .map(|v| v.map(|v| ColumnData::Int(v as i64)))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Int8 => {
                self.0.try_get::<i64, _>(idx).map(|v| v.map(ColumnData::Int)).map_err(Error::FetchError)
            },
            tiberius::ColumnType::Intn => self
                .0
                .try_get::<i64, _>(idx)
                .or_else(|_| self.0.try_get::<i32, _>(idx).map(|v| v.map(|v| v as i64)))
                .map(|v| v.map(ColumnData::Int))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Float4 => self
                .0
                .try_get::<f32, _>(idx)
                .map(|v| v.map(|v| ColumnData::Float(v as f64)))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Float8 => {
                self.0.try_get::<f64, _>(idx).map(|v| v.map(ColumnData::Float)).map_err(Error::FetchError)
            },
            tiberius::ColumnType::Floatn => self
                .0
                .try_get::<f64, _>(idx)
                .or_else(|_| self.0.try_get::<f32, _>(idx).map(|v| v.map(|v| v as f64)))
                .map(|v| v.map(ColumnData::Float))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Decimaln
            | tiberius::ColumnType::Numericn
            | tiberius::ColumnType::Money
            | tiberius::ColumnType::Money4 => self
                .0
                .try_get::<Decimal, _>(idx)
                .map(|v| v.map(ColumnData::Decimal))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Datetime
            | tiberius::ColumnType::Datetime2
            | tiberius::ColumnType::Datetime4
            | tiberius::ColumnType::Datetimen
            | tiberius::ColumnType::DatetimeOffsetn => self
                .0
                .try_get::<NaiveDateTime, _>(idx)
                .map(|v| v.map(ColumnData::DateTime))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Daten => self
                .0
                .try_get::<NaiveDate, _>(idx)
                .map(|v| v.map(ColumnData::Date))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Timen => self
                .0
                .try_get::<NaiveTime, _>(idx)
                .map(|v| v.map(ColumnData::Time))
                .map_err(Error::FetchError),
            tiberius::ColumnType::Guid => {
                self.0.try_get::<Uuid, _>(idx).map(|v| v.map(ColumnData::Uuid)).map_err(Error::FetchError)
            },
            tiberius::ColumnType::Null => Ok(None),
            _ => self
                .0
                .try_get::<&str, _>(idx)
                .map(|v| v.map(|v| ColumnData::String(v.to_owned())))
                .map_err(Error::FetchError),
        }
    }

    #[inline]
    pub fn get_str(&self, idx: impl QueryIdx) -> Option<&str> {
        self.try_get_str(idx).unwrap()
    }
    #[inline]
    pub fn get_u8(&self, idx: impl QueryIdx) -> Option<u8> {
        self.try_get_u8(idx).unwrap()
    }
    #[inline]
    pub fn get_i16(&self, idx: impl QueryIdx) -> Option<i16> {
        self.try_get_i16(idx).unwrap()
    }
    #[inline]
    pub fn get_i32(&self, idx: impl QueryIdx) -> Option<i32> {
        self.try_get_i32(idx).unwrap()
    }
    #[inline]
    pub fn get_i64(&self, idx: impl QueryIdx) -> Option<i64> {
        self.try_get_i64(idx).unwrap()
    }
    #[inline]
    pub fn get_f32(&self, idx: impl QueryIdx) -> Option<f32> {
        self.try_get_f32(idx).unwrap()
    }
    #[inline]
    pub fn get_f64(&self, idx: impl QueryIdx) -> Option<f64> {
        self.try_get_f64(idx).unwrap()
    }
    #[inline]
    pub fn get_dec(&self, idx: impl QueryIdx) -> Option<Decimal> {
        self.try_get_dec(idx).unwrap()
    }
    #[inline]
    pub fn get_datetime(&self, idx: impl QueryIdx) -> Option<NaiveDateTime> {
        self.try_get_datetime(idx).unwrap()
    }
    #[inline]
    pub fn get_date(&self, idx: impl QueryIdx) -> Option<NaiveDate> {
        self.try_get_date(idx).unwrap()
    }
    #[inline]
    pub fn get_time(&self, idx: impl QueryIdx) -> Option<NaiveTime> {
        self.try_get_time(idx).unwrap()
    }
    #[inline]
    pub fn get_uuid(&self, idx: impl QueryIdx) -> Option<Uuid> {
        self.try_get_uuid(idx).unwrap()
    }
    #[inline]
    pub fn get_any(&self, idx: impl QueryIdx) -> Option<ColumnData> {
        self.try_get_any(idx).unwrap()
    }
}
