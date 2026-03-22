use crate::row::Row;
use ::serde::{
    de::{
        value::Error as SerdeError, DeserializeSeed, Deserializer, Error as SerdeErrorTrait, MapAccess, SeqAccess, Visitor
    }, forward_to_deserialize_any
};
use std::{borrow::Cow, vec::IntoIter};
use tiberius::{Column, ColumnData, FromSql};

/// 多行结果集反序列化
pub(crate) struct RowCollection(Vec<Row>);

impl RowCollection {
    pub(crate) fn new(rows: Vec<Row>) -> RowCollection { RowCollection(rows) }
}

impl<'de> Deserializer<'de> for RowCollection {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        self.deserialize_seq(visitor)
    }

    /// 支持反序列化为`Array`
    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        struct RowsAccessor {
            rows: IntoIter<Row>,
            len: usize
        }

        impl<'de> SeqAccess<'de> for RowsAccessor {
            type Error = SerdeError;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: DeserializeSeed<'de>
            {
                match self.rows.next() {
                    Some(row) => seed.deserialize(row).map(Some),
                    None => Ok(None),
                }
            }

            fn size_hint(&self) -> Option<usize> { Some(self.len) }
        }

        visitor.visit_seq(RowsAccessor {
            len: self.0.len(),
            rows: self.0.into_iter()
        })
    }

    fn is_human_readable(&self) -> bool { false }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct struct map enum identifier ignored_any
    }
}

/// 单行反序列化
pub(crate) struct RowOptional(Option<Row>);

/// 支持反序列化首字段
///
/// 为方便提取单字段结果集，直接使用简单目标类型提取值
///
/// # Notice
///
/// 结果集仅包含一个字段时可用
///
/// # Examples
///
/// ```rust
/// # use mssql::prelude::*;
/// # async fn _example() -> Result<(), mssql::Error> {
/// # let conn: mssql::Connection = unimplemented!();
/// let result: Vec<String> =
///     conn.query_collect("SELECT 'aaaa' AS col1 UNION SELECT 'bbb' AS col1").await?;
/// println!("result: {:?}", result);
/// let result: String = conn.query_first("SELECT 'aaaa' AS col1").await?;
/// println!("result: {}", result);
/// let result: Option<String> = conn.query_first("SELECT 'aaaa' AS col1").await?;
/// println!("result: {:?}", result);
/// # Ok(())
/// # }
/// ```
macro_rules! forward_to_row_first_column{
    ($($func:ident)*) => {
        $(
            forward_to_row_first_column_impl!{ $func }
        )*
    };
}

#[macro_export(local_inner_macros)]
macro_rules! forward_to_row_first_column_impl {
    (deserialize_option) => {
        #[inline]
        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>
        {
            match self.0 {
                Some(row) => {
                    if row.column_count() == 1 {
                        row.deserialize_option(visitor)
                    } else {
                        visitor.visit_some(row)
                    }
                },
                None => visitor.visit_none()
            }
        }
    };
    ($func:ident) => {
        #[inline]
        fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>
        {
            match self.0 {
                Some(row) => {
                    if row.column_count() == 1 {
                        row.$func(visitor)
                    } else {
                        row.deserialize_any(visitor)
                    }
                },
                None => visitor.visit_none()
            }
        }
    };
}

impl RowOptional {
    pub(crate) fn new(row: Option<Row>) -> RowOptional { RowOptional(row) }
}

impl<'de> Deserializer<'de> for RowOptional {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => row.deserialize_any(visitor),
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_unit_struct(name, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            },
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_newtype_struct(name, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            },
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_enum(name, variants, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            },
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => row.deserialize_seq(visitor),
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => row.deserialize_tuple(len, visitor),
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => row.deserialize_tuple_struct(name, len, visitor),
            None => visitor.visit_none()
        }
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            Some(row) => row.deserialize_struct(name, fields, visitor),
            None => visitor.visit_none()
        }
    }

    fn is_human_readable(&self) -> bool { false }

    forward_to_row_first_column! {
        deserialize_option
        deserialize_bool
        deserialize_i8
        deserialize_i16
        deserialize_i32
        deserialize_i64
        deserialize_i128
        deserialize_u8
        deserialize_u16
        deserialize_u32
        deserialize_u64
        deserialize_f32
        deserialize_f64
        deserialize_char
        deserialize_str
        deserialize_string
        deserialize_bytes
        deserialize_byte_buf
        deserialize_unit
        deserialize_identifier
    }

    forward_to_deserialize_any! {
        map ignored_any
    }
}

/// 透传给首字段
macro_rules! forward_to_first_column{
    ($($func:ident)*) => {
        $(
            #[inline]
            fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                if let Some(col) = self.0.into_iter().next() {
                    ColumnAccessor(col).$func(visitor)
                } else {
                    visitor.visit_none()
                }
            }
        )*
    }
}

/// 使`mssql::Row`类型支持反序列化
impl<'de> Deserializer<'de> for Row {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        visitor.visit_map(RowAccessor::new(self))
    }

    /// 支持反序列化为`Array`
    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    /// 支持反序列化为`Tuple`
    #[inline]
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    /// 支持反序列化为`TupleStruct`
    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    fn is_human_readable(&self) -> bool { false }

    forward_to_first_column! {
        deserialize_option
        deserialize_bool
        deserialize_i8
        deserialize_i16
        deserialize_i32
        deserialize_i64
        deserialize_i128
        deserialize_u8
        deserialize_u16
        deserialize_u32
        deserialize_u64
        deserialize_f32
        deserialize_f64
        deserialize_char
        deserialize_str
        deserialize_string
        deserialize_bytes
        deserialize_byte_buf
        deserialize_unit
        deserialize_identifier
    }

    forward_to_deserialize_any! {
        unit_struct newtype_struct
        struct map enum ignored_any
    }
}

/// 字段值访问器
struct ColumnAccessor(ColumnData<'static>);

macro_rules! forward_to_deserialize_integer {
    ($($func:ident -> $vis:ident)*) => {
        $(
            #[inline]
            fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                match self.0 {
                    ColumnData::I64(Some(v)) => visitor.$vis(v as _),
                    ColumnData::I32(Some(v)) => visitor.$vis(v as _),
                    ColumnData::I16(Some(v)) => visitor.$vis(v as _),
                    ColumnData::U8(Some(v)) => visitor.$vis(v as _),
                    ColumnData::Numeric(Some(v)) => visitor.$vis(v.int_part() as _),
                    ColumnData::F64(Some(v)) => visitor.$vis(v as _),
                    ColumnData::F32(Some(v)) => visitor.$vis(v as _),
                    ColumnData::Bit(Some(v)) => {
                        visitor.$vis(if v {
                            1
                        } else {
                            0
                        })
                    },
                    _ => self.deserialize_any(visitor)
                }
            }
        )*
    }
}

macro_rules! forward_to_deserialize_float {
    ($($func:ident -> $vis:ident)*) => {
        $(
            #[inline]
            fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                match self.0 {
                    ColumnData::Numeric(Some(v)) => visitor.$vis(f64::from(v) as _),
                    ColumnData::F64(Some(v)) => visitor.$vis(v as _),
                    ColumnData::F32(Some(v)) => visitor.$vis(v as _),
                    ColumnData::I64(Some(v)) => visitor.$vis(v as _),
                    ColumnData::I32(Some(v)) => visitor.$vis(v as _),
                    ColumnData::I16(Some(v)) => visitor.$vis(v as _),
                    ColumnData::U8(Some(v)) => visitor.$vis(v as _),
                    ColumnData::Bit(Some(v)) => {
                        visitor.$vis(if v {
                            1.
                        } else {
                            0.
                        })
                    },
                    _ => self.deserialize_any(visitor)
                }
            }
        )*
    }
}

impl<'de> Deserializer<'de> for ColumnAccessor {
    type Error = SerdeError;

    #[inline]
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::Bit(Some(_)) |
            ColumnData::U8(Some(_)) |
            ColumnData::I16(Some(_)) |
            ColumnData::I32(Some(_)) |
            ColumnData::I64(Some(_)) |
            ColumnData::F32(Some(_)) |
            ColumnData::F64(Some(_)) |
            ColumnData::Guid(Some(_)) |
            ColumnData::String(Some(_)) |
            ColumnData::Binary(Some(_)) |
            ColumnData::DateTime(Some(_)) |
            ColumnData::SmallDateTime(Some(_)) |
            ColumnData::DateTime2(Some(_)) |
            ColumnData::Date(Some(_)) |
            ColumnData::Time(Some(_)) |
            ColumnData::DateTimeOffset(Some(_)) |
            ColumnData::Numeric(Some(_)) |
            ColumnData::Xml(Some(_)) => visitor.visit_some(self),
            _ => {
                // None/null
                visitor.visit_none()
            }
        }
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::Bit(Some(v)) => visitor.visit_bool(v),
            ColumnData::U8(Some(v)) => visitor.visit_u8(v),
            ColumnData::I16(Some(v)) => visitor.visit_i16(v),
            ColumnData::I32(Some(v)) => visitor.visit_i32(v),
            ColumnData::I64(Some(v)) => visitor.visit_i64(v),
            ColumnData::F32(Some(v)) => visitor.visit_f32(v),
            ColumnData::F64(Some(v)) => visitor.visit_f64(v),
            ColumnData::Guid(Some(v)) => visitor.visit_string(v.to_string()),
            ColumnData::String(Some(Cow::Borrowed(v))) => visitor.visit_str(v),
            ColumnData::String(Some(Cow::Owned(v))) => visitor.visit_string(v),
            ColumnData::Binary(Some(Cow::Borrowed(v))) => visitor.visit_bytes(v),
            ColumnData::Binary(Some(Cow::Owned(v))) => visitor.visit_byte_buf(v),
            ColumnData::DateTime(Some(_)) |
            ColumnData::DateTime2(Some(_)) |
            ColumnData::SmallDateTime(Some(_)) => {
                if let Some(v) = chrono::NaiveDateTime::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Date(Some(_)) => {
                if let Some(v) = chrono::NaiveDate::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Time(Some(_)) => {
                if let Some(v) = chrono::NaiveTime::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::DateTimeOffset(Some(_)) => {
                if let Some(v) =
                    chrono::DateTime::<chrono::Utc>::from_sql(&self.0).map_err(Self::Error::custom)?
                {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Numeric(Some(_)) => {
                if let Some(v) = rust_decimal::Decimal::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Xml(Some(Cow::Borrowed(v))) => visitor.visit_str(v.as_ref()),
            ColumnData::Xml(Some(Cow::Owned(v))) => visitor.visit_string(v.into_string()),
            _ => {
                // None/null
                visitor.visit_none()
            }
        }
    }

    #[inline]
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::Bit(Some(v)) => visitor.visit_bool(v),
            //NOTE 字符串Y/N转义为bool
            ColumnData::String(Some(v)) if v == "Y" || v == "N" => visitor.visit_bool(v == "Y"),
            _ => self.deserialize_any(visitor)
        }
    }

    #[inline]
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::I64(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::I32(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::I16(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::U8(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::Numeric(Some(v)) => visitor.visit_i64(v.int_part() as _),
            ColumnData::F64(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::F32(Some(v)) => visitor.visit_i64(v as _),
            ColumnData::Bit(Some(v)) => {
                visitor.visit_i64(if v {
                    1
                } else {
                    0
                })
            },
            //NOTE 特殊处理NaiveDateTime，支持从时间戳反序列化
            ColumnData::DateTime(Some(_)) |
            ColumnData::DateTime2(Some(_)) |
            ColumnData::SmallDateTime(Some(_)) => {
                if let Some(v) = chrono::NaiveDateTime::from_sql(&self.0).map_err(Self::Error::custom)? {
                    let nanos = v
                        .and_utc()
                        .timestamp_nanos_opt()
                        .ok_or_else(|| Self::Error::custom("timestamp nanos overflow"))?;
                    visitor.visit_i64(nanos)
                } else {
                    visitor.visit_none()
                }
            },
            _ => self.deserialize_any(visitor)
        }
    }

    #[inline]
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::String(Some(v)) => visitor.visit_str(&v),
            //NOTE 特殊处理Decimal，支持从字符串反序列化
            ColumnData::Numeric(Some(_)) => {
                if let Some(v) = rust_decimal::Decimal::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            //NOTE 特殊处理Uuid，支持从字符串反序列化
            ColumnData::Guid(Some(v)) => visitor.visit_string(v.to_string()),
            //NOTE 特殊处理NaiveDateTime/NaiveDate/NaiveTime，支持从字符串反序列化
            ColumnData::DateTime(Some(_)) |
            ColumnData::DateTime2(Some(_)) |
            ColumnData::SmallDateTime(Some(_)) => {
                if let Some(v) = chrono::NaiveDateTime::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Date(Some(_)) => {
                if let Some(v) = chrono::NaiveDate::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::Time(Some(_)) => {
                if let Some(v) = chrono::NaiveTime::from_sql(&self.0).map_err(Self::Error::custom)? {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            ColumnData::DateTimeOffset(Some(_)) => {
                if let Some(v) =
                    chrono::DateTime::<chrono::Utc>::from_sql(&self.0).map_err(Self::Error::custom)?
                {
                    visitor.visit_string(format!("{:?}", v))
                } else {
                    visitor.visit_none()
                }
            },
            _ => self.deserialize_any(visitor)
        }
    }

    #[inline]
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::String(Some(v)) => visitor.visit_string(v.into_owned()),
            _ => self.deserialize_str(visitor)
        }
    }

    #[inline]
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::Binary(Some(v)) => visitor.visit_bytes(&v),
            //NOTE 特殊处理Uuid，支持从Bytes反序列化
            ColumnData::Guid(Some(v)) => visitor.visit_bytes(v.as_bytes()),
            _ => self.deserialize_any(visitor)
        }
    }

    #[inline]
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.0 {
            ColumnData::Binary(Some(v)) => visitor.visit_byte_buf(v.into_owned()),
            //NOTE 特殊处理Uuid，支持从Bytes反序列化
            ColumnData::Guid(Some(v)) => visitor.visit_byte_buf(v.as_bytes().to_vec()),
            _ => self.deserialize_any(visitor)
        }
    }

    fn is_human_readable(&self) -> bool { matches!(self.0, ColumnData::String(_)) }

    forward_to_deserialize_integer! {
        deserialize_i8 -> visit_i8
        deserialize_u8 -> visit_u8
        deserialize_i16 -> visit_i16
        deserialize_u16 -> visit_u16
        deserialize_i32 -> visit_i32
        deserialize_u32 -> visit_u32
        deserialize_u64 -> visit_u64
        deserialize_i128 -> visit_i128
    }

    forward_to_deserialize_float! {
        deserialize_f32 -> visit_f32
        deserialize_f64 -> visit_f64
    }

    forward_to_deserialize_any! {
        char
        unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// 行数据访问器
struct RowAccessor {
    col_meta: Vec<Column>,
    col_data: IntoIter<ColumnData<'static>>,
    col_idx: usize
}

impl RowAccessor {
    fn new(row: Row) -> RowAccessor {
        let col_meta = row.0.columns().to_owned();
        let col_data = row.0.into_iter();
        RowAccessor {
            col_meta,
            col_data,
            col_idx: 0
        }
    }
}

/// 顺序访问
impl<'de> SeqAccess<'de> for RowAccessor {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>
    {
        let Some(data) = self.col_data.next() else {
            return Ok(None);
        };
        self.col_idx += 1;
        seed.deserialize(ColumnAccessor(data)).map(Some).map_err(|e| {
            Self::Error::custom(format!("column: {}, {}", self.col_meta[self.col_idx - 1].name(), e))
        })
    }

    fn size_hint(&self) -> Option<usize> { Some(self.col_meta.len()) }
}

/// `Key-Value`访问
impl<'de> MapAccess<'de> for RowAccessor {
    type Error = SerdeError;

    /// 获取当前`Key`
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>
    {
        if self.col_idx >= self.col_meta.len() {
            return Ok(None);
        }

        struct KeyStr<'a>(&'a str);

        impl<'de, 'a> Deserializer<'de> for KeyStr<'a> {
            type Error = SerdeError;

            fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                visitor.visit_str(self.0)
            }

            forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map struct enum identifier ignored_any
            }
        }

        seed.deserialize(KeyStr(self.col_meta[self.col_idx].name())).map(Some)
    }

    /// 获取当前`Value`
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>
    {
        let data = self
            .col_data
            .next()
            .ok_or_else(|| Self::Error::custom("unexpected end of row"))?;
        self.col_idx += 1;
        seed.deserialize(ColumnAccessor(data)).map_err(|e| {
            Self::Error::custom(format!("column: {}, {}", self.col_meta[self.col_idx - 1].name(), e))
        })
    }

    fn size_hint(&self) -> Option<usize> { Some(self.col_meta.len()) }
}
