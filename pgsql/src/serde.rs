use crate::row::{ColumnData, Row};
use ::serde::{
    de::{
        value::Error as SerdeError, DeserializeSeed, Deserializer, Error as SerdeErrorTrait,
        MapAccess, SeqAccess, Visitor,
    },
    forward_to_deserialize_any,
};
use std::vec::IntoIter;

/// 多行结果集反序列化
pub(crate) struct RowCollection(Vec<Row>);

impl RowCollection {
    pub(crate) fn new(rows: Vec<Row>) -> RowCollection {
        RowCollection(rows)
    }
}

impl<'de> Deserializer<'de> for RowCollection {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    /// 支持反序列化为`Array`
    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct RowsAccessor {
            rows: IntoIter<Row>,
            len: usize,
        }

        impl<'de> SeqAccess<'de> for RowsAccessor {
            type Error = SerdeError;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: DeserializeSeed<'de>,
            {
                match self.rows.next() {
                    Some(row) => seed.deserialize(row).map(Some),
                    None => Ok(None),
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(RowsAccessor {
            len: self.0.len(),
            rows: self.0.into_iter(),
        })
    }

    fn is_human_readable(&self) -> bool {
        true
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct struct map enum identifier ignored_any
    }
}

/// 单行反序列化
pub(crate) struct RowOptional(Option<Row>);

impl RowOptional {
    pub(crate) fn new(row: Option<Row>) -> RowOptional {
        RowOptional(row)
    }
}

macro_rules! forward_to_row_first_column{
    ($($func:ident)*) => {
        $(
            forward_to_row_first_column_impl!{ $func }
        )*
    };
}

macro_rules! forward_to_row_first_column_impl {
    (deserialize_option) => {
        #[inline]
        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self.0 {
                Some(row) => {
                    if row.column_count() == 1 {
                        row.deserialize_option(visitor)
                    } else {
                        visitor.visit_some(row)
                    }
                }
                None => visitor.visit_none(),
            }
        }
    };
    ($func:ident) => {
        #[inline]
        fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self.0 {
                Some(row) => {
                    if row.column_count() == 1 {
                        row.$func(visitor)
                    } else {
                        row.deserialize_any(visitor)
                    }
                }
                None => visitor.visit_none(),
            }
        }
    };
}

impl<'de> Deserializer<'de> for RowOptional {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => row.deserialize_any(visitor),
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_unit_struct(name, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            }
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_newtype_struct(name, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            }
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_enum(name, variants, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            }
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => row.deserialize_seq(visitor),
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => row.deserialize_tuple(len, visitor),
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => row.deserialize_tuple_struct(name, len, visitor),
            None => visitor.visit_none(),
        }
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Some(row) => {
                if row.column_count() == 1 {
                    row.deserialize_struct(name, fields, visitor)
                } else {
                    row.deserialize_any(visitor)
                }
            }
            None => visitor.visit_none(),
        }
    }

    fn is_human_readable(&self) -> bool {
        true
    }

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
                if self.column_count() > 0 {
                    if let Ok(Some(col)) = self.try_get_any(0) {
                        ColumnAccessor(col).$func(visitor)
                    } else {
                        visitor.visit_none()
                    }
                } else {
                    visitor.visit_none()
                }
            }
        )*
    }
}

/// 使`pgsql::Row`类型支持反序列化
impl<'de> Deserializer<'de> for Row {
    type Error = SerdeError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(RowAccessor::new(self))
    }

    /// 支持反序列化为`Array`
    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    /// 支持反序列化为`Tuple`
    #[inline]
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    /// 支持反序列化为`TupleStruct`
    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(RowAccessor::new(self))
    }

    fn is_human_readable(&self) -> bool {
        true
    }

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
struct ColumnAccessor(ColumnData);

macro_rules! forward_to_deserialize_integer {
    ($($func:ident -> $vis:ident)*) => {
        $(
            #[inline]
            fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                match self.0 {
                    ColumnData::Int(v) => visitor.$vis(v as _),
                    ColumnData::Float(v) => visitor.$vis(v as _),
                    ColumnData::Bool(v) => {
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
                    ColumnData::Float(v) => visitor.$vis(v as _),
                    ColumnData::Int(v) => visitor.$vis(v as _),
                    ColumnData::Bool(v) => {
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
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            ColumnData::Bool(v) => visitor.visit_bool(v),
            ColumnData::Int(v) => visitor.visit_i64(v),
            ColumnData::Float(v) => visitor.visit_f64(v),
            ColumnData::Decimal(v) => visitor.visit_string(format!("{:?}", v)),
            ColumnData::DateTime(v) => visitor.visit_string(format!("{:?}", v)),
            ColumnData::Date(v) => visitor.visit_string(format!("{:?}", v)),
            ColumnData::Time(v) => visitor.visit_string(format!("{:?}", v)),
            ColumnData::Uuid(v) => visitor.visit_string(v.to_string()),
            ColumnData::String(v) => visitor.visit_string(v),
        }
    }

    #[inline]
    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            ColumnData::String(v) => {
                visitor.visit_enum(::serde::de::value::StrDeserializer::new(&v))
            }
            _ => Err(SerdeError::custom("expected string for enum")),
        }
    }

    fn is_human_readable(&self) -> bool {
        matches!(
            self.0,
            ColumnData::String(_)
                | ColumnData::Decimal(_)
                | ColumnData::DateTime(_)
                | ColumnData::Date(_)
                | ColumnData::Time(_)
                | ColumnData::Uuid(_)
        )
    }

    forward_to_deserialize_integer! {
        deserialize_i8 -> visit_i8
        deserialize_i16 -> visit_i16
        deserialize_i32 -> visit_i32
        deserialize_i64 -> visit_i64
        deserialize_i128 -> visit_i128
        deserialize_u8 -> visit_u8
        deserialize_u16 -> visit_u16
        deserialize_u32 -> visit_u32
        deserialize_u64 -> visit_u64
        deserialize_u128 -> visit_u128
    }

    forward_to_deserialize_float! {
        deserialize_f32 -> visit_f32
        deserialize_f64 -> visit_f64
    }

    forward_to_deserialize_any! {
        bool char str string bytes byte_buf unit unit_struct newtype_struct
        seq tuple tuple_struct struct map identifier ignored_any
    }
}

/// 行数据访问器
struct RowAccessor {
    row: Row,
    current: usize,
    len: usize,
}

impl RowAccessor {
    fn new(row: Row) -> Self {
        let len = row.column_count();
        RowAccessor {
            row,
            current: 0,
            len,
        }
    }
}

impl<'de> MapAccess<'de> for RowAccessor {
    type Error = SerdeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.current >= self.len {
            return Ok(None);
        }
        let name = self
            .row
            .column_name(self.current)
            .ok_or_else(|| SerdeError::custom("column name not found"))?;
        seed.deserialize(::serde::de::value::StrDeserializer::new(name))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let data = self
            .row
            .try_get_any(self.current)
            .map_err(|e| SerdeError::custom(e.to_string()))?;
        self.current += 1;
        match data {
            Some(v) => seed.deserialize(ColumnAccessor(v)),
            None => seed.deserialize(::serde::de::value::UnitDeserializer::new()),
        }
    }
}

impl<'de> SeqAccess<'de> for RowAccessor {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.current >= self.len {
            return Ok(None);
        }
        let data = self
            .row
            .try_get_any(self.current)
            .map_err(|e| SerdeError::custom(e.to_string()))?;
        self.current += 1;
        match data {
            Some(v) => seed.deserialize(ColumnAccessor(v)).map(Some),
            None => seed
                .deserialize(::serde::de::value::UnitDeserializer::new())
                .map(Some),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len - self.current)
    }
}
