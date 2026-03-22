use crate::row::Row;
use ::serde::{
    de::{
        value::Error as SerdeError, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor,
    },
    forward_to_deserialize_any,
};
use mysql_async::Value;
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
        false
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct struct map enum identifier ignored_any
    }
}

/// 单行反序列化
pub(crate) struct RowOptional(Option<Row>);

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

impl RowOptional {
    pub(crate) fn new(row: Option<Row>) -> RowOptional {
        RowOptional(row)
    }
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
            Some(row) => row.deserialize_struct(name, fields, visitor),
            None => visitor.visit_none(),
        }
    }

    fn is_human_readable(&self) -> bool {
        false
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
                let mut accessor = RowAccessor::new(self);
                if let Some(val) = accessor.next_value() {
                    ColumnAccessor(val).$func(visitor)
                } else {
                    visitor.visit_none()
                }
            }
        )*
    }
}

/// 使`mysql::Row`类型支持反序列化
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
        false
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
struct ColumnAccessor(Value);

macro_rules! forward_to_deserialize_integer {
    ($($func:ident -> $vis:ident)*) => {
        $(
            #[inline]
            fn $func<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>
            {
                match self.0 {
                    Value::Int(v) => visitor.$vis(v as _),
                    Value::UInt(v) => visitor.$vis(v as _),
                    Value::Float(v) => visitor.$vis(v as _),
                    Value::Double(v) => visitor.$vis(v as _),
                    Value::Bytes(ref v) => {
                        if let Ok(s) = std::str::from_utf8(v) {
                            if let Ok(n) = s.parse() {
                                return visitor.$vis(n);
                            }
                        }
                        self.deserialize_any(visitor)
                    }
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
        match self.0 {
            Value::NULL => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::NULL => visitor.visit_none(),
            Value::Bytes(v) => {
                let bytes = v;
                if let Ok(s) = String::from_utf8(bytes.clone()) {
                    visitor.visit_string(s)
                } else {
                    visitor.visit_byte_buf(bytes)
                }
            }
            Value::Int(v) => visitor.visit_i64(v),
            Value::UInt(v) => visitor.visit_u64(v),
            Value::Float(v) => visitor.visit_f32(v),
            Value::Double(v) => visitor.visit_f64(v),
            Value::Date(y, m, d, h, mi, s, ms) => visitor.visit_string(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                y, m, d, h, mi, s, ms
            )),
            Value::Time(neg, d, h, m, s, ms) => visitor.visit_string(format!(
                "{}{:02}d {:02}:{:02}:{:02}.{:03}",
                if neg { "-" } else { "" },
                d,
                h,
                m,
                s,
                ms / 1000
            )),
        }
    }

    #[inline]
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Int(v) => visitor.visit_bool(v != 0),
            Value::UInt(v) => visitor.visit_bool(v != 0),
            Value::Bytes(ref v) => {
                if v == b"Y" || v == b"N" || v == b"1" || v == b"0" {
                    visitor.visit_bool(v == b"Y" || v == b"1")
                } else {
                    self.deserialize_any(visitor)
                }
            }
            _ => self.deserialize_any(visitor),
        }
    }

    #[inline]
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Int(v) => visitor.visit_i64(v),
            Value::UInt(v) => visitor.visit_i64(v as i64),
            Value::Bytes(ref v) => {
                if let Ok(s) = std::str::from_utf8(v) {
                    if let Ok(n) = s.parse() {
                        return visitor.visit_i64(n);
                    }
                }
                self.deserialize_any(visitor)
            }
            _ => self.deserialize_any(visitor),
        }
    }

    #[inline]
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Value::Bytes(ref v) = self.0 {
            if let Ok(s) = String::from_utf8(v.clone()) {
                return visitor.visit_string(s);
            }
        }
        self.deserialize_any(visitor)
    }

    #[inline]
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Value::Bytes(ref v) = self.0 {
            if let Ok(s) = std::str::from_utf8(v) {
                return visitor.visit_str(s);
            }
        }
        self.deserialize_any(visitor)
    }

    forward_to_deserialize_integer! {
        deserialize_i8 -> visit_i8
        deserialize_u8 -> visit_u8
        deserialize_i16 -> visit_i16
        deserialize_u16 -> visit_u16
        deserialize_i32 -> visit_i32
        deserialize_u32 -> visit_u32
        deserialize_u64 -> visit_u64
        deserialize_i128 -> visit_i128
        deserialize_f32 -> visit_f32
        deserialize_f64 -> visit_f64
    }

    forward_to_deserialize_any! {
        char
        bytes byte_buf
        unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// 行数据访问器
struct RowAccessor {
    row: Row,
    col_idx: usize,
    col_count: usize,
}

impl RowAccessor {
    fn new(row: Row) -> RowAccessor {
        let col_count = row.column_count();
        RowAccessor {
            row,
            col_idx: 0,
            col_count,
        }
    }

    fn next_value(&mut self) -> Option<Value> {
        if self.col_idx < self.col_count {
            let val = self
                .row
                .0
                .take::<Value, _>(self.col_idx)
                .unwrap_or(Value::NULL);
            self.col_idx += 1;
            Some(val)
        } else {
            None
        }
    }
}

/// 顺序访问
impl<'de> SeqAccess<'de> for RowAccessor {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if let Some(val) = self.next_value() {
            seed.deserialize(ColumnAccessor(val)).map(Some)
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.col_count)
    }
}

/// `Key-Value`访问
impl<'de> MapAccess<'de> for RowAccessor {
    type Error = SerdeError;

    /// 获取当前`Key`
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.col_idx >= self.col_count {
            return Ok(None);
        }

        struct KeyStr(String);

        impl<'de> Deserializer<'de> for KeyStr {
            type Error = SerdeError;

            fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                visitor.visit_string(self.0)
            }

            forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map struct enum identifier ignored_any
            }
        }

        let name = self
            .row
            .0
            .columns()
            .get(self.col_idx)
            .map(|c| c.name_str().into_owned())
            .unwrap_or_default();
        seed.deserialize(KeyStr(name)).map(Some)
    }

    /// 获取当前`Value`
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let val = self.next_value().unwrap_or(Value::NULL);
        seed.deserialize(ColumnAccessor(val))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.col_count)
    }
}
