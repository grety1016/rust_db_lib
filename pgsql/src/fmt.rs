use std::fmt;

/// SQL标识符
#[derive(Debug, Clone)]
pub struct SqlIdent(String);

impl SqlIdent {
    /// 创建SQL标识符
    pub fn new(ident: impl Into<String>) -> Self {
        SqlIdent(ident.into())
    }

    /// 获取标识符字符串
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // PostgreSQL使用双引号转义标识符
        write!(f, "\"{}\"", self.0.replace('"', "\"\""))
    }
}

/// 转换为SQL字符串
pub trait ToSqlString {
    fn to_sql_string(&self) -> String;
}

impl ToSqlString for &str {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

impl ToSqlString for &String {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

impl ToSqlString for String {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

impl ToSqlString for SqlIdent {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for uuid::Uuid {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self)
    }
}

impl ToSqlString for &uuid::Uuid {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self)
    }
}

impl ToSqlString for chrono::NaiveDateTime {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl ToSqlString for &chrono::NaiveDateTime {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl ToSqlString for chrono::NaiveDate {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d"))
    }
}

impl ToSqlString for &chrono::NaiveDate {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d"))
    }
}

impl ToSqlString for chrono::NaiveTime {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%H:%M:%S"))
    }
}

impl ToSqlString for &chrono::NaiveTime {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%H:%M:%S"))
    }
}

impl ToSqlString for rust_decimal::Decimal {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for &rust_decimal::Decimal {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for i16 {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for i32 {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for i64 {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}

impl ToSqlString for bool {
    fn to_sql_string(&self) -> String {
        if *self {
            "TRUE".to_owned()
        } else {
            "FALSE".to_owned()
        }
    }
}

impl<T: ToSqlString> ToSqlString for Option<T> {
    fn to_sql_string(&self) -> String {
        match self {
            Some(v) => v.to_sql_string(),
            None => "NULL".to_owned(),
        }
    }
}

impl<T: ToSqlString> ToSqlString for Vec<T> {
    fn to_sql_string(&self) -> String {
        format!(
            "({})",
            self.iter()
                .map(|item| item.to_sql_string())
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

#[macro_export]
macro_rules! sql_ident {
    ($ident:expr) => {
        $crate::SqlIdent::new(AsRef::<str>::as_ref(&$ident))
    };
    ($ident:ident) => {
        $crate::SqlIdent::new(stringify!($ident))
    };
}

/// 静态SQL参数绑定
#[macro_export]
macro_rules! sql_format {
    ($fmt:literal, $($key:ident = $args:expr),*) => {
        $crate::Sql::new(format!($fmt, $($key = $crate::ToSqlString::to_sql_string(&$args)),*))
    };
    ($fmt:literal, $($args:expr),*) => {
        $crate::Sql::new(format!($fmt, $($crate::ToSqlString::to_sql_string(&$args)),*))
    };
}
