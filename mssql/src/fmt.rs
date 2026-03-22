use uuid::Uuid;

pub struct SqlIdent<'a>(&'a str);

impl<'a> SqlIdent<'a> {
    pub fn new(ident: &'a str) -> Self { SqlIdent(ident) }
}

/// 转换为SQL安全字符串
pub trait ToSqlString {
    fn to_sql_string(&self) -> String;
}

impl ToSqlString for &str {
    fn to_sql_string(&self) -> String { format!("N'{}'", self.replace("'", "''")) }
}
impl ToSqlString for &String {
    fn to_sql_string(&self) -> String { format!("N'{}'", self.replace("'", "''")) }
}
impl ToSqlString for String {
    fn to_sql_string(&self) -> String { format!("N'{}'", self.replace("'", "''")) }
}
impl ToSqlString for SqlIdent<'_> {
    fn to_sql_string(&self) -> String { format!("[{}]", self.0.replace(']', "]]")) }
}
impl ToSqlString for Uuid {
    fn to_sql_string(&self) -> String { format!("'{}'", self) }
}
impl ToSqlString for &Uuid {
    fn to_sql_string(&self) -> String { format!("'{}'", self) }
}
impl ToSqlString for chrono::NaiveDateTime {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%Y-%m-%d %H:%M:%S")) }
}
impl ToSqlString for &chrono::NaiveDateTime {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%Y-%m-%d %H:%M:%S")) }
}
impl ToSqlString for chrono::NaiveDate {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%Y-%m-%d")) }
}
impl ToSqlString for &chrono::NaiveDate {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%Y-%m-%d")) }
}
impl ToSqlString for chrono::NaiveTime {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%H:%M:%S")) }
}
impl ToSqlString for &chrono::NaiveTime {
    fn to_sql_string(&self) -> String { format!("'{}'", self.format("%H:%M:%S")) }
}
impl ToSqlString for rust_decimal::Decimal {
    fn to_sql_string(&self) -> String { format!("{}", self) }
}
impl ToSqlString for &rust_decimal::Decimal {
    fn to_sql_string(&self) -> String { format!("{}", self) }
}
impl ToSqlString for u8 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for &u8 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for i16 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for &i16 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for i32 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for &i32 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for i64 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for &i64 {
    fn to_sql_string(&self) -> String { self.to_string() }
}
impl ToSqlString for bool {
    fn to_sql_string(&self) -> String {
        if *self {
            "'Y'".to_owned()
        } else {
            "'N'".to_owned()
        }
    }
}
impl ToSqlString for &bool {
    fn to_sql_string(&self) -> String {
        if **self {
            "'Y'".to_owned()
        } else {
            "'N'".to_owned()
        }
    }
}

impl<T: ToSqlString> ToSqlString for Option<T> {
    fn to_sql_string(&self) -> String {
        match self {
            Some(v) => v.to_sql_string(),
            None => "NULL".to_owned()
        }
    }
}

impl<T: ToSqlString> ToSqlString for Vec<T> {
    fn to_sql_string(&self) -> String {
        format!("({})", self.iter().map(|item| item.to_sql_string()).collect::<Vec<String>>().join(","))
    }
}

impl<T: ToSqlString> ToSqlString for &[T] {
    fn to_sql_string(&self) -> String {
        format!("({})", self.iter().map(|item| item.to_sql_string()).collect::<Vec<String>>().join(","))
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
        $crate::Sql::new(format!($fmt,$($key = $crate::ToSqlString::to_sql_string(&$args)),*))
    };
    ($fmt:literal, $($args:expr),*) => {
        $crate::Sql::new(format!($fmt,$($crate::ToSqlString::to_sql_string(&$args)),*))
    };
}

/// 动态SQL参数绑定（构建`Sql`对象）
#[macro_export]
macro_rules! sql_bind {
    ($sql:expr) => {
        {
            $crate::IntoSql::into_sql($sql)
        }
    };
    ($sql:expr, $($args:expr),*) => {
        {
            let mut sql = $crate::IntoSql::into_sql($sql);
            $(
                sql.bind($args);
            )*
            sql
        }
    };
}
