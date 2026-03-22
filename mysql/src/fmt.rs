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
        // MySQL 使用反引号 (`) 转义标识符
        write!(f, "`{}`", self.0.replace('`', "``"))
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

impl ToSqlString for chrono::NaiveDateTime {
    fn to_sql_string(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl ToSqlString for rust_decimal::Decimal {
    fn to_sql_string(&self) -> String {
        self.to_string()
    }
}
