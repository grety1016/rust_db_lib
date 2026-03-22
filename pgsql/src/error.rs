use std::io;
use thiserror::Error;

/// 数据库错误
#[derive(Debug, Error)]
pub enum Error {
    #[error("连接池超时")]
    PoolTimeout,

    #[error("连接初始化超时")]
    PoolInitTimeout,

    #[error("连接检测超时")]
    PoolReuseTimeout,

    #[error("connection error: {0}")]
    ConnectionError(String),

    #[error("execution error: {0}")]
    ExecError(String),

    #[error("query error: {0}")]
    QueryError(String),

    #[error("query timeout")]
    QueryTimeout,

    #[error("execution timeout")]
    ExecTimeout,

    #[error("pool error: {0}")]
    PoolError(String),

    #[error("pending operation error")]
    PendingError,

    #[error("column not exists")]
    ColumnNotExists,

    #[error("row not exists")]
    RowNotExists,

    #[error("type conversion error: {0}")]
    TypeConversion(String),

    #[error("custom error: {0}")]
    Custom(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),
}

impl Error {
    /// 创建自定义错误
    pub fn custom(msg: impl Into<String>) -> Self {
        Self::Custom(msg.into())
    }

    /// 检查是否为唯一约束错误
    pub fn is_unique_violation(&self) -> bool {
        matches!(self.server_code(), Some("23505"))
    }

    /// 检查是否为RAISE产生的用户错误
    pub fn is_raised(&self) -> bool {
        matches!(self.server_code(), Some("P0001"))
    }

    /// 获取PostgreSQL错误代码
    pub fn server_code(&self) -> Option<&str> {
        match self {
            Error::ConnectionError(msg) => extract_pg_code(msg),
            Error::ExecError(msg) => extract_pg_code(msg),
            Error::QueryError(msg) => extract_pg_code(msg),
            Error::PoolError(msg) => extract_pg_code(msg),
            _ => None,
        }
    }
}

fn extract_pg_code(msg: &str) -> Option<&str> {
    // 从错误消息中提取PostgreSQL错误代码
    // 例如: "error returned from database: 23505 (unique_violation)"
    if let Some(start) = msg.find("error returned from database: ") {
        let code_start = start + "error returned from database: ".len();
        if let Some(end) = msg[code_start..].find(|c: char| !c.is_ascii_digit()) {
            return Some(&msg[code_start..code_start + end]);
        }
    }
    None
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        if err.is_closed() {
            Error::ConnectionError(err.to_string())
        } else if let Some(db_err) = err.as_db_error() {
            match db_err.code().code() {
                "23505" => Error::ExecError(err.to_string()), // unique violation
                "P0001" => Error::ExecError(err.to_string()), // raise exception
                _ => Error::ExecError(err.to_string()),
            }
        } else {
            Error::ExecError(err.to_string())
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::ConnectionError(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<rust_decimal::Error> for Error {
    fn from(err: rust_decimal::Error) -> Self {
        Error::TypeConversion(err.to_string())
    }
}

impl From<bb8::RunError<Error>> for Error {
    fn from(e: bb8::RunError<Error>) -> Self {
        match e {
            bb8::RunError::User(e) => e,
            bb8::RunError::TimedOut => Error::PoolTimeout,
        }
    }
}

impl From<crate::pool::Error> for Error {
    fn from(err: crate::pool::Error) -> Self {
        match err {
            crate::pool::Error::Pg(e) => Error::from(e),
            crate::pool::Error::Io(e) => Error::from(e),
        }
    }
}
