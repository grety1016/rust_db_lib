use thiserror::Error;

/// 数据库错误
#[derive(Debug, Error)]
pub enum Error {
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
        match self {
            Error::ExecError(msg) | Error::QueryError(msg) => {
                msg.contains("UNIQUE constraint failed")
            }
            _ => false,
        }
    }
}

impl From<tokio_rusqlite::Error> for Error {
    fn from(err: tokio_rusqlite::Error) -> Self {
        match err {
            tokio_rusqlite::Error::Rusqlite(e) => Error::from(e),
            tokio_rusqlite::Error::ConnectionClosed => {
                Error::ConnectionError("Connection closed".to_string())
            }
            tokio_rusqlite::Error::Close((_, e)) => Error::ConnectionError(e.to_string()),
            tokio_rusqlite::Error::Other(e) => Error::Custom(e.to_string()),
            _ => Error::Custom("Unknown error".to_string()),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => Error::RowNotExists,
            rusqlite::Error::InvalidColumnName(_name) => Error::ColumnNotExists,
            rusqlite::Error::InvalidColumnIndex(_idx) => Error::ColumnNotExists,
            rusqlite::Error::ToSqlConversionFailure(e) => Error::TypeConversion(e.to_string()),
            rusqlite::Error::FromSqlConversionFailure(idx, ty, e) => {
                Error::TypeConversion(format!("column {}: {} -> {}", idx, ty, e))
            }
            _ => Error::ExecError(err.to_string()),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}
