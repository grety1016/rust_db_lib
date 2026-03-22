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

    #[error("MySQL Driver Error: {0}")]
    DriverError(#[from] mysql_async::Error),

    #[error("MySQL URL Error: {0}")]
    UrlError(#[from] mysql_async::UrlError),

    #[error("MySQL Row Error: {0}")]
    RowError(#[from] mysql_async::FromRowError),

    #[error("MySQL From Value Error: {0}")]
    FromValueError(#[from] mysql_async::FromValueError),
}

impl Error {
    /// 创建自定义错误
    pub fn custom(msg: impl Into<String>) -> Self {
        Self::Custom(msg.into())
    }

    /// 检查是否为唯一约束错误 (MySQL Error 1062)
    pub fn is_unique_violation(&self) -> bool {
        match self {
            Error::DriverError(mysql_async::Error::Server(e)) => e.code == 1062,
            _ => false,
        }
    }
}

impl From<bb8::RunError<Error>> for Error {
    fn from(err: bb8::RunError<Error>) -> Self {
        match err {
            bb8::RunError::User(e) => e,
            bb8::RunError::TimedOut => Error::PoolTimeout,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Custom(err.to_string())
    }
}
