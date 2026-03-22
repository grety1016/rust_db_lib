/// 数据库相关错误
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("连接池超时")]
    PoolTimeout,
    #[error("连接初始化超时")]
    PoolInitTimeout,
    #[error("连接检测超时")]
    PoolReuseTimeout,
    #[error("连接池错误, {0:?}")]
    PoolError(#[from] crate::pool::Error),
    #[error(transparent)]
    RawError(#[from] tiberius::error::Error),
    #[error("连接错误, {0:?}")]
    ConnectError(tiberius::error::Error),
    #[error("SQL执行错误, {0:?}")]
    ExecError(tiberius::error::Error),
    #[error("SQL执行超时")]
    ExecTimeout,
    #[error("SQL查询错误, {0:?}")]
    QueryError(tiberius::error::Error),
    #[error("SQL查询超时")]
    QueryTimeout,
    #[error("命令未结束")]
    PendingError,
    #[error("无效的元数据")]
    InvalidMeta,
    #[error("字段不存在")]
    ColumnNotExists,
    #[error("字段类型不匹配")]
    ColumnTypeMismatched,
    #[error("SQL取值错误, {0:?}")]
    FetchError(tiberius::error::Error),
    #[error("{0}")]
    Deserialize(#[from] ::serde::de::value::Error),
    #[error("{0}")]
    Custom(std::borrow::Cow<'static, str>)
}

impl Error {
    pub fn custom(msg: impl std::fmt::Display) -> Error { Error::Custom(msg.to_string().into()) }
}

impl From<bb8::RunError<Error>> for Error {
    fn from(e: bb8::RunError<Error>) -> Self {
        match e {
            bb8::RunError::User(e) => e,
            bb8::RunError::TimedOut => Error::PoolTimeout
        }
    }
}

impl Error {
    /// 服务端错误代码
    pub fn server_code(&self) -> Option<u32> {
        match self {
            Error::PoolError(crate::pool::Error::Tiberius(err)) |
            Error::RawError(err) |
            Error::ExecError(err) |
            Error::QueryError(err) => {
                match err {
                    tiberius::error::Error::Server(err) => Some(err.code()),
                    _ => None
                }
            },
            _ => None
        }
    }
    /// 服务端错误
    pub fn is_server(&self) -> bool { self.server_code().is_some() }
    /// 客户端错误
    pub fn is_client(&self) -> bool { !self.is_server() }
    /// 请求阶段的错误
    pub fn is_request(&self) -> bool {
        match self {
            Error::PoolError(crate::pool::Error::Io(..)) |
            Error::PoolTimeout |
            Error::PoolInitTimeout |
            Error::PoolReuseTimeout |
            Error::ExecTimeout |
            Error::QueryTimeout => true,
            Error::PoolError(crate::pool::Error::Tiberius(err)) |
            Error::RawError(err) |
            Error::ExecError(err) |
            Error::QueryError(err) => {
                matches!(err, tiberius::error::Error::Io { .. })
            },
            _ => false
        }
    }
    /// 数据库正在恢复
    pub fn is_recovering(&self) -> bool {
        matches!(self.server_code(), Some(921..=927))
    }
    /// 唯一约束错误
    pub fn is_unique_violation(&self) -> bool {
        matches!(self.server_code(), Some(2601 | 2627))
    }
    /// 由RAISERROR产生的用户错误
    pub fn is_raised(&self) -> bool {
        matches!(self.server_code(), Some(50000))
    }
}
