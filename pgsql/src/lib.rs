//!
//! PostgreSQL数据库接口
//!

#![allow(dead_code)]

#[macro_use]
extern crate tracing;

mod connection;
mod error;
mod fmt;
mod pool;
mod resultset;
mod row;
mod serde;
mod sql;

pub use connection::{Connection, CopyOutStream};
pub use error::Error;
pub use fmt::{SqlIdent, ToSqlString};
pub use pool::{Executor, Pool, PooledConnection, TokioRuntimeExecutor};
pub use resultset::ResultSet;
pub use row::{ColumnData, ColumnType, Row};
pub use sql::{IntoSql, Sql};

pub type Result<T> = ::std::result::Result<T, error::Error>;

pub mod prelude {
    pub use crate::{fmt::*, sql_bind, sql_format, sql_ident, Sql};
    pub use chrono::prelude::*;
    pub use rust_decimal::prelude::*;
    pub use uuid::Uuid;
}
