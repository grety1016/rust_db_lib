//!
//! MSSQL数据库接口
//!

#![allow(dead_code)]

#[macro_use]
extern crate tracing;

mod connection;
mod resultset;
mod row;
mod pool;
mod serde;
mod fmt;
mod error;
mod sql;

pub use connection::Connection;
pub use error::Error;
pub use fmt::{SqlIdent, ToSqlString};
pub use pool::{Executor, Pool, PooledConnection, TokioRuntimeExecutor};
pub use resultset::ResultSet;
pub use row::{ColumnData, ColumnType, Row};
pub use sql::{Sql, IntoSql};

pub type Result<T> = ::std::result::Result<T, error::Error>;

pub mod prelude {
    pub use crate::{fmt::*, sql_bind, sql_format, sql_ident, Sql};
    pub use chrono::prelude::*;
    pub use rust_decimal::prelude::*;
    pub use uuid::Uuid;
}
