//!
//! SQLite 数据库接口
//!

#![allow(dead_code)]

mod connection;
mod error;
mod fmt;
mod resultset;
mod row;
mod serde;
pub mod sql;

pub use connection::Connection;
pub use error::Error;
pub use resultset::ResultSet;
pub use row::{ColumnData, ColumnType, Row};
pub use sql::{IntoSql, Sql};

pub type Result<T> = ::std::result::Result<T, error::Error>;

pub mod prelude {
    pub use crate::{
        sql_bind, ColumnData, ColumnType, Connection, Error, Result, ResultSet, Row, Sql,
    };
    pub use chrono::prelude::*;
    pub use rust_decimal::prelude::*;
    pub use uuid::Uuid;
}
