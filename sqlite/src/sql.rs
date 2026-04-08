use crate::{ColumnData, Connection, Result, ResultSet, Row};
use std::borrow::Cow;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

/// SQL语句
#[derive(Debug, Clone)]
pub struct Sql<'a> {
    pub(crate) sql: Cow<'a, str>,
    pub(crate) params: Vec<ColumnData>,
}

impl<'a> Sql<'a> {
    /// 创建参数化SQL
    pub fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        Sql {
            sql: sql.into(),
            params: vec![],
        }
    }

    /// 绑定参数
    pub fn bind<T>(&mut self, param: T) -> &mut Self
    where
        T: Into<ColumnData>,
    {
        self.params.push(param.into());
        self
    }

    /// 预览语句
    pub fn preview(&self) -> String {
        let mut sql = self.sql.to_string();
        // SQLite common placeholder is '?'
        for val in &self.params {
            sql = sql.replacen('?', &format!("{:?}", val), 1);
        }
        // Handle $1, $2 style as well for preview (supported by SQLite as named params)
        if sql.contains('$') {
            for (i, val) in self.params.iter().enumerate() {
                let placeholder = format!("${}", i + 1);
                sql = sql.replace(&placeholder, &format!("{:?}", val));
            }
        }
        format!("{} [params={}]", sql, self.params.len())
    }

    /// 执行命令
    pub(crate) async fn exec(&self, conn: &Connection, _duration: Option<Duration>) -> Result<u64> {
        let sql_preview = self.preview();
        debug!("SQLite exec: {}", sql_preview);
        let now = Instant::now();

        let rv = conn
            .execute_raw(self.sql.as_ref(), self.params.clone())
            .await;

        match rv {
            Ok(effected) => {
                info!(
                    "SQLite exec success: {}ms, effected: {}, sql: {}",
                    now.elapsed().as_millis(),
                    effected,
                    sql_preview
                );
                Ok(effected as u64)
            }
            Err(e) => {
                error!(
                    "SQLite exec error: {}ms, error: {}, sql: {}",
                    now.elapsed().as_millis(),
                    e,
                    sql_preview
                );
                Err(e)
            }
        }
    }

    /// 查询多行并返回 ResultSet
    pub(crate) async fn query(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<ResultSet> {
        let sql_preview = self.preview();
        debug!("SQLite query: {}", sql_preview);
        let now = Instant::now();

        let rv = conn
            .fetch_all_raw(self.sql.as_ref(), self.params.clone())
            .await;

        match rv {
            Ok(rs) => {
                info!(
                    "SQLite query success: {}ms, rows: {}, sql: {}",
                    now.elapsed().as_millis(),
                    rs.len(),
                    sql_preview
                );
                Ok(rs)
            }
            Err(e) => {
                error!(
                    "SQLite query error: {}ms, error: {}, sql: {}",
                    now.elapsed().as_millis(),
                    e,
                    sql_preview
                );
                Err(e)
            }
        }
    }

    /// 查询多行并返回 Vec<Row>
    pub(crate) async fn query_collect_row(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Vec<Row>> {
        let sql_preview = self.preview();
        debug!("SQLite query: {}", sql_preview);
        let now = Instant::now();

        let rv = conn
            .fetch_all_raw(self.sql.as_ref(), self.params.clone())
            .await;

        match rv {
            Ok(rs) => {
                info!(
                    "SQLite query success: {}ms, rows: {}, sql: {}",
                    now.elapsed().as_millis(),
                    rs.len(),
                    sql_preview
                );
                Ok(rs.rows)
            }
            Err(e) => {
                error!(
                    "SQLite query error: {}ms, error: {}, sql: {}",
                    now.elapsed().as_millis(),
                    e,
                    sql_preview
                );
                Err(e)
            }
        }
    }

    /// 查询第一条行数据
    pub(crate) async fn query_first_row(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Option<Row>> {
        let rows = self.query_collect_row(conn, _duration).await?;
        Ok(rows.into_iter().next())
    }

    /// 查询标量值(i64)
    pub(crate) async fn query_scalar_i64(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Option<i64>> {
        let row = self.query_first_row(conn, _duration).await?;
        match row {
            Some(row) => row.try_get_i64(0),
            None => Ok(None),
        }
    }

    /// 查询标量值(字符串)
    pub(crate) async fn query_scalar_string(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Option<String>> {
        let row = self.query_first_row(conn, _duration).await?;
        match row {
            Some(row) => row.try_get_string(0),
            None => Ok(None),
        }
    }

    /// 查询标量值(bool)
    pub(crate) async fn query_scalar_bool(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Option<bool>> {
        let row = self.query_first_row(conn, _duration).await?;
        match row {
            Some(row) => row.try_get_bool(0),
            None => Ok(None),
        }
    }

    /// 查询标量值(Decimal)
    pub(crate) async fn query_scalar_decimal(
        &self,
        conn: &Connection,
        _duration: Option<Duration>,
    ) -> Result<Option<rust_decimal::Decimal>> {
        let row = self.query_first_row(conn, _duration).await?;
        match row {
            Some(row) => row.try_get_decimal(0),
            None => Ok(None),
        }
    }
}

/// 转换为SQL语句
pub trait IntoSql<'a> {
    fn into_sql(self) -> Sql<'a>;
}

impl<'a> IntoSql<'a> for &'a str {
    fn into_sql(self) -> Sql<'a> {
        Sql::new(self)
    }
}

impl<'a> IntoSql<'a> for String {
    fn into_sql(self) -> Sql<'a> {
        Sql::new(self)
    }
}

impl<'a> IntoSql<'a> for Sql<'a> {
    fn into_sql(self) -> Sql<'a> {
        self
    }
}

impl<'a> IntoSql<'a> for &mut Sql<'a> {
    fn into_sql(self) -> Sql<'a> {
        self.clone()
    }
}

/// 宏支持
#[macro_export]
macro_rules! sql_bind {
    ($sql:expr) => {{
        $crate::sql::IntoSql::into_sql($sql)
    }};
    ($sql:expr, $($arg:expr),*) => {{
        let mut sql = $crate::sql::IntoSql::into_sql($sql);
        $(sql.bind($arg);)*
        sql
    }};
}

// Implement From for ColumnData to support various types in bind
impl From<String> for ColumnData {
    fn from(s: String) -> Self {
        ColumnData::String(s)
    }
}

impl From<&str> for ColumnData {
    fn from(s: &str) -> Self {
        ColumnData::String(s.to_string())
    }
}

impl From<i64> for ColumnData {
    fn from(i: i64) -> Self {
        ColumnData::Int(i)
    }
}

impl From<i32> for ColumnData {
    fn from(i: i32) -> Self {
        ColumnData::Int(i as i64)
    }
}

impl From<u32> for ColumnData {
    fn from(i: u32) -> Self {
        ColumnData::Int(i as i64)
    }
}

impl From<f64> for ColumnData {
    fn from(f: f64) -> Self {
        ColumnData::Float(f)
    }
}

impl From<bool> for ColumnData {
    fn from(b: bool) -> Self {
        ColumnData::Bool(b)
    }
}

impl From<rust_decimal::Decimal> for ColumnData {
    fn from(d: rust_decimal::Decimal) -> Self {
        ColumnData::Decimal(d)
    }
}

impl From<chrono::NaiveDateTime> for ColumnData {
    fn from(dt: chrono::NaiveDateTime) -> Self {
        ColumnData::DateTime(dt)
    }
}

impl From<uuid::Uuid> for ColumnData {
    fn from(u: uuid::Uuid) -> Self {
        ColumnData::Uuid(u)
    }
}

impl From<Vec<u8>> for ColumnData {
    fn from(v: Vec<u8>) -> Self {
        ColumnData::Blob(v)
    }
}
