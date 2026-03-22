use crate::{Connection, Error, Result, ResultSet};
use futures_util::{StreamExt, TryStreamExt};
use std::{borrow::Cow, time};
use tokio::time::timeout;

fn log_sql_params_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        fn parse_bool(v: &str) -> bool {
            let v = v.trim().to_ascii_lowercase();
            if matches!(v.as_str(), "0" | "false" | "no" | "off") {
                return false;
            }
            matches!(v.as_str(), "1" | "true" | "yes" | "on" | "full") || !v.is_empty()
        }

        fn read_from_file() -> Option<bool> {
            let mut candidates: Vec<std::path::PathBuf> = Vec::new();
            if let Ok(cwd) = std::env::current_dir() {
                candidates.push(cwd.join("db_log.conf"));
                candidates.push(cwd.join(".db_log.conf"));
                candidates.push(cwd.join(".env"));
                candidates.push(cwd.join(".env.local"));
            }
            let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            candidates.push(manifest_dir.join("db_log.conf"));
            candidates.push(manifest_dir.join(".db_log.conf"));
            candidates.push(manifest_dir.join(".env"));
            candidates.push(manifest_dir.join(".env.local"));
            if let Some(parent) = manifest_dir.parent() {
                candidates.push(parent.join("db_log.conf"));
                candidates.push(parent.join(".db_log.conf"));
                candidates.push(parent.join(".env"));
                candidates.push(parent.join(".env.local"));
            }

            for path in candidates {
                if let Ok(s) = std::fs::read_to_string(&path) {
                    for line in s.lines() {
                        let line = line.trim();
                        if line.is_empty() || line.starts_with('#') {
                            continue;
                        }
                        if let Some((k, v)) = line.split_once('=') {
                            let k = k.trim().to_ascii_lowercase();
                            let v = v.trim();
                            if k == "db_log_sql_params"
                                || k == "db_sql_log_params"
                                || k == "log_sql_params"
                            {
                                return Some(parse_bool(v));
                            }
                        }
                    }
                }
            }
            None
        }

        if let Ok(v) = std::env::var("DB_LOG_SQL_PARAMS") {
            return parse_bool(&v);
        }
        if let Ok(v) = std::env::var("DB_SQL_LOG_PARAMS") {
            return parse_bool(&v);
        }
        read_from_file().unwrap_or(true)
    })
}

/// SQL语句
#[derive(Debug)]
pub struct Sql<'a> {
    sql: Cow<'a, str>,
    params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send + 'a>>,
    param_previews: Vec<String>,
}

impl<'a> Sql<'a> {
    /// 创建参数化SQL
    pub fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        Sql {
            sql: sql.into(),
            params: vec![],
            param_previews: vec![],
        }
    }

    /// 绑定参数
    pub fn bind<T>(&mut self, param: T)
    where
        T: tokio_postgres::types::ToSql + Sync + Send + std::fmt::Debug + 'a,
    {
        if log_sql_params_enabled() {
            self.param_previews.push(format!("{:?}", &param));
        } else {
            self.param_previews.push("<redacted>".to_string());
        }
        self.params.push(Box::new(param));
    }

    /// 绑定字符串参数 (handles string slices by converting to owned String)
    pub fn bind_str(&mut self, param: &str) {
        if log_sql_params_enabled() {
            self.param_previews.push(format!("{:?}", param));
        } else {
            self.param_previews.push("<redacted>".to_string());
        }
        self.params.push(Box::new(param.to_owned()));
    }

    /// 绑定 optional string 参数
    pub fn bind_opt_str(&mut self, param: Option<&str>) {
        if log_sql_params_enabled() {
            self.param_previews.push(format!("{:?}", param));
        } else {
            self.param_previews.push("<redacted>".to_string());
        }
        self.params.push(Box::new(param.map(|s| s.to_owned())));
    }

    /// 预览语句
    pub fn preview(&self) -> String {
        lazy_static::lazy_static! {
            static ref RE_SQL_LOG_FMT: regex::Regex = regex::Regex::new(r"\s+").unwrap();
        }
        let mut sql = self.sql.to_string();
        for (i, val) in self.param_previews.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            sql = sql.replace(&placeholder, val);
        }
        let sql = RE_SQL_LOG_FMT.replace_all(&sql, " ").into_owned();
        format!("{} [params={}]", sql, self.params.len())
    }

    /// 执行命令
    ///
    /// # Parameters
    ///
    /// - **conn** 连接
    /// - **duration** 超时时间
    ///
    /// # Returns
    ///
    /// 返回影响的行数
    pub(crate) async fn exec(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<u64> {
        if conn.is_pending() {
            return Err(Error::PendingError);
        }
        let sql_preview = self.preview();
        info!(
            "#{}> [{}] @{} - executing, sql: {}",
            conn.spid(),
            conn.log_category(),
            conn.log_db_name(),
            sql_preview
        );
        conn.set_pending(true);
        let now = time::Instant::now();

        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            self.params.iter().map(|p| p.as_ref() as _).collect();

        let rv = {
            let raw = conn.raw_ref().lock().await;
            if let Some(duration) = duration {
                timeout(duration, raw.execute(self.sql.as_ref(), &params))
                    .await
                    .map_err(|_| Error::ExecTimeout)
                    .and_then(|rv| rv.map_err(Error::from))
            } else {
                raw.execute(self.sql.as_ref(), &params)
                    .await
                    .map_err(Error::from)
            }
        };

        match rv {
            Ok(effected) => {
                conn.set_pending(false);
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, effected: {}, sql: {}",
                    conn.spid(),
                    conn.log_category(),
                    conn.log_db_name(),
                    now.elapsed().as_millis(),
                    effected,
                    sql_preview
                );
                Ok(effected)
            }
            Err(e) => {
                conn.set_pending(false);
                if matches!(e, Error::ExecTimeout) {
                    warn!(
                        "#{}> [{}] @{} - elapsed: {}ms, timeout, sql: {}",
                        conn.spid(),
                        conn.log_category(),
                        conn.log_db_name(),
                        now.elapsed().as_millis(),
                        sql_preview
                    );
                } else {
                    error!(
                        "#{}> [{}] @{} - elapsed: {}ms, error: {}, sql: {}",
                        conn.spid(),
                        conn.log_category(),
                        conn.log_db_name(),
                        now.elapsed().as_millis(),
                        e,
                        sql_preview
                    );
                }
                if matches!(e, Error::ExecTimeout) {
                    let _ = conn.reconnect().await;
                }
                Err(e)
            }
        }
    }

    /// 查询
    ///
    /// # Parameters
    ///
    /// - **conn** 连接
    /// - **duration** 超时时间
    ///
    /// # Returns
    ///
    /// 返回结果集
    pub(crate) async fn query<'b>(
        self,
        conn: &'b Connection,
        duration: Option<time::Duration>,
    ) -> Result<ResultSet<'b>> {
        if conn.is_pending() {
            return Err(Error::PendingError);
        }
        let sql_preview = self.preview();
        info!(
            "#{}> [{}] @{} - querying, sql: {}",
            conn.spid(),
            conn.log_category(),
            conn.log_db_name(),
            sql_preview
        );
        conn.set_pending(true);
        let now = time::Instant::now();

        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            self.params.iter().map(|p| p.as_ref() as _).collect();

        let guard = conn.raw_ref().lock().await;
        let rv = if let Some(duration) = duration {
            timeout(duration, guard.query_raw(self.sql.as_ref(), params))
                .await
                .map_err(|_| Error::QueryTimeout)
                .and_then(|rv| rv.map_err(Error::from))
        } else {
            guard
                .query_raw(self.sql.as_ref(), params)
                .await
                .map_err(Error::from)
        };

        match rv {
            Ok(stream) => {
                conn.set_pending(false);
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, streaming started, sql: {}",
                    conn.spid(),
                    conn.log_category(),
                    conn.log_db_name(),
                    now.elapsed().as_millis(),
                    sql_preview
                );
                // 使用 unsafe transmute 延长流的生命周期到 'b (Connection 生命周期)
                // 这样流就可以和 MutexGuard 一起存在于 ResultSet 中
                let stream = stream.map_err(Error::from);
                let stream: futures_util::stream::BoxStream<'b, Result<tokio_postgres::Row>> =
                    unsafe { std::mem::transmute(stream.boxed()) };

                Ok(crate::resultset::ResultSet::new(
                    stream,
                    conn.alive_rs_ref(),
                    guard,
                ))
            }
            Err(e) => {
                conn.set_pending(false);
                if matches!(e, Error::QueryTimeout) {
                    warn!(
                        "#{}> [{}] @{} - elapsed: {}ms, timeout, sql: {}",
                        conn.spid(),
                        conn.log_category(),
                        conn.log_db_name(),
                        now.elapsed().as_millis(),
                        sql_preview
                    );
                } else {
                    error!(
                        "#{}> [{}] @{} - elapsed: {}ms, error: {}, sql: {}",
                        conn.spid(),
                        conn.log_category(),
                        conn.log_db_name(),
                        now.elapsed().as_millis(),
                        e,
                        sql_preview
                    );
                }
                if matches!(e, Error::QueryTimeout) {
                    let _ = conn.reconnect().await;
                }
                Err(e)
            }
        }
    }

    /// 查询第一条行数据
    pub(crate) async fn query_first_row(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<crate::row::Row>> {
        let mut rs = self.query(conn, duration).await?;
        rs.first_row().await
    }

    /// 查询标量值(字符串)
    pub(crate) async fn query_scalar_string(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<String>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_string().await
    }

    /// 查询标量值(i64)
    pub(crate) async fn query_scalar_i64(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<i64>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_i64().await
    }

    /// 查询标量值(f64)
    pub(crate) async fn query_scalar_f64(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<f64>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_f64().await
    }

    /// 查询标量值(bool)
    pub(crate) async fn query_scalar_bool(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<bool>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_bool().await
    }

    /// 查询标量值(Decimal)
    pub(crate) async fn query_scalar_dec(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<rust_decimal::Decimal>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_dec().await
    }

    /// 查询标量值(DateTime)
    pub(crate) async fn query_scalar_datetime(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<chrono::NaiveDateTime>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_datetime().await
    }

    /// 查询标量值(UUID)
    pub(crate) async fn query_scalar_uuid(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<uuid::Uuid>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_uuid().await
    }

    /// 查询并收集行数据
    pub(crate) async fn query_collect_row(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Vec<crate::row::Row>> {
        let mut rs = self.query(conn, duration).await?;
        rs.collect_row().await
    }

    /// 查询并收集结果
    pub(crate) async fn query_collect<R>(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let rs = self.query(conn, duration).await?;
        rs.collect().await
    }

    /// 查询第一条记录
    pub(crate) async fn query_first<R>(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let mut rs = self.query(conn, duration).await?;
        rs.first().await
    }

    /// 查询标量值(u8)
    pub(crate) async fn query_scalar_u8(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<u8>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_u8().await
    }

    /// 查询标量值(i16)
    pub(crate) async fn query_scalar_i16(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<i16>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_i16().await
    }

    /// 查询标量值(i32)
    pub(crate) async fn query_scalar_i32(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<i32>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_i32().await
    }

    /// 查询标量值(f32)
    pub(crate) async fn query_scalar_f32(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<f32>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_f32().await
    }

    /// 查询标量值(Date)
    pub(crate) async fn query_scalar_date(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<chrono::NaiveDate>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_date().await
    }

    /// 查询标量值(Time)
    pub(crate) async fn query_scalar_time(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<chrono::NaiveTime>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_time().await
    }

    /// 查询标量值(任意类型)
    pub(crate) async fn query_scalar_any(
        self,
        conn: &Connection,
        duration: Option<time::Duration>,
    ) -> Result<Option<crate::row::ColumnData>> {
        let mut rs = self.query(conn, duration).await?;
        rs.scalar_any().await
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

/// 宏支持
#[macro_export]
macro_rules! sql_bind {
    ($sql:expr) => {{
        $crate::IntoSql::into_sql($sql)
    }};
    ($sql:expr, $($arg:expr),*) => {{
        let mut sql = $crate::IntoSql::into_sql($sql);
        $(sql.bind($arg);)*
        sql
    }};
}
