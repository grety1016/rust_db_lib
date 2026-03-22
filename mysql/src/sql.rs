use crate::{Error, Result, ResultSet};
use mysql_async::params::Params;
use mysql_async::prelude::Queryable;
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
    params: Vec<mysql_async::Value>,
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
        T: Into<mysql_async::Value> + std::fmt::Debug,
    {
        if log_sql_params_enabled() {
            self.param_previews.push(format!("{:?}", &param));
        } else {
            self.param_previews.push("<redacted>".to_string());
        }
        self.params.push(param.into());
    }

    /// 预览语句
    pub fn preview(&self) -> String {
        lazy_static::lazy_static! {
            static ref RE_SQL_LOG_FMT: regex::Regex = regex::Regex::new(r"\s+").unwrap();
        }
        let mut sql = self.sql.to_string();
        for val in &self.param_previews {
            if let Some(pos) = sql.find('?') {
                sql.replace_range(pos..pos + 1, val);
            }
        }
        let sql = RE_SQL_LOG_FMT.replace_all(&sql, " ").into_owned();
        format!("{} [params={}]", sql, self.params.len())
    }

    /// 执行接口
    pub(crate) async fn exec_interface<E>(
        self,
        conn: &E,
        duration: Option<time::Duration>,
    ) -> Result<u64>
    where
        E: crate::connection::MySqlExecutor + ?Sized,
    {
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
        let is_empty_params = self.params.is_empty();
        let params = if is_empty_params {
            Params::Empty
        } else {
            Params::Positional(self.params)
        };

        let (rv, effected) = {
            let mut raw = conn.lock_raw().await;
            let rv = if let Some(duration) = duration {
                if is_empty_params {
                    timeout(duration, raw.query_drop(self.sql.as_ref()))
                        .await
                        .map_err(|_| Error::ExecTimeout)
                        .and_then(|rv| rv.map_err(Error::from))
                } else {
                    timeout(duration, raw.exec_drop(self.sql.as_ref(), params))
                        .await
                        .map_err(|_| Error::ExecTimeout)
                        .and_then(|rv| rv.map_err(Error::from))
                }
            } else if is_empty_params {
                raw.query_drop(self.sql.as_ref()).await.map_err(Error::from)
            } else {
                raw.exec_drop(self.sql.as_ref(), params)
                    .await
                    .map_err(Error::from)
            };
            let effected = rv.as_ref().ok().map(|_| raw.affected_rows());
            (rv, effected)
        };

        match rv {
            Ok(_) => {
                conn.set_pending(false);
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, effected: {}, sql: {}",
                    conn.spid(),
                    conn.log_category(),
                    conn.log_db_name(),
                    now.elapsed().as_millis(),
                    effected.unwrap_or(0),
                    sql_preview
                );
                Ok(effected.unwrap_or(0))
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

    /// 查询接口
    pub(crate) async fn query_interface<'b, E>(
        self,
        conn: &'b E,
        duration: Option<time::Duration>,
    ) -> Result<ResultSet<'b>>
    where
        E: crate::connection::MySqlExecutor + ?Sized,
    {
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
        let is_empty_params = self.params.is_empty();
        let params = if is_empty_params {
            Params::Empty
        } else {
            Params::Positional(self.params)
        };

        let rv = {
            let mut raw = conn.lock_raw().await;
            if let Some(duration) = duration {
                if is_empty_params {
                    timeout(duration, raw.query(self.sql.as_ref()))
                        .await
                        .map_err(|_| Error::QueryTimeout)
                        .and_then(|rv| rv.map_err(Error::from))
                } else {
                    timeout(duration, raw.exec(self.sql.as_ref(), params))
                        .await
                        .map_err(|_| Error::QueryTimeout)
                        .and_then(|rv| rv.map_err(Error::from))
                }
            } else if is_empty_params {
                raw.query(self.sql.as_ref()).await.map_err(Error::from)
            } else {
                raw.exec(self.sql.as_ref(), params)
                    .await
                    .map_err(Error::from)
            }
        };

        match rv {
            Ok(rows) => {
                conn.set_pending(false);
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, rows: {}, sql: {}",
                    conn.spid(),
                    conn.log_category(),
                    conn.log_db_name(),
                    now.elapsed().as_millis(),
                    rows.len(),
                    sql_preview
                );
                Ok(ResultSet::new(rows))
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
}

pub trait IntoSql<'a> {
    fn into_sql(self) -> Sql<'a>;
}

impl<'a> IntoSql<'a> for Sql<'a> {
    fn into_sql(self) -> Sql<'a> {
        self
    }
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

#[macro_export]
macro_rules! sql_bind {
    ($sql:expr, $($arg:expr),*) => {{
        let mut sql = $crate::IntoSql::into_sql($sql);
        $(sql.bind($arg);)*
        sql
    }};
}

#[macro_export]
macro_rules! sql_format {
    ($fmt:expr, $($arg:expr),*) => {{
        $crate::Sql::new(format!($fmt, $($arg.to_sql_string()),*))
    }};
}

#[macro_export]
macro_rules! sql_ident {
    ($ident:expr) => {
        $crate::SqlIdent::new($ident)
    };
}
