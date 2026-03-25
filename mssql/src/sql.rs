use crate::{Connection, Error, Result, ResultSet};
use std::{borrow::Cow, mem::transmute, time};
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
                            if k == "db_log_sql_params" || k == "db_sql_log_params" || k == "log_sql_params" {
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
    query: tiberius::Query<'a>,
    param_count: usize,
}

impl<'a> Sql<'a> {
    /// 创建参数化SQL
    pub fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        let sql: Cow<'a, str> = sql.into();
        let query = tiberius::Query::new(sql.clone());
        Sql {
            sql,
            query,
            param_count: 0,
        }
    }
    /// 绑定参数 `@PN`
    pub fn bind(&mut self, param: impl tiberius::IntoSql<'a> + 'a) {
        self.query.bind(param);
        self.param_count += 1;
    }
    /// 预览语句
    pub fn preview(&self) -> String {
        lazy_static::lazy_static! {
            static ref RE_SQL_LOG_FMT: regex::Regex = regex::Regex::new(r"[ \t]+").unwrap();
        }
        if log_sql_params_enabled() {
            let sql = format!("{:?}", self.query);
            RE_SQL_LOG_FMT.replace_all(&sql, " ").into_owned()
        } else {
            let sql = RE_SQL_LOG_FMT.replace_all(self.sql.as_ref(), " ").into_owned();
            format!("{sql} [params={}]", self.param_count)
        }
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
    pub(crate) async fn exec(self, conn: &Connection, duration: Option<time::Duration>) -> Result<u64> {
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
        let rv = {
            let mut raw = conn.raw_ref().lock().await;
            if let Some(duration) = duration {
                timeout(duration, self.query.execute(&mut raw))
                    .await
                    .map_err(|_| Error::ExecTimeout)
                    .and_then(|rv| rv.map_err(Error::ExecError))
            } else {
                self.query.execute(&mut raw).await.map_err(Error::ExecError)
            }
        };
        match rv {
            Ok(rs) => {
                conn.set_pending(false);
                //FIXME
                //**BUG**
                //取数组最后一个值
                let effected = *rs.rows_affected().last().unwrap_or(&0); //rv.total();
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
            },
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
                    warn!(
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
                    conn.reconnect().await?;
                }
                Err(e)
            },
        }
    }
    /// 查询语句
    ///
    /// # Parameters
    ///
    /// - **conn** 连接
    /// - **duration** 超时时间
    ///
    /// # Returns
    ///
    /// 返回结果集对象
    pub(crate) async fn query<'con>(
        self,
        conn: &'con Connection,
        duration: Option<time::Duration>,
    ) -> Result<ResultSet<'con>> {
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
        let mut raw = conn.raw_ref().lock().await;
        let rv = if let Some(duration) = duration {
            timeout(duration, self.query.query(&mut *raw))
                .await
                .map_err(|_| Error::QueryTimeout)
                .and_then(|rv| rv.map_err(Error::QueryError))
        } else {
            self.query.query(&mut *raw).await.map_err(Error::QueryError)
        };
        let rv = match rv {
            Ok(rs) => ResultSet::new(rs, conn.alive_rs_ref()).await,
            Err(e) => Err(e),
        };

        match rv {
            Ok(rs) => {
                conn.set_pending(false);
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, sql: {}",
                    conn.spid(),
                    conn.log_category(),
                    conn.log_db_name(),
                    now.elapsed().as_millis(),
                    sql_preview
                );
                Ok(unsafe { transmute::<ResultSet<'_>, ResultSet<'_>>(rs) })
            },
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
                    warn!(
                        "#{}> [{}] @{} - elapsed: {}ms, error: {}, sql: {}",
                        conn.spid(),
                        conn.log_category(),
                        conn.log_db_name(),
                        now.elapsed().as_millis(),
                        e,
                        sql_preview
                    );
                }
                Err(e)
            },
        }
    }
}

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
