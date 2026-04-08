use crate::{sql::IntoSql, ColumnData, Error, Result, ResultSet, Row};
use rusqlite::types::ValueRef;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tokio_rusqlite::Connection as AsyncConnection;

/// SQLite 连接管理器
pub struct Connection {
    path: PathBuf,
    conn: AsyncConnection,
    /// 事务嵌套深度
    trans_depth: AtomicI32,
}

impl Connection {
    /// 连接 SQLite 数据库
    pub async fn connect<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        // 自动创建父目录
        if let Some(parent) = path_buf.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::ConnectionError(format!("Failed to create database directory: {}", e))
                })?;
            }
        }

        let conn = AsyncConnection::open(&path_buf).await?;
        let this = Self {
            path: path_buf,
            conn,
            trans_depth: AtomicI32::new(0),
        };
        this.init().await?;
        Ok(this)
    }

    /// 路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 是否为内存数据库
    pub fn is_memory(&self) -> bool {
        self.path.to_string_lossy() == ":memory:"
    }

    /// 初始化连接，开启 WAL 模式
    async fn init(&self) -> Result<()> {
        self.conn
            .call(|conn| {
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// 是否开启事务
    pub fn has_trans(&self) -> bool {
        self.trans_depth.load(Ordering::SeqCst) > 0
    }

    /// 开始事务 (内部实现)
    pub async fn begin_trans(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        if depth == 0 {
            self.execute_raw("BEGIN TRANSACTION", vec![]).await?;
        } else {
            self.execute_raw(&format!("SAVEPOINT _sp_{}", depth), vec![])
                .await?;
        }
        self.trans_depth.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// 提交事务
    pub async fn commit(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        if depth <= 0 {
            return Err(Error::Custom("No active transaction to commit".to_string()));
        }
        if depth == 1 {
            self.execute_raw("COMMIT", vec![]).await?;
        } else {
            self.execute_raw(&format!("RELEASE SAVEPOINT _sp_{}", depth - 1), vec![])
                .await?;
        }
        self.trans_depth.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// 回滚事务
    pub async fn rollback(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        if depth <= 0 {
            return Err(Error::Custom("No active transaction to rollback".to_string()));
        }
        if depth == 1 {
            self.execute_raw("ROLLBACK", vec![]).await?;
        } else {
            self.execute_raw(&format!("ROLLBACK TO SAVEPOINT _sp_{}", depth - 1), vec![])
                .await?;
        }
        self.trans_depth.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// 作用域事务
    pub async fn scoped_trans<Fut, R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        self.begin_trans().await?;
        match f().await {
            Ok(rv) => {
                self.commit().await?;
                Ok(rv)
            }
            Err(e) => {
                let _ = self.rollback().await;
                Err(e)
            }
        }
    }

    /// 沙盒事务 (自动回滚，常用于测试)
    pub async fn sandbox_trans<Fut, R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        self.begin_trans().await?;
        match f().await {
            Ok(rv) => {
                let _ = self.rollback().await;
                Ok(rv)
            }
            Err(e) => {
                let _ = self.rollback().await;
                Err(e)
            }
        }
    }

    /// 执行 SQL 命令 (对齐 pgsql)
    pub async fn exec(&self, sql: impl IntoSql<'_>) -> Result<u64> {
        sql.into_sql().exec(self, None).await
    }

    /// 带超时的 SQL 命令执行
    pub async fn exec_timeout(&self, sql: impl IntoSql<'_>, duration: Duration) -> Result<u64> {
        sql.into_sql().exec(self, Some(duration)).await
    }

    /// 查询多行 (对齐 pgsql)
    pub async fn query(&self, sql: impl IntoSql<'_>) -> Result<ResultSet> {
        sql.into_sql().query(self, None).await
    }

    /// 查询多行并返回 Vec<Row> (对齐 pgsql query_collect_row)
    pub async fn query_collect_row(&self, sql: impl IntoSql<'_>) -> Result<Vec<Row>> {
        sql.into_sql().query_collect_row(self, None).await
    }

    /// 查询第一条行数据
    pub async fn query_first_row(&self, sql: impl IntoSql<'_>) -> Result<Option<Row>> {
        sql.into_sql().query_first_row(self, None).await
    }

    /// 查询标量值(i64)
    pub async fn query_scalar_i64(&self, sql: impl IntoSql<'_>) -> Result<Option<i64>> {
        sql.into_sql().query_scalar_i64(self, None).await
    }

    /// 查询标量值(字符串)
    pub async fn query_scalar_string(&self, sql: impl IntoSql<'_>) -> Result<Option<String>> {
        sql.into_sql().query_scalar_string(self, None).await
    }

    /// 查询标量值(bool)
    pub async fn query_scalar_bool(&self, sql: impl IntoSql<'_>) -> Result<Option<bool>> {
        sql.into_sql().query_scalar_bool(self, None).await
    }

    /// 查询标量值(Decimal)
    pub async fn query_scalar_decimal(
        &self,
        sql: impl IntoSql<'_>,
    ) -> Result<Option<rust_decimal::Decimal>> {
        sql.into_sql().query_scalar_decimal(self, None).await
    }

    /// 内部执行方法
    pub(crate) async fn execute_raw(&self, sql: &str, params: Vec<ColumnData>) -> Result<usize> {
        let sql = sql.to_string();
        let rows_affected = self
            .conn
            .call(move |conn| {
                let params = to_rusqlite_params(&params);
                Ok(conn.execute(&sql, rusqlite::params_from_iter(params))?)
            })
            .await?;
        Ok(rows_affected)
    }

    /// 内部查询方法
    pub(crate) async fn fetch_all_raw(&self, sql: &str, params: Vec<ColumnData>) -> Result<ResultSet> {
        let sql = sql.to_string();
        let rows = self
            .conn
            .call(move |conn| {
                let params = to_rusqlite_params(&params);
                let mut stmt = conn.prepare(&sql)?;
                let column_names: Vec<String> =
                    stmt.column_names().into_iter().map(|s| s.to_string()).collect();

                let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
                    let mut data = Vec::with_capacity(column_names.len());
                    for i in 0..column_names.len() {
                        let val = row.get_ref(i)?;
                        data.push(map_rusqlite_value(val));
                    }
                    Ok(Row {
                        columns: column_names.clone(),
                        data,
                    })
                })?;

                let mut result = Vec::new();
                for row in rows {
                    result.push(row?);
                }
                Ok(result)
            })
            .await?;
        Ok(ResultSet::new(rows))
    }
}

fn map_rusqlite_value(val: ValueRef) -> ColumnData {
    match val {
        ValueRef::Null => ColumnData::Null,
        ValueRef::Integer(i) => ColumnData::Int(i),
        ValueRef::Real(f) => ColumnData::Float(f),
        ValueRef::Text(s) => ColumnData::String(String::from_utf8_lossy(s).to_string()),
        ValueRef::Blob(b) => ColumnData::Blob(b.to_vec()),
    }
}

fn to_rusqlite_params(params: &[ColumnData]) -> Vec<rusqlite::types::Value> {
    params.iter().map(|p| match p {
        ColumnData::String(s) => rusqlite::types::Value::Text(s.clone()),
        ColumnData::Int(i) => rusqlite::types::Value::Integer(*i),
        ColumnData::Float(f) => rusqlite::types::Value::Real(*f),
        ColumnData::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        ColumnData::Decimal(d) => rusqlite::types::Value::Text(d.to_string()),
        ColumnData::DateTime(dt) => rusqlite::types::Value::Text(dt.to_string()),
        ColumnData::Uuid(u) => rusqlite::types::Value::Text(u.to_string()),
        ColumnData::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        ColumnData::Null => rusqlite::types::Value::Null,
        _ => rusqlite::types::Value::Null,
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_bind;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_basic_ops() -> Result<()> {
        let conn = Connection::connect(":memory:").await?;
        conn.execute_raw(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, price TEXT, created_at TEXT, uid TEXT, data BLOB)",
            vec![],
        )
        .await?;

        let now = chrono::DateTime::from_timestamp(1600000000, 0).unwrap().naive_utc();
        let uid = Uuid::new_v4();
        let price = dec!(123.45);
        let blob = vec![1, 2, 3, 4];

        conn.execute_raw(
            "INSERT INTO test (name, price, created_at, uid, data) VALUES (?, ?, ?, ?, ?)",
            vec![
                "test_item".into(),
                price.into(),
                now.into(),
                uid.into(),
                blob.clone().into(),
            ],
        )
        .await?;

        let row = conn
            .query_first_row(sql_bind!("SELECT * FROM test WHERE name = ?", "test_item"))
            .await?
            .unwrap();

        assert_eq!(row.try_get_string("name")?.unwrap(), "test_item");
        assert_eq!(row.try_get_decimal("price")?.unwrap(), price);
        assert_eq!(row.try_get_datetime("created_at")?.unwrap(), now);
        assert_eq!(row.try_get_uuid("uid")?.unwrap(), uid);
        assert_eq!(row.try_get_blob("data")?.unwrap(), blob);

        Ok(())
    }
}
