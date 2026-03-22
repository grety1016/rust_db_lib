use crate::{pool::RawConnection, row::Row, sql::*, Error, Result};
use ::serde::de::DeserializeOwned;
use bytes::Bytes;
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use mysql_async::prelude::Queryable;
use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time;
use tracing::{error, info};

type InfileStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + 'static>>;

lazy_static::lazy_static! {
    /// 全局 LocalInfile 注册表，用于将文件名映射到异步流
    static ref LOCAL_INFILE_REGISTRY: Arc<Mutex<HashMap<String, InfileStream>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

/// 默认的 LocalInfile 处理器，由驱动在需要 LOCAL INFILE 时调用
pub(crate) struct RegistryInfileHandler;

impl mysql_async::prelude::GlobalHandler for RegistryInfileHandler {
    fn handle(
        &self,
        file_name: &[u8],
    ) -> BoxFuture<'static, std::result::Result<InfileStream, mysql_async::LocalInfileError>> {
        let name = String::from_utf8_lossy(file_name).to_string();
        async move {
            let mut registry = LOCAL_INFILE_REGISTRY.lock().map_err(|_| {
                mysql_async::LocalInfileError::from(std::io::Error::other(
                    "local infile registry poisoned",
                ))
            })?;
            if let Some(stream) = registry.remove(&name) {
                Ok(stream)
            } else {
                Err(mysql_async::LocalInfileError::from(std::io::Error::other(
                    format!("No registered stream for local infile: {}", name),
                )))
            }
        }
        .boxed()
    }
}

pub trait IntoConfig {
    fn into_config(self) -> Result<mysql_async::Opts>;
}

impl IntoConfig for &str {
    fn into_config(self) -> Result<mysql_async::Opts> {
        let opts = mysql_async::Opts::from_url(self)
            .map_err(|e| Error::custom(format!("Invalid connection string: {}", e)))?;
        Ok(mysql_async::OptsBuilder::from_opts(opts)
            .local_infile_handler(Some(RegistryInfileHandler))
            .into())
    }
}

impl IntoConfig for mysql_async::Opts {
    fn into_config(self) -> Result<mysql_async::Opts> {
        Ok(mysql_async::OptsBuilder::from_opts(self)
            .local_infile_handler(Some(RegistryInfileHandler))
            .into())
    }
}

pub enum RawGuard<'a> {
    Owned(tokio::sync::MutexGuard<'a, RawConnection>),
    Mapped(tokio::sync::MappedMutexGuard<'a, RawConnection>),
}

impl<'a> std::ops::Deref for RawGuard<'a> {
    type Target = RawConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            RawGuard::Owned(g) => g,
            RawGuard::Mapped(g) => g,
        }
    }
}

impl<'a> std::ops::DerefMut for RawGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            RawGuard::Owned(g) => g,
            RawGuard::Mapped(g) => g,
        }
    }
}

/// 数据库连接接口 (Trait)
#[async_trait::async_trait]
pub trait MySqlExecutor: Send + Sync {
    async fn lock_raw(&self) -> RawGuard<'_>;
    fn spid(&self) -> u32;
    fn log_category(&self) -> &str;
    fn log_db_name(&self) -> &str;
    fn is_pending(&self) -> bool;
    fn set_pending(&self, pending: bool);
    fn trans_depth(&self) -> u8;
    fn change_trans_depth(&self, delta: i8);
    fn current_db(&self) -> &str;

    /// 重新连接
    async fn reconnect(&self) -> Result<()>;

    /// 检查数据库是否存在
    async fn db_exists(&self, db_name: &str) -> Result<bool> {
        let sql = crate::sql_bind!(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            db_name
        );
        Ok(self.query_scalar_i64(sql).await?.unwrap_or(0) > 0)
    }

    /// 检查对象(表/视图)是否存在
    async fn object_exists(&self, obj_name: &str) -> Result<bool> {
        let sql = crate::sql_bind!("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()", obj_name);
        Ok(self.query_scalar_i64(sql).await?.unwrap_or(0) > 0)
    }

    /// 检查列是否存在
    async fn column_exists(&self, table_name: &str, col_name: &str) -> Result<bool> {
        let sql = crate::sql_bind!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()",
            table_name,
            col_name
        );
        Ok(self.query_scalar_i64(sql).await?.unwrap_or(0) > 0)
    }

    /// 是否已连接
    async fn is_connected(&self) -> bool {
        self.exec("SELECT 1").await.is_ok()
    }

    /// 是否在事务中
    fn has_trans(&self) -> bool {
        self.trans_depth() > 0
    }

    /// 执行 SQL
    async fn exec<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<u64> {
        sql.into_sql().exec_interface(self, None).await
    }

    /// 指定超时时间执行 SQL
    async fn exec_timeout<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
        duration: impl Into<Option<time::Duration>> + Send,
    ) -> Result<u64> {
        sql.into_sql().exec_interface(self, duration.into()).await
    }

    /// 查询 SQL
    async fn query<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<crate::ResultSet<'_>> {
        sql.into_sql().query_interface(self, None).await
    }

    /// 指定超时时间查询 SQL
    async fn query_timeout<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
        duration: impl Into<Option<time::Duration>> + Send,
    ) -> Result<crate::ResultSet<'_>> {
        sql.into_sql().query_interface(self, duration.into()).await
    }

    /// 查询并反序列化为指定类型
    async fn query_collect<'a, R>(&self, sql: impl IntoSql<'a> + Send) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let rs = self.query(sql).await?;
        let rows = rs.collect().await?;
        R::deserialize(crate::serde::RowCollection::new(rows))
            .map_err(|e| Error::Deserialization(e.to_string()))
    }

    /// 指定超时时间查询并反序列化为指定类型
    async fn query_collect_timeout<'a, R>(
        &self,
        sql: impl IntoSql<'a> + Send,
        duration: impl Into<Option<time::Duration>> + Send,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let rs = self.query_timeout(sql, duration).await?;
        let rows = rs.collect().await?;
        R::deserialize(crate::serde::RowCollection::new(rows))
            .map_err(|e| Error::Deserialization(e.to_string()))
    }

    /// 查询并收集行数据
    async fn query_collect_row<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Vec<Row>> {
        let mut rs = self.query(sql).await?;
        rs.collect_row().await
    }

    /// 指定超时时间查询并收集行数据
    async fn query_collect_row_timeout<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
        duration: impl Into<Option<time::Duration>> + Send,
    ) -> Result<Vec<Row>> {
        let mut rs = self.query_timeout(sql, duration).await?;
        rs.collect_row().await
    }

    /// 查询首行并反序列化为指定类型
    async fn query_first<'a, R>(&self, sql: impl IntoSql<'a> + Send) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let mut rs = self.query(sql).await?;
        let row = rs.first_row().await?;
        R::deserialize(crate::serde::RowOptional::new(row))
            .map_err(|e| Error::Deserialization(e.to_string()))
    }

    /// 查询首行
    async fn query_first_row<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<Row>> {
        let mut rs = self.query(sql).await?;
        rs.first_row().await
    }

    /// 查询标量值(字符串)
    async fn query_scalar_string<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<String>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_string().await
    }

    /// 查询标量值(i64)
    async fn query_scalar_i64<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<i64>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_i64().await
    }

    /// 查询标量值(i32)
    async fn query_scalar_i32<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<i32>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_i32().await
    }

    /// 查询标量值(f64)
    async fn query_scalar_f64<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<f64>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_f64().await
    }

    /// 查询标量值(bool)
    async fn query_scalar_bool<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<bool>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_bool().await
    }

    /// 开启事务 (支持嵌套)
    async fn begin_trans(&self) -> Result<()> {
        let depth = self.trans_depth();
        if depth == 0 {
            self.exec("START TRANSACTION").await?;
        } else {
            self.exec(format!("SAVEPOINT _sp_{}", depth)).await?;
        }
        self.change_trans_depth(1);
        Ok(())
    }

    /// 提交事务
    async fn commit(&self) -> Result<()> {
        let depth = self.trans_depth();
        if depth == 0 {
            return Err(Error::custom("No transaction to commit"));
        }
        if depth == 1 {
            self.exec("COMMIT").await?;
        } else {
            self.exec(format!("RELEASE SAVEPOINT _sp_{}", depth - 1))
                .await?;
        }
        self.change_trans_depth(-1);
        Ok(())
    }

    /// 回滚事务
    async fn rollback(&self) -> Result<()> {
        let depth = self.trans_depth();
        if depth == 0 {
            return Err(Error::custom("No transaction to rollback"));
        }
        if depth == 1 {
            self.exec("ROLLBACK").await?;
        } else {
            self.exec(format!("ROLLBACK TO SAVEPOINT _sp_{}", depth - 1))
                .await?;
        }
        self.change_trans_depth(-1);
        Ok(())
    }

    /// 在事务中执行
    async fn scoped_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: std::future::Future<Output = Result<R>> + Send,
        R: Send,
    {
        self.begin_trans().await?;
        match exec.await {
            Ok(rv) => {
                self.commit().await?;
                Ok(rv)
            }
            Err(e) => {
                self.rollback().await.ok();
                Err(e)
            }
        }
    }

    /// 沙盒事务 (执行后自动回滚，常用于测试)
    async fn sandbox_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: std::future::Future<Output = Result<R>> + Send,
        R: Send,
    {
        self.begin_trans().await?;
        let rv = exec.await;
        self.rollback().await.ok();
        rv
    }

    /// 查询标量值(u8)
    async fn query_scalar_u8<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<u8>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_u8().await
    }

    /// 查询标量值(i16)
    async fn query_scalar_i16<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<i16>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_i16().await
    }

    /// 查询标量值(f32)
    async fn query_scalar_f32<'a>(&self, sql: impl IntoSql<'a> + Send) -> Result<Option<f32>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_f32().await
    }

    /// 查询标量值(Decimal)
    async fn query_scalar_dec<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<rust_decimal::Decimal>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_decimal().await
    }

    /// 查询标量值(DateTime)
    async fn query_scalar_datetime<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<chrono::NaiveDateTime>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_datetime().await
    }

    /// 查询标量值(Date)
    async fn query_scalar_date<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<chrono::NaiveDate>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_date().await
    }

    /// 查询标量值(Time)
    async fn query_scalar_time<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<chrono::NaiveTime>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_time().await
    }

    /// 查询标量值(Uuid)
    async fn query_scalar_uuid<'a>(
        &self,
        sql: impl IntoSql<'a> + Send,
    ) -> Result<Option<uuid::Uuid>> {
        let mut rs = self.query(sql).await?;
        rs.scalar_uuid().await
    }

    /// 获取最后插入的 ID (对齐 mssql 返回 Option<i64>)
    async fn last_identity_opt(&self) -> Result<Option<i64>> {
        let mut rs = self.query("SELECT LAST_INSERT_ID()").await?;
        rs.scalar_i64().await
    }

    /// 获取最后插入的 ID
    async fn last_identity(&self) -> Result<u64> {
        let mut rs = self.query("SELECT LAST_INSERT_ID()").await?;
        rs.scalar_i64()
            .await?
            .map(|v| v as u64)
            .ok_or_else(|| Error::custom("Failed to get LAST_INSERT_ID"))
    }

    /// 协议级 copy_in (用于海量数据导入)
    ///
    /// # 参数
    /// - `sql`: LOAD DATA LOCAL INFILE 命令
    /// - `stream`: 数据流, 每一项必须实现 `Buf` (如 `Bytes`)
    async fn copy_in<S, B>(&self, sql: &str, stream: S) -> Result<u64>
    where
        S: futures_util::Stream<Item = std::result::Result<B, std::io::Error>>
            + Unpin
            + Send
            + 'static,
        B: bytes::Buf + Send + 'static,
    {
        if self.is_pending() {
            return Err(Error::PendingError);
        }
        self.set_pending(true);

        // 生成唯一标识作为虚拟文件名
        let file_id = format!("stream_{}", self.spid());

        // 将 Stream 转换为 InfileStream
        let stream = stream.map(|res| res.map(|mut b| b.copy_to_bytes(b.remaining())));
        {
            let mut registry = LOCAL_INFILE_REGISTRY
                .lock()
                .map_err(|_| Error::custom("local infile registry poisoned"))?;
            registry.insert(file_id.clone(), Box::pin(stream));
        }

        // 构造 SQL, 支持 {{STREAM}} 占位符
        let final_sql = sql.replace("{{STREAM}}", &file_id);

        let mut conn = self.lock_raw().await;
        let start = std::time::Instant::now();
        info!(
            "#{}> [{}] @{} - copy_in starting, sql: {}",
            self.spid(),
            "mysql",
            self.log_db_name(),
            final_sql
        );

        let res = conn.query_drop(final_sql).await.map_err(Error::from);

        self.set_pending(false);

        match res {
            Ok(_) => {
                let rows = conn.affected_rows();
                info!(
                    "#{}> [{}] @{} - copy_in finished, elapsed: {}ms, rows: {}, sql: {}",
                    self.spid(),
                    "mysql",
                    self.log_db_name(),
                    start.elapsed().as_millis(),
                    rows,
                    sql
                );
                Ok(rows)
            }
            Err(e) => {
                error!(
                    "#{}> [{}] @{} - copy_in failed, elapsed: {}ms, error: {}, sql: {}",
                    self.spid(),
                    "mysql",
                    self.log_db_name(),
                    start.elapsed().as_millis(),
                    e,
                    sql
                );
                // 确保清理注册表 (虽然 handle 成功会自动清理, 但如果执行失败可能还残留)
                let mut registry = LOCAL_INFILE_REGISTRY
                    .lock()
                    .map_err(|_| Error::custom("local infile registry poisoned"))?;
                registry.remove(&file_id);
                Err(e)
            }
        }
    }
}

/// 数据库连接 (拥有所有权)
pub struct Connection {
    raw: tokio::sync::Mutex<RawConnection>,
    conn_cfg: mysql_async::Opts,
    pending: AtomicBool,
    spid: AtomicU32,
    trans_depth: AtomicU8,
    db_name: String,
    log_category: String,
}

impl Connection {
    /// 创建新连接
    pub async fn connect(conn_cfg: impl IntoConfig) -> Result<Connection> {
        let conn_cfg = conn_cfg.into_config()?;
        let raw = mysql_async::Conn::new(conn_cfg.clone()).await?;
        let mut conn = Connection::new(raw, conn_cfg);
        conn.init().await?;
        Ok(conn)
    }

    pub(crate) fn new(raw: RawConnection, conn_cfg: mysql_async::Opts) -> Self {
        Self {
            raw: tokio::sync::Mutex::new(raw),
            conn_cfg,
            pending: AtomicBool::new(false),
            spid: AtomicU32::new(0),
            trans_depth: AtomicU8::new(0),
            db_name: String::new(),
            log_category: String::new(),
        }
    }

    /// 重新连接
    pub async fn reconnect(&self) -> Result<()> {
        let raw = mysql_async::Conn::new(self.conn_cfg.clone()).await?;
        let mut conn = self.raw.lock().await;
        *conn = raw;
        drop(conn);
        let mut conn = self.raw.lock().await;
        let row: (u32, Option<String>) = conn
            .query_first("SELECT CONNECTION_ID(), DATABASE()")
            .await?
            .unwrap_or((0, None));
        self.spid.store(row.0, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn init(&mut self) -> Result<()> {
        let mut conn = self.raw.lock().await;
        let row: (u32, Option<String>) = conn
            .query_first("SELECT CONNECTION_ID(), DATABASE()")
            .await?
            .unwrap_or((0, None));

        self.spid.store(row.0, Ordering::SeqCst);
        self.db_name = row.1.unwrap_or_else(|| "unknown".to_string());
        Ok(())
    }

    pub fn set_log_category(&mut self, cat: &str) {
        self.log_category = cat.to_string();
    }

    pub fn set_log_db_name(&mut self, name: &str) {
        self.db_name = name.to_string();
    }

    /// 切换数据库
    pub async fn change_db(&mut self, db_name: &str) -> Result<()> {
        self.exec(format!("USE `{}`", db_name)).await?;
        self.db_name = db_name.to_string();
        Ok(())
    }

    /// 恢复到初始数据库
    pub async fn restore_db(&mut self) -> Result<()> {
        let db_name = self.conn_cfg.db_name().unwrap_or("mysql").to_string();
        self.change_db(&db_name).await
    }
}

#[async_trait::async_trait]
impl MySqlExecutor for Connection {
    async fn lock_raw(&self) -> RawGuard<'_> {
        RawGuard::Owned(self.raw.lock().await)
    }
    fn spid(&self) -> u32 {
        self.spid.load(Ordering::SeqCst)
    }
    fn log_category(&self) -> &str {
        &self.log_category
    }
    fn log_db_name(&self) -> &str {
        &self.db_name
    }
    fn is_pending(&self) -> bool {
        self.pending.load(Ordering::SeqCst)
    }
    fn set_pending(&self, pending: bool) {
        self.pending.store(pending, Ordering::SeqCst);
    }
    fn trans_depth(&self) -> u8 {
        self.trans_depth.load(Ordering::SeqCst)
    }
    fn change_trans_depth(&self, delta: i8) {
        if delta > 0 {
            self.trans_depth.fetch_add(delta as u8, Ordering::SeqCst);
        } else {
            self.trans_depth
                .fetch_sub(delta.unsigned_abs(), Ordering::SeqCst);
        }
    }
    fn current_db(&self) -> &str {
        &self.db_name
    }
    async fn reconnect(&self) -> Result<()> {
        Connection::reconnect(self).await
    }
}

pub type PooledConnection<'a> = bb8::PooledConnection<'a, crate::pool::ConnectionManager>;

#[async_trait::async_trait]
impl<'a> MySqlExecutor for bb8::PooledConnection<'a, crate::pool::ConnectionManager> {
    async fn lock_raw(&self) -> RawGuard<'_> {
        self.deref().lock_raw().await
    }
    fn spid(&self) -> u32 {
        self.deref().spid()
    }
    fn log_category(&self) -> &str {
        self.deref().log_category()
    }
    fn log_db_name(&self) -> &str {
        self.deref().log_db_name()
    }
    fn is_pending(&self) -> bool {
        self.deref().is_pending()
    }
    fn set_pending(&self, pending: bool) {
        self.deref().set_pending(pending)
    }
    fn trans_depth(&self) -> u8 {
        self.deref().trans_depth()
    }
    fn change_trans_depth(&self, delta: i8) {
        self.deref().change_trans_depth(delta)
    }
    fn current_db(&self) -> &str {
        self.deref().current_db()
    }
    async fn reconnect(&self) -> Result<()> {
        self.deref().reconnect().await
    }
}
