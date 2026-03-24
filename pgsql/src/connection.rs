use crate::{
    pool::RawConnection,
    prelude::*,
    resultset::ResultSet,
    row::{ColumnData, Row},
    sql::*,
    Error, Result,
};
use ::serde::de::DeserializeOwned;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicI16, AtomicI32, Ordering},
    time,
};
use tokio::sync::Mutex;

/// Implemented for `&str` (connection string) and `tokio_postgres::Config`
pub trait IntoConfig {
    fn into_config(self) -> Result<tokio_postgres::Config>;
}

impl IntoConfig for &str {
    fn into_config(self) -> Result<tokio_postgres::Config> {
        self.parse::<tokio_postgres::Config>()
            .map_err(|e| Error::custom(format!("Failed to parse connection string: {}", e)))
    }
}

impl IntoConfig for &String {
    fn into_config(self) -> Result<tokio_postgres::Config> {
        self.as_str()
            .parse::<tokio_postgres::Config>()
            .map_err(|e| Error::custom(format!("Failed to parse connection string: {}", e)))
    }
}

impl IntoConfig for tokio_postgres::Config {
    fn into_config(self) -> Result<tokio_postgres::Config> {
        Ok(self)
    }
}

/// 数据库连接
pub struct Connection {
    raw: Mutex<RawConnection>,
    /// 连接配置,用于重连
    conn_cfg: tokio_postgres::Config,
    /// 是否正在执行命令
    ///
    /// 临时解决中途取消执行时造成后续执行永远挂起的问题
    pending: AtomicBool,
    /// 是否存在有效的`ResultSet`,防止查询过程中意外执行其他命令
    alive_rs: AtomicBool,
    /// 事务嵌套深度
    trans_depth: AtomicI32,
    /// 会话ID (PostgreSQL doesn't have SPID like MSSQL, we'll use a counter)
    spid: AtomicI16,
    /// 连接获取时的原始当前DB
    orig_db_name: String,
    db_name: String,
    /// 日志分类
    log_category: String,
    /// 日志数据库名
    log_db_name: String,
}

impl Connection {
    /// 创建新连接
    pub async fn connect(conn_cfg: impl IntoConfig) -> Result<Connection> {
        let conn_cfg = conn_cfg.into_config()?;
        let raw = Connection::connect_raw(conn_cfg.clone()).await?;
        let mut conn = Connection::new(raw, conn_cfg);
        conn.init().await?;
        Ok(conn)
    }

    /// 建立原始连接
    async fn connect_raw(cfg: tokio_postgres::Config) -> Result<RawConnection> {
        let (client, connection) = cfg.connect(tokio_postgres::NoTls).await?;

        // Spawn the connection handling task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(client)
    }

    /// 初始化连接（连接获取时）
    pub(crate) async fn init(&mut self) -> Result<()> {
        assert!(self.spid.load(Ordering::SeqCst) == 0);
        self.set_pending(true);
        let conn = self.raw.lock().await;
        // 取当前SPID和数据库名
        let row = conn
            .query_one("SELECT pg_backend_pid(), current_database()", &[])
            .await
            .map_err(Error::from)?;

        let spid: i32 = row.get(0);
        let db_name: String = row.get(1);

        self.spid.store(spid as i16, Ordering::SeqCst);
        self.orig_db_name = db_name.clone();
        self.db_name = db_name.clone();
        self.log_db_name = db_name;

        self.set_pending(false);
        Ok(())
    }

    /// 复用连接
    pub(crate) async fn reuse(&mut self) -> Result<()> {
        self.log_category.clear();
        self.log_db_name = self.orig_db_name.clone();
        self.set_pending(true);
        let conn = self.raw.lock().await;

        // 重置当前事务
        conn.execute("ROLLBACK", &[]).await.ok(); // Ignore error if no transaction

        // 还原当前数据库 (PostgreSQL cannot change DB within a session like MSSQL's USE)
        // So we just ensure we are in a clean state

        self.set_pending(false);
        Ok(())
    }

    /// 内部构造函数
    pub(crate) fn new(raw: RawConnection, conn_cfg: tokio_postgres::Config) -> Self {
        let db_name = conn_cfg.get_dbname().unwrap_or("postgres").to_string();
        Self {
            raw: Mutex::new(raw),
            conn_cfg,
            pending: AtomicBool::new(false),
            alive_rs: AtomicBool::new(false),
            trans_depth: AtomicI32::new(0),
            spid: AtomicI16::new(0),
            orig_db_name: db_name.clone(),
            db_name: db_name.clone(),
            log_category: String::new(),
            log_db_name: db_name,
        }
    }

    /// 重连
    pub async fn reconnect(&self) -> Result<()> {
        if self.alive_rs.load(Ordering::SeqCst) {
            return Err(Error::PendingError);
        }

        let spid = self.spid.load(Ordering::SeqCst);
        info!(
            "#{}> [{}] @{} - reconnecting",
            spid,
            self.log_category(),
            self.log_db_name()
        );

        let raw = Self::connect_raw(self.conn_cfg.clone()).await?;
        let mut conn = self.raw.lock().await;
        *conn = raw;
        drop(conn);

        // We can't call init() because it needs &mut self to update orig_db_name etc.
        // But since we are just reconnecting to the same config, we don't need to update those.
        // We just need to update spid.
        let conn = self.raw.lock().await;
        let row = conn
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(Error::from)?;
        let spid: i32 = row.get(0);
        self.spid.store(spid as i16, Ordering::SeqCst);
        self.set_pending(false);
        Ok(())
    }

    /// 检查连接是否有效
    pub async fn is_connected(&self) -> bool {
        self.exec("SELECT 1").await.is_ok()
    }

    /// 检查连接是否已关闭
    pub fn is_closed(&self) -> bool {
        self.raw.try_lock().map(|g| g.is_closed()).unwrap_or(true)
    }

    /// 获取原始连接的引用
    pub(crate) fn raw_ref(&self) -> &Mutex<RawConnection> {
        &self.raw
    }

    /// 正在执行命令标识
    pub(crate) fn pending_ref(&self) -> &AtomicBool {
        &self.pending
    }

    /// 获取结果集存活标识引用
    pub(crate) fn alive_rs_ref(&self) -> &AtomicBool {
        &self.alive_rs
    }

    /// 设置正在执行命令标识
    pub(crate) fn set_pending(&self, pending: bool) {
        self.pending.store(pending, Ordering::Release);
    }

    /// 检查是否正在执行命令
    pub(crate) fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// 会话ID
    pub fn spid(&self) -> i16 {
        self.spid.load(Ordering::Acquire)
    }

    /// 日志分类
    pub fn log_category(&self) -> &str {
        &self.log_category
    }

    /// 日志数据库名
    pub fn log_db_name(&self) -> &str {
        &self.log_db_name
    }

    pub fn set_log_category(&mut self, cat: &str) {
        self.log_category = cat.to_string();
    }

    pub fn set_log_db_name(&mut self, name: &str) {
        self.log_db_name = name.to_string();
    }

    /// 当前数据库
    pub fn current_db(&self) -> &str {
        &self.db_name
    }

    /// 是否开启事务
    pub fn has_trans(&self) -> bool {
        self.trans_depth.load(Ordering::SeqCst) > 0
    }

    /// 开始事务
    pub async fn begin_trans(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        if depth == 0 {
            self.exec("BEGIN").await?;
        } else {
            self.exec(format!("SAVEPOINT _sp_{}", depth)).await?;
        }
        self.trans_depth.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// 提交事务
    pub async fn commit(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        assert!(depth > 0);
        if depth == 1 {
            self.exec("COMMIT").await?;
        } else {
            // PostgreSQL release savepoint
            self.exec(format!("RELEASE SAVEPOINT _sp_{}", depth - 1))
                .await?;
        }
        self.trans_depth.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// 回滚事务
    pub async fn rollback(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        assert!(depth > 0);
        if depth == 1 {
            self.exec("ROLLBACK").await?;
        } else {
            self.exec(format!("ROLLBACK TO SAVEPOINT _sp_{}", depth - 1))
                .await?;
        }
        self.trans_depth.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// 作用域事务
    pub async fn scoped_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: Future<Output = Result<R>>,
    {
        self.begin_trans().await?;
        match exec.await {
            Ok(rv) => {
                self.commit().await?;
                Ok(rv)
            }
            Err(e) => {
                self.rollback().await?;
                Err(e)
            }
        }
    }

    /// 沙盒事务(自动回滚)
    pub async fn sandbox_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: Future<Output = Result<R>>,
    {
        self.begin_trans().await?;
        match exec.await {
            Ok(rv) => {
                self.rollback().await?;
                Ok(rv)
            }
            Err(e) => {
                self.rollback().await?;
                Err(e)
            }
        }
    }

    /// 执行SQL命令
    pub async fn exec(&self, sql: impl IntoSql<'_>) -> Result<u64> {
        sql.into_sql().exec(self, None).await
    }

    /// 带超时的SQL命令执行
    pub async fn exec_timeout(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<u64> {
        sql.into_sql().exec(self, duration.into()).await
    }

    /// 查询SQL
    pub async fn query<'a>(&'a self, sql: impl IntoSql<'_>) -> Result<ResultSet<'a>> {
        sql.into_sql().query(self, None).await
    }

    /// 带超时的SQL查询
    pub async fn query_timeout<'a>(
        &'a self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<ResultSet<'a>> {
        sql.into_sql().query(self, duration.into()).await
    }

    /// 查询并收集结果
    pub async fn query_collect<R>(&self, sql: impl IntoSql<'_>) -> Result<R>
    where
        R: DeserializeOwned,
    {
        sql.into_sql().query_collect(self, None).await
    }

    /// 带超时的查询并收集结果
    pub async fn query_collect_timeout<R>(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        sql.into_sql().query_collect(self, duration.into()).await
    }

    /// 查询并收集行数据
    pub async fn query_collect_row(&self, sql: impl IntoSql<'_>) -> Result<Vec<Row>> {
        sql.into_sql().query_collect_row(self, None).await
    }

    /// 带超时的查询并收集行数据
    pub async fn query_collect_row_timeout(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<Vec<Row>> {
        sql.into_sql()
            .query_collect_row(self, duration.into())
            .await
    }

    /// 查询第一条记录
    pub async fn query_first<R>(&self, sql: impl IntoSql<'_>) -> Result<R>
    where
        R: DeserializeOwned,
    {
        sql.into_sql().query_first(self, None).await
    }

    /// 查询第一条行数据
    pub async fn query_first_row(&self, sql: impl IntoSql<'_>) -> Result<Option<Row>> {
        sql.into_sql().query_first_row(self, None).await
    }

    /// 查询标量值(字符串)
    pub async fn query_scalar_string(&self, sql: impl IntoSql<'_>) -> Result<Option<String>> {
        sql.into_sql().query_scalar_string(self, None).await
    }

    /// 查询标量值(u8)
    pub async fn query_scalar_u8(&self, sql: impl IntoSql<'_>) -> Result<Option<u8>> {
        sql.into_sql().query_scalar_u8(self, None).await
    }

    /// 查询标量值(i16)
    pub async fn query_scalar_i16(&self, sql: impl IntoSql<'_>) -> Result<Option<i16>> {
        sql.into_sql().query_scalar_i16(self, None).await
    }

    /// 查询标量值(i32)
    pub async fn query_scalar_i32(&self, sql: impl IntoSql<'_>) -> Result<Option<i32>> {
        sql.into_sql().query_scalar_i32(self, None).await
    }

    /// 查询标量值(i64)
    pub async fn query_scalar_i64(&self, sql: impl IntoSql<'_>) -> Result<Option<i64>> {
        sql.into_sql().query_scalar_i64(self, None).await
    }

    /// 查询标量值(f32)
    pub async fn query_scalar_f32(&self, sql: impl IntoSql<'_>) -> Result<Option<f32>> {
        sql.into_sql().query_scalar_f32(self, None).await
    }

    /// 查询标量值(f64)
    pub async fn query_scalar_f64(&self, sql: impl IntoSql<'_>) -> Result<Option<f64>> {
        sql.into_sql().query_scalar_f64(self, None).await
    }

    /// 查询标量值(bool)
    pub async fn query_scalar_bool(&self, sql: impl IntoSql<'_>) -> Result<Option<bool>> {
        sql.into_sql().query_scalar_bool(self, None).await
    }

    /// 查询标量值(Decimal)
    pub async fn query_scalar_dec(&self, sql: impl IntoSql<'_>) -> Result<Option<Decimal>> {
        sql.into_sql().query_scalar_dec(self, None).await
    }

    /// 查询标量值(DateTime)
    pub async fn query_scalar_datetime(
        &self,
        sql: impl IntoSql<'_>,
    ) -> Result<Option<NaiveDateTime>> {
        sql.into_sql().query_scalar_datetime(self, None).await
    }

    /// 查询标量值(Date)
    pub async fn query_scalar_date(&self, sql: impl IntoSql<'_>) -> Result<Option<NaiveDate>> {
        sql.into_sql().query_scalar_date(self, None).await
    }

    /// 查询标量值(Time)
    pub async fn query_scalar_time(&self, sql: impl IntoSql<'_>) -> Result<Option<NaiveTime>> {
        sql.into_sql().query_scalar_time(self, None).await
    }

    /// 查询标量值(UUID)
    pub async fn query_scalar_uuid(&self, sql: impl IntoSql<'_>) -> Result<Option<uuid::Uuid>> {
        sql.into_sql().query_scalar_uuid(self, None).await
    }

    /// 查询标量值(任意类型)
    pub async fn query_scalar_any(&self, sql: impl IntoSql<'_>) -> Result<Option<ColumnData>> {
        sql.into_sql().query_scalar_any(self, None).await
    }

    /// 检查指定的数据库是否存在
    pub async fn db_exists(&self, db_name: &str) -> Result<bool> {
        let sql = sql_bind!("SELECT 1 FROM pg_database WHERE datname = $1", db_name);
        Ok(self.query_scalar_i32(sql).await?.unwrap_or(0) == 1)
    }

    /// 检查对象(表/视图)是否存在
    pub async fn object_exists(&self, obj_name: &str) -> Result<bool> {
        let sql = sql_bind!(
            "SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public'",
            obj_name
        );
        Ok(self.query_scalar_i32(sql).await?.unwrap_or(0) == 1)
    }

    /// 检查指定的表是否存在
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
        self.object_exists(table_name).await
    }

    /// 检查指定表中的列是否存在
    pub async fn column_exists(&self, table_name: &str, column_name: &str) -> Result<bool> {
        let sql = sql_bind!(
            "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2 AND table_schema = 'public'",
            table_name,
            column_name
        );
        Ok(self.query_scalar_i32(sql).await?.unwrap_or(0) == 1)
    }

    /// 获取最后插入的ID (PostgreSQL使用RETURNING子句)
    pub async fn last_identity(&self) -> Result<Option<i64>> {
        // PostgreSQL使用序列，需要查询currval
        self.query_scalar_i64("SELECT lastval()").await
    }

    /// 协议级 copy_in (用于海量数据导入)
    ///
    /// # 参数
    /// - `sql`: COPY 命令, 例如 "COPY table_name (col1, col2) FROM STDIN WITH CSV"
    /// - `stream`: 数据流, 每一项必须实现 `Buf` (如 `Bytes`)
    ///
    /// # 1. HTTP 场景 (从 HTTP API 接收流式数据直接推送到 DB)
    /// ```rust
    /// # use bytes::Bytes;
    /// # use futures_util::stream;
    /// # async fn _example() -> Result<(), pgsql::Error> {
    /// # let conn: pgsql::Connection = unimplemented!();
    /// let sql = "COPY users (name, email) FROM STDIN WITH CSV";
    /// let chunks = vec![Ok::<_, std::io::Error>(Bytes::from("Alice,alice@example.com\n"))];
    /// let stream = stream::iter(chunks);
    /// let count = conn.copy_in(sql, stream).await?;
    /// println!("导入 {} 条记录", count);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # 2. 本地文件场景 (将本地大文件流式推送到 DB)
    /// ```rust
    /// # use bytes::Bytes;
    /// # use futures_util::stream;
    /// # async fn _example() -> Result<(), pgsql::Error> {
    /// # let conn: pgsql::Connection = unimplemented!();
    /// let sql = "COPY large_table FROM STDIN WITH CSV";
    /// let stream = stream::iter(Vec::<std::result::Result<Bytes, std::io::Error>>::new());
    /// let _count = conn.copy_in(sql, stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # 3. 内存流场景 (将内存中的 Vec<u8> 或 String 模拟流推送)
    /// ```rust
    /// # use bytes::Bytes;
    /// # use futures_util::stream;
    /// # async fn _example() -> Result<(), pgsql::Error> {
    /// # let conn: pgsql::Connection = unimplemented!();
    /// let data = "1,Alice\n2,Bob\n";
    /// let chunks = vec![Ok::<_, std::io::Error>(Bytes::from(data))];
    /// let stream = stream::iter(chunks);
    /// let _count = conn.copy_in("COPY users FROM STDIN WITH CSV", stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_in<S, B>(&self, sql: &str, mut stream: S) -> Result<u64>
    where
        S: futures_util::Stream<Item = std::result::Result<B, std::io::Error>> + Unpin + Send,
        B: bytes::Buf + Send + 'static,
    {
        use futures_util::SinkExt;
        use futures_util::StreamExt;

        if self.is_pending() {
            return Err(Error::PendingError);
        }
        self.set_pending(true);
        let now = time::Instant::now();
        info!(
            "#{}> [{}] @{} - copy_in starting, sql: {}",
            self.spid(),
            self.log_category(),
            self.log_db_name(),
            sql
        );

        let raw = self.raw.lock().await;
        let sink = raw.copy_in(sql).await.map_err(Error::from)?;
        futures_util::pin_mut!(sink);

        let res = async {
            while let Some(item) = stream.next().await {
                let buf = item.map_err(|e| Error::custom(format!("Stream error: {}", e)))?;
                sink.send(buf).await.map_err(Error::from)?;
            }
            sink.finish().await.map_err(Error::from)
        }
        .await;

        self.set_pending(false);
        match res {
            Ok(count) => {
                info!(
                    "#{}> [{}] @{} - copy_in finished, elapsed: {}ms, rows: {}, sql: {}",
                    self.spid(),
                    self.log_category(),
                    self.log_db_name(),
                    now.elapsed().as_millis(),
                    count,
                    sql
                );
                Ok(count)
            }
            Err(e) => {
                error!(
                    "#{}> [{}] @{} - copy_in failed, elapsed: {}ms, error: {}, sql: {}",
                    self.spid(),
                    self.log_category(),
                    self.log_db_name(),
                    now.elapsed().as_millis(),
                    e,
                    sql
                );
                Err(e)
            }
        }
    }

    /// 协议级 copy_out (用于海量数据导出)
    ///
    /// # 参数
    /// - `sql`: COPY 命令, 例如 "COPY table_name (col1, col2) TO STDOUT WITH CSV"
    ///
    /// # 返回值
    /// 返回一个字节流，可以从该流中读取从数据库导出的数据
    ///
    /// # 示例
    /// ```rust
    /// # use bytes::Bytes;
    /// # use futures_util::StreamExt;
    /// # async fn _example() -> Result<(), pgsql::Error> {
    /// # let conn: pgsql::Connection = unimplemented!();
    /// let sql = "COPY users (name, email) TO STDOUT WITH CSV";
    /// let mut stream = conn.copy_out(sql).await?;
    ///
    /// // 逐块读取导出的数据
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(bytes) => {
    ///             let data = String::from_utf8_lossy(&bytes);
    ///             println!("接收到数据块: {}", data);
    ///         },
    ///         Err(e) => {
    ///             eprintln!("读取数据时发生错误: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_out(
        &self,
        sql: &str,
    ) -> Result<impl futures_util::Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>>
    {
        use futures_util::TryStreamExt;

        if self.is_pending() {
            return Err(Error::PendingError);
        }
        self.set_pending(true);
        let now = time::Instant::now();
        info!(
            "#{}> [{}] @{} - copy_out starting, sql: {}",
            self.spid(),
            self.log_category(),
            self.log_db_name(),
            sql
        );

        let raw = self.raw.lock().await;
        let stream = raw.copy_out(sql).await.map_err(Error::from)?;

        // 将 tokio_postgres::CopyOutStream 转换为标准的 futures::Stream，同时处理错误类型
        let result_stream = stream.map_err(std::io::Error::other);

        self.set_pending(false);
        info!(
            "#{}> [{}] @{} - copy_out initialized, elapsed: {}ms, sql: {}",
            self.spid(),
            self.log_category(),
            self.log_db_name(),
            now.elapsed().as_millis(),
            sql
        );

        Ok(Box::pin(result_stream))
    }
}
