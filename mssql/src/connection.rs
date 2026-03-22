use crate::{
    Error, Result,
    pool::RawConnection,
    prelude::*,
    resultset::ResultSet,
    row::{ColumnData, Row},
    sql::*,
};
use ::serde::de::DeserializeOwned;
use futures_util::TryFutureExt;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicI16, AtomicI32, Ordering},
    time,
};
use tokio::sync::Mutex;

/// Implemented for `&str` (ADO-style string) and `tiberius::Config`
pub trait IntoConfig {
    fn into_config(self) -> tiberius::Result<tiberius::Config>;
}

impl IntoConfig for &str {
    fn into_config(self) -> tiberius::Result<tiberius::Config> {
        tiberius::Config::from_ado_string(self)
    }
}

impl IntoConfig for &String {
    fn into_config(self) -> tiberius::Result<tiberius::Config> {
        tiberius::Config::from_ado_string(self)
    }
}

impl IntoConfig for tiberius::Config {
    fn into_config(self) -> tiberius::Result<tiberius::Config> {
        Ok(self)
    }
}

/// 数据库连接
pub struct Connection {
    raw: Mutex<RawConnection>,
    /// 连接配置,用于重连
    conn_cfg: tiberius::Config,
    /// 是否正在执行命令
    ///
    /// 临时解决中途取消执行时造成后续执行永远挂起的问题
    ///
    /// issue: https://github.com/prisma/tiberius/issues/300
    pending: AtomicBool,
    /// 是否存在有效的`ResultSet`,防止查询过程中意外执行其他命令
    alive_rs: AtomicBool,
    /// 事务嵌套深度
    trans_depth: AtomicI32,
    /// 会话ID
    spid: AtomicI16,
    /// 连接获取时的原始当前DB
    orig_db_name: String,
    db_name: String,
    /// 日志
    log_category: String,
    log_db_name: String,
}

impl Connection {
    pub(crate) fn new(conn: RawConnection, conn_cfg: tiberius::Config) -> Connection {
        Connection {
            raw: Mutex::new(conn),
            conn_cfg,
            trans_depth: AtomicI32::new(0),
            pending: AtomicBool::new(false),
            alive_rs: AtomicBool::new(false),
            spid: AtomicI16::new(0),
            orig_db_name: "".to_owned(),
            db_name: "".to_owned(),
            log_category: "".to_owned(),
            log_db_name: "".to_owned(),
        }
    }
    /// 建立原始连接
    async fn connect_raw(cfg: tiberius::Config) -> Result<RawConnection> {
        use tiberius::SqlBrowser;
        use tokio::net::TcpStream;
        use tokio_util::compat::TokioAsyncWriteCompatExt;

        let tcp = TcpStream::connect_named(&cfg).await.map_err(Error::ConnectError)?;
        let client =
            tiberius::Client::connect(cfg.clone(), tcp.compat_write()).await.map_err(Error::ConnectError)?;

        Ok(client)
    }
    /// 初始化连接（连接获取时）
    pub(crate) async fn init(&mut self) -> Result<()> {
        assert!(self.spid.load(Ordering::SeqCst) == 0);
        self.set_pending(true);
        let mut conn = self.raw.lock().await;
        //支持'= NULL'
        conn.execute("SET ANSI_NULLS OFF", &[]).await?;
        //取当前数据库与SPID
        let row = conn
            .simple_query("SELECT @@SPID, DB_NAME()")
            .await?
            .into_row()
            .await?
            .ok_or(Error::custom("获取当前DB失败!"))?;
        let spid = row.get::<i16, _>(0).unwrap_or_default();
        if spid == 0 {
            return Err(Error::custom("获取SPID失败!"));
        }
        let db_name = row.get::<&str, _>(1).unwrap_or_default();
        if db_name.is_empty() {
            return Err(Error::custom("获取当前DB失败!"));
        }
        self.spid.store(spid, Ordering::SeqCst);
        self.orig_db_name = db_name.to_owned();
        self.db_name = db_name.to_owned();
        //NOTE 异常情况不需要重置，直接使连接无效
        self.set_pending(false);
        Ok(())
    }
    /// 复用连接
    pub(crate) async fn reuse(&mut self) -> Result<()> {
        self.log_category.clear();
        self.log_db_name.clear();
        self.set_pending(true);
        let mut conn = self.raw.lock().await;
        //NOTE 暂时不支持指令重置
        //issue: https://github.com/prisma/tiberius/issues/299
        //重置当前事务
        conn.execute("IF @@TRANCOUNT > 0 ROLLBACK", &[]).await?;
        //还原当前数据库
        if self.db_name != self.orig_db_name {
            conn.execute(format!("USE {}", self.orig_db_name), &[]).await?;
            self.db_name = self.orig_db_name.clone();
        }
        //NOTE 异常情况不需要重置，直接使连接无效
        self.set_pending(false);
        Ok(())
    }
    /// 原始连接
    pub(crate) fn raw_ref(&self) -> &Mutex<RawConnection> {
        &self.raw
    }
    /// 正在执行命令标识
    pub(crate) fn pending_ref(&self) -> &AtomicBool {
        &self.pending
    }
    /// 存活的`ResultSet`标识
    pub(crate) fn alive_rs_ref(&self) -> &AtomicBool {
        &self.alive_rs
    }
    /// 设置正在执行命令标识
    pub(crate) fn set_pending(&self, pending: bool) {
        self.pending.store(pending, Ordering::SeqCst);
    }
    /// 当前是否有未完成的命令
    pub(crate) fn is_pending(&self) -> bool {
        self.pending.load(Ordering::SeqCst) || self.alive_rs.load(Ordering::SeqCst)
    }
    /// 修改事务嵌套深度
    fn change_trans_depth(&self, by: i32, result: Result<()>) -> Result<()> {
        if result.is_ok() {
            self.trans_depth.fetch_add(by, Ordering::SeqCst);
        }
        result
    }
}

/// 公开接口
impl Connection {
    /// 连接数据库
    pub async fn connect(conn_cfg: impl IntoConfig) -> Result<Connection> {
        let conn_cfg = conn_cfg.into_config()?;
        let raw = Connection::connect_raw(conn_cfg.clone()).await?;
        let mut conn = Connection::new(raw, conn_cfg);
        conn.init().await?;
        Ok(conn)
    }
    /// 重新连接
    ///
    /// # NOTICE
    ///
    /// 重新连接后会丢失当前设置的`SessionContext`
    pub async fn reconnect(&self) -> Result<()> {
        if self.alive_rs.load(Ordering::SeqCst) {
            return Err(Error::PendingError);
        }
        //建立新连接
        let now = time::Instant::now();
        let spid = self.spid.load(Ordering::SeqCst);
        info!("#{}> [{}] @{} - reconnecting", spid, self.log_category(), self.log_db_name());
        let raw_conn = match Self::connect_raw(self.conn_cfg.clone()).await {
            Ok(mut conn) => {
                //更新SPID
                let row = conn
                    .simple_query("SELECT @@SPID")
                    .await?
                    .into_row()
                    .await?
                    .ok_or(Error::custom("获取会话ID失败!"))?;
                self.spid.store(row.get::<i16, _>(0).unwrap_or_default(), Ordering::SeqCst);
                //支持'= NULL'
                conn.execute("SET ANSI_NULLS OFF", &[]).await?;
                //切换当前数据库
                conn.execute(format!("USE {}", self.db_name), &[]).await?;
                info!(
                    "#{}> [{}] @{} - reconnected, new_spid: {}, elapsed: {}ms",
                    spid,
                    self.log_category(),
                    self.log_db_name(),
                    self.spid.load(Ordering::SeqCst),
                    now.elapsed().as_millis()
                );
                conn
            },
            Err(e) => {
                warn!(
                    "#{}> [{}] @{} - reconnect error: {}",
                    spid,
                    self.log_category(),
                    self.log_db_name(),
                    e
                );
                return Err(e);
            },
        };
        //覆盖当前连接
        let mut conn = self.raw.lock().await;
        *conn = raw_conn;
        //重置状态
        self.pending.store(false, Ordering::SeqCst);
        self.trans_depth.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn reconnect_locked(&self, conn: &mut RawConnection) -> Result<()> {
        if self.alive_rs.load(Ordering::SeqCst) {
            return Err(Error::PendingError);
        }
        let now = time::Instant::now();
        let spid = self.spid.load(Ordering::SeqCst);
        info!("#{}> [{}] @{} - reconnecting", spid, self.log_category(), self.log_db_name());
        let mut raw_conn = Self::connect_raw(self.conn_cfg.clone()).await?;
        let row = raw_conn
            .simple_query("SELECT @@SPID")
            .await?
            .into_row()
            .await?
            .ok_or(Error::custom("获取会话ID失败!"))?;
        self.spid
            .store(row.get::<i16, _>(0).unwrap_or_default(), Ordering::SeqCst);
        raw_conn.execute("SET ANSI_NULLS OFF", &[]).await?;
        raw_conn.execute(format!("USE {}", self.db_name), &[]).await?;
        info!(
            "#{}> [{}] @{} - reconnected, new_spid: {}, elapsed: {}ms",
            spid,
            self.log_category(),
            self.log_db_name(),
            self.spid.load(Ordering::SeqCst),
            now.elapsed().as_millis()
        );
        *conn = raw_conn;
        self.pending.store(false, Ordering::SeqCst);
        self.trans_depth.store(0, Ordering::SeqCst);
        Ok(())
    }
    /// 日志大类
    pub fn log_category(&self) -> &str {
        if !self.log_category.is_empty() {
            &self.log_category
        } else {
            "DB"
        }
    }
    pub fn set_log_category(&mut self, cat: &str) {
        self.log_category = cat.to_owned();
    }
    /// 日志输出时的数据库名
    pub fn log_db_name(&self) -> &str {
        if !self.log_db_name.is_empty() {
            &self.log_db_name
        } else {
            &self.db_name
        }
    }
    pub fn set_log_db_name(&mut self, name: &str) {
        self.log_db_name = name.to_owned();
    }

    /// 连接是否有效
    pub async fn is_connected(&self) -> bool {
        self.exec("SELECT 1").await.is_ok()
    }
    /// 当前会话ID
    pub fn spid(&self) -> i16 {
        self.spid.load(Ordering::SeqCst)
    }
    /// 改变当前会话数据库
    pub async fn change_db(&mut self, db_name: &str) -> Result<()> {
        assert!(!db_name.is_empty());
        if self.db_name == db_name {
            return Ok(());
        }
        self.exec(sql_format!("USE {}", sql_ident!(db_name))).await?;
        self.db_name = db_name.to_owned();
        Ok(())
    }
    /// 还原当前会话数据库
    pub async fn restore_db(&mut self) -> Result<()> {
        let db_name = self.orig_db_name.clone();
        self.change_db(&db_name).await
    }
    /// 当前会话数据库名称
    pub fn current_db(&self) -> &str {
        &self.db_name
    }
    /// 判断指定数据库是否存在
    pub async fn db_exists(&self, db_name: &str) -> Result<bool> {
        self.query_scalar_i32(sql_bind!("SELECT 1 WHERE DB_ID(@P1) IS NOT NULL", db_name))
            .map_ok(|v| v.map(|v| v == 1).unwrap_or_default())
            .await
    }
    /// 判断指定对象是否存在
    pub async fn object_exists(&self, obj_name: &str) -> Result<bool> {
        self.query_scalar_i32(sql_bind!("SELECT 1 WHERE OBJECT_ID(@P1) IS NOT NULL", obj_name))
            .map_ok(|v| v.map(|v| v == 1).unwrap_or_default())
            .await
    }
    /// 判断指定表的字段是否存在
    pub async fn column_exists(&self, table_name: &str, col_name: &str) -> Result<bool> {
        self.query_scalar_i32(sql_bind!(
            "SELECT 1 WHERE COL_LENGTH(@P1, @P2) IS NOT NULL",
            table_name,
            col_name
        ))
        .map_ok(|v| v.map(|v| v == 1).unwrap_or_default())
        .await
    }

    /// 是否开启事务
    pub fn has_trans(&self) -> bool {
        self.trans_depth.load(Ordering::SeqCst) > 0
    }
    /// 开启事务
    pub async fn begin_trans(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        self.change_trans_depth(
            1,
            if depth == 0 {
                self.exec_trans_sql("BEGIN TRANSACTION").await
            } else {
                //FIXME
                //MSSQL不支持嵌套事务独立提交，因此采用事务保存点
                //创建保存点（保存点名称可以重复）
                self.exec_trans_sql(&format!("SAVE TRANSACTION _sp_{}", depth)).await
            },
        )
    }
    /// 提交事务
    pub async fn commit(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        assert!(depth > 0);
        self.change_trans_depth(
            -1,
            if depth == 1 {
                self.exec_trans_sql("COMMIT TRANSACTION").await
            } else {
                //FIXME 由于采用事务保存点，因此这里不真正提交
                Ok(())
            },
        )
    }
    /// 回滚事务
    pub async fn rollback(&self) -> Result<()> {
        let depth = self.trans_depth.load(Ordering::SeqCst);
        assert!(depth > 0);
        self.change_trans_depth(
            -1,
            if depth == 1 {
                self.exec_trans_sql("ROLLBACK TRANSACTION").await
            } else {
                //回滚到最近的指定名称的保存点
                self.exec_trans_sql(&format!("ROLLBACK TRANSACTION _sp_{}", depth - 1)).await
            },
        )
    }

    /// 在事务里执行`Future`
    pub async fn scoped_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: Future<Output = Result<R>>,
    {
        self.begin_trans()
            .and_then(|_| {
                exec.and_then(|rv| self.commit().map_ok(|_| rv)).or_else(|e| async {
                    if let Err(e) = self.rollback().await {
                        warn!("rollback failed: {}", e);
                    }
                    Err(e)
                })
            })
            .await
    }
    /// 在事务里执行`Future`并回滚
    pub async fn sandbox_trans<Fut, R>(&self, exec: Fut) -> Result<R>
    where
        Fut: Future<Output = Result<R>>,
    {
        self.begin_trans()
            .and_then(|_| async {
                let rv = exec.await;
                if let Err(e) = self.rollback().await {
                    warn!("rollback failed: {}", e);
                }
                rv
            })
            .await
    }

    /// 执行事务指令
    async fn exec_trans_sql(&self, sql: &str) -> Result<()> {
        if self.is_pending() {
            return Err(Error::PendingError);
        }
        info!(
            "#{}> [{}] @{} - executing, sql: {}",
            self.spid.load(Ordering::SeqCst),
            self.log_category(),
            self.log_db_name(),
            sql
        );
        self.pending.store(true, Ordering::SeqCst);
        let now = time::Instant::now();
        let mut conn = self.raw.lock().await;
        //NOTE tiberius需要使用simple_query接口执行事务命令
        let rv = conn.simple_query(sql).await.map_err(Error::ExecError);
        self.pending.store(false, Ordering::SeqCst);
        match rv {
            Ok(_) => {
                info!(
                    "#{}> [{}] @{} - elapsed: {}ms, sql: {}",
                    self.spid.load(Ordering::SeqCst),
                    self.log_category(),
                    self.log_db_name(),
                    now.elapsed().as_millis(),
                    sql
                );
                Ok(())
            },
            Err(e) => {
                warn!(
                    "#{}> [{}] @{} - elapsed: {}ms,error: {}, sql: {}",
                    self.spid.load(Ordering::SeqCst),
                    self.log_category(),
                    self.log_db_name(),
                    now.elapsed().as_millis(),
                    e,
                    sql
                );
                Err(e)
            },
        }
    }
    /// 执行SQL
    ///
    /// 返回影响的行数
    #[inline]
    pub async fn exec(&self, sql: impl IntoSql<'_>) -> Result<u64> {
        sql.into_sql().exec(self, None).await
    }
    /// 指定超时时间执行SQL
    ///
    /// 返回影响的行数
    pub async fn exec_timeout(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<u64> {
        sql.into_sql().exec(self, duration.into()).await
    }

    /// 查询SQL
    ///
    /// # Returns
    ///
    /// 返回结果集游标对象
    ///
    /// NOTE 结果集对象存活期间不能使用此连接进行任何DB操作
    pub async fn query<'a>(&'a self, sql: impl IntoSql<'_>) -> Result<ResultSet<'a>> {
        sql.into_sql().query(self, None).await
    }
    /// 指定超时时间查询SQL
    ///
    /// # Returns
    ///
    /// 返回结果集游标对象
    ///
    /// NOTE 结果集对象存活期间不能使用此连接进行任何DB操作
    pub async fn query_timeout<'a>(
        &'a self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<ResultSet<'a>> {
        sql.into_sql().query(self, duration.into()).await
    }

    /// 查询
    ///
    /// # Parameters
    ///
    /// - **sql** SQL语句
    ///
    /// # Returns
    ///
    /// 反序列化为指定类型
    #[inline]
    pub async fn query_collect<R>(&self, sql: impl IntoSql<'_>) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let rs = sql.into_sql().query(self, None).await?;
        let rows = rs.collect().await?;
        Ok(R::deserialize(crate::serde::RowCollection::new(rows))?)
    }
    /// 查询
    ///
    /// # Parameters
    ///
    /// - **sql** SQL语句
    /// - **duration** 超时时间
    ///
    /// # Returns
    ///
    /// 反序列化为指定类型
    #[inline]
    pub async fn query_collect_timeout<R>(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let rs = sql.into_sql().query(self, duration.into()).await?;
        let rows = rs.collect().await?;
        Ok(R::deserialize(crate::serde::RowCollection::new(rows))?)
    }
    /// 查询
    ///
    /// # Parameters
    ///
    /// - **sql** SQL语句
    ///
    /// # Returns
    ///
    /// 返回结果集行
    #[inline]
    pub async fn query_collect_row(&self, sql: impl IntoSql<'_>) -> Result<Vec<Row>> {
        let rs = sql.into_sql().query(self, None).await?;
        rs.collect().await
    }
    /// 查询
    ///
    /// # Parameters
    ///
    /// - **sql** SQL语句
    /// - **duration** 超时时间
    ///
    /// # Returns
    ///
    /// 返回结果集行
    #[inline]
    pub async fn query_collect_row_timeout(
        &self,
        sql: impl IntoSql<'_>,
        duration: impl Into<Option<time::Duration>>,
    ) -> Result<Vec<Row>> {
        let rs = sql.into_sql().query(self, duration.into()).await?;
        rs.collect().await
    }

    /// 查询首行并反序列化为指定类型
    #[inline]
    pub async fn query_first<R>(&self, sql: impl IntoSql<'_>) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let rs = sql.into_sql().query(self, None).await?;
        let row = rs.first_row().await?;
        Ok(R::deserialize(crate::serde::RowOptional::new(row))?)
    }
    /// 查询首行
    #[inline]
    pub async fn query_first_row(&self, sql: impl IntoSql<'_>) -> Result<Option<Row>> {
        let rs = sql.into_sql().query(self, None).await?;
        rs.first_row().await
    }

    /// 查询SQL
    ///
    /// 返回首行首列的值
    #[inline]
    pub async fn query_scalar_string(&self, sql: impl IntoSql<'_>) -> Result<Option<String>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_str(0).map(|v| v.map(|v| v.to_owned())),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_u8(&self, sql: impl IntoSql<'_>) -> Result<Option<u8>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_u8(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_i16(&self, sql: impl IntoSql<'_>) -> Result<Option<i16>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_i16(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_i32(&self, sql: impl IntoSql<'_>) -> Result<Option<i32>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_i32(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_i64(&self, sql: impl IntoSql<'_>) -> Result<Option<i64>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_i64(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_f32(&self, sql: impl IntoSql<'_>) -> Result<Option<f32>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_f32(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_f64(&self, sql: impl IntoSql<'_>) -> Result<Option<f64>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_f64(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_dec(&self, sql: impl IntoSql<'_>) -> Result<Option<Decimal>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_dec(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_datetime(&self, sql: impl IntoSql<'_>) -> Result<Option<NaiveDateTime>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_datetime(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_date(&self, sql: impl IntoSql<'_>) -> Result<Option<NaiveDate>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_date(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_time(&self, sql: impl IntoSql<'_>) -> Result<Option<NaiveTime>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_time(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_uuid(&self, sql: impl IntoSql<'_>) -> Result<Option<Uuid>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_uuid(0),
            None => Ok(None),
        }
    }
    #[inline]
    pub async fn query_scalar_any(&self, sql: impl IntoSql<'_>) -> Result<Option<ColumnData>> {
        match self.query_first_row(sql).await? {
            Some(row) => row.try_get_any(0),
            None => Ok(None),
        }
    }
    /// 获取最近一条`INSERT`/`SELECT INTO`语句产生的`ID`值
    #[inline]
    pub async fn last_identity(&self) -> Result<Option<i64>> {
        //FIXME Numeric类型
        self.query_scalar_dec("SELECT @@IDENTITY").await.map(|v| v.and_then(|v| v.to_i64()))
    }
}
