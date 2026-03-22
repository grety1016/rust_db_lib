use crate::{Connection, Result};
use bb8::{Builder, CustomizeConnection, ErrorSink, ManageConnection};
use std::time;
use tokio::time::timeout;

mod wrap;

pub(crate) use wrap::Error;
pub(crate) type RawConnection = <wrap::ConnectionManager as ManageConnection>::Connection;

pub type PooledConnection<'pool> = bb8::PooledConnection<'pool, ConnectionManager>;
pub use bb8::Executor;

/// 连接池
#[derive(Clone)]
pub struct Pool(bb8::Pool<ConnectionManager>);

impl Pool {
    pub fn builder() -> PoolBuilder { PoolBuilder(Default::default()) }
    /// 获取数据库连接
    pub async fn get(&self) -> Result<PooledConnection<'_>> { Ok(self.0.get().await?) }
}

/// 连接池构造器
pub struct PoolBuilder(Builder<ConnectionManager>);

impl PoolBuilder {
    /// 最大连接数
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.0 = self.0.max_size(max_size);
        self
    }
    /// 最小保持空间连接数
    pub fn min_idle(mut self, min_idle: u32) -> Self {
        self.0 = self.0.min_idle(Some(min_idle));
        self
    }
    /// 连接最大空闲时间(sec)
    pub fn idle_timeout(mut self, secs: u64) -> Self {
        self.0 = self.0.idle_timeout(Some(time::Duration::from_secs(secs)));
        self
    }
    /// 连接最大生命期(sec)
    pub fn max_lifetime(mut self, secs: u64) -> Self {
        self.0 = self.0.max_lifetime(Some(time::Duration::from_secs(secs)));
        self
    }
    /// 连接超时(sec)
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.0 = self.0.connection_timeout(time::Duration::from_secs(secs));
        self
    }
    /// 构造连接池并建立连接
    pub async fn connect(self, conn_str: impl AsRef<str>) -> Result<Pool> {
        let mgr = ConnectionManager::build(conn_str.as_ref())?;
        Ok(Pool(
            self.0
                .test_on_check_out(true)
                .error_sink(Box::new(PoolErrorSink))
                .connection_customizer(Box::new(PoolConnectionCustomizer))
                .build(mgr)
                .await?
        ))
    }
    /// 构造连接池并建立连接
    ///
    /// # Executor
    ///
    /// 指定异步运行时解决跨运行时使用连接的问题
    ///
    /// ## 场景步骤:
    ///
    /// 1. 运行时`A`建立连接
    /// 2. 运行时`B`使用连接
    /// 3. 运行时`A`销毁
    /// 4. 运行时`B`使用连接时报错`IO driver has terminated`
    ///
    /// ## 原因
    ///
    /// `bb8`使用`tokio::spawn`后台任务的方式进行连接建立,而`tokio::net::TcpStream`会注册到当前运行时的事件驱动监听器中(`reactor event driver`),
    /// 如果此时建立连接的运行时与使用连接的运行时不同,并且建立连接的运行时早于使用连接的运行时销毁将会触发错误
    pub async fn connect_with_executor(
        self,
        conn_str: impl AsRef<str>,
        executor: impl Executor
    ) -> Result<Pool> {
        let mgr = ConnectionManager::build(conn_str.as_ref())?;
        Ok(Pool(
            self.0
                .test_on_check_out(true)
                .error_sink(Box::new(PoolErrorSink))
                .connection_customizer(Box::new(PoolConnectionCustomizer))
                .build_with_executor(mgr, executor)
                .await?
        ))
    }
    /// 仅构造连接池，不建立连接
    pub fn build(self, conn_str: impl AsRef<str>) -> Result<Pool> {
        let mgr = ConnectionManager::build(conn_str.as_ref())?;
        Ok(Pool(
            self.0
                .test_on_check_out(true)
                .error_sink(Box::new(PoolErrorSink))
                .connection_customizer(Box::new(PoolConnectionCustomizer))
                .build_unchecked(mgr)
        ))
    }
    /// 仅构造连接池，不建立连接
    ///
    /// # Executor
    ///
    /// 指定异步运行时解决跨运行时使用连接的问题
    ///
    /// ## 场景步骤:
    ///
    /// 1. 运行时`A`建立连接
    /// 2. 运行时`B`使用连接
    /// 3. 运行时`A`销毁
    /// 4. 运行时`B`使用连接时报错`IO driver has terminated`
    ///
    /// ## 原因
    ///
    /// `bb8`使用`tokio::spawn`后台任务的方式进行连接建立,而`tokio::net::TcpStream`会注册到当前运行时的事件驱动监听器中(`reactor event driver`),
    /// 如果此时建立连接的运行时与使用连接的运行时不同,并且建立连接的运行时早于使用连接的运行时销毁将会触发错误
    pub fn build_with_executor(self, conn_str: impl AsRef<str>, executor: impl Executor) -> Result<Pool> {
        let mgr = ConnectionManager::build(conn_str.as_ref())?;
        Ok(Pool(
            self.0
                .test_on_check_out(true)
                .error_sink(Box::new(PoolErrorSink))
                .connection_customizer(Box::new(PoolConnectionCustomizer))
                .build_unchecked_with_executor(mgr, executor)
        ))
    }
}

/// 连接池管理容器
pub struct ConnectionManager(wrap::ConnectionManager);

impl ConnectionManager {
    fn build(conn_string: &str) -> Result<ConnectionManager> {
        Ok(ConnectionManager(wrap::ConnectionManager::build(conn_string)?))
    }
}

#[async_trait::async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = crate::Error;

    async fn connect(&self) -> Result<Self::Connection> {
        let conn = self.0.connect().await.map_err(crate::Error::PoolError)?;
        Ok(Connection::new(conn, self.0.config()))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<()> {
        match timeout(time::Duration::from_secs(3), conn.reuse()).await {
            Ok(rv) => rv,
            Err(_) => Err(crate::Error::PoolReuseTimeout)
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if conn.has_trans() {
            //释放时如果事务未处理，则回收连接自动Rollback
            error!("transaction leaked in db pool!");
            true
        } else {
            //FIXME
            conn.is_pending()
        }
    }
}

/// 异步运行时
pub struct TokioRuntimeExecutor(tokio::runtime::Handle);

impl Executor for TokioRuntimeExecutor {
    fn execute(&self, fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>) {
        self.0.spawn(fut);
    }
}

impl From<tokio::runtime::Handle> for TokioRuntimeExecutor {
    fn from(handle: tokio::runtime::Handle) -> Self { TokioRuntimeExecutor(handle) }
}

#[derive(Debug, Clone)]
struct PoolErrorSink;

impl ErrorSink<crate::Error> for PoolErrorSink {
    fn sink(&self, error: crate::Error) {
        error!("db pool error: {}", error);
    }
    fn boxed_clone(&self) -> Box<dyn ErrorSink<crate::Error>> { Box::new(self.clone()) }
}

#[derive(Debug)]
struct PoolConnectionCustomizer;

#[async_trait::async_trait]
impl CustomizeConnection<Connection, crate::Error> for PoolConnectionCustomizer {
    async fn on_acquire(&self, conn: &mut Connection) -> Result<()> {
        match timeout(time::Duration::from_secs(3), conn.init()).await {
            Ok(rv) => rv,
            Err(_) => Err(crate::Error::PoolInitTimeout)
        }
    }
}
