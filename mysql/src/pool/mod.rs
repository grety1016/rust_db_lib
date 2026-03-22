use crate::{connection::MySqlExecutor, Error, Result};
use bb8::{Builder, ManageConnection};
use tracing::info;

pub(crate) type RawConnection = mysql_async::Conn;

/// 连接池管理容器
pub struct ConnectionManager {
    opts: mysql_async::Opts,
}

impl ConnectionManager {
    pub fn new(opts: mysql_async::Opts) -> Self {
        Self { opts }
    }

    pub fn build(conn_str: &str) -> Result<Self> {
        use crate::connection::IntoConfig;
        let opts = conn_str.into_config()?;
        Ok(Self::new(opts))
    }
}

#[async_trait::async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = crate::Connection;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection> {
        info!("[mysql] 正在创建新的物理连接...");
        let raw = mysql_async::Conn::new(self.opts.clone())
            .await
            .map_err(Error::from)?;
        let mut conn = crate::Connection::new(raw, self.opts.clone());
        conn.init().await?;
        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<()> {
        conn.exec("SELECT 1").await.map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// 连接池
#[derive(Clone)]
pub struct Pool {
    inner: bb8::Pool<ConnectionManager>,
}

impl Pool {
    pub fn builder() -> PoolBuilder {
        PoolBuilder(Builder::new())
    }

    /// 获取池化连接
    pub async fn get(&self) -> Result<crate::connection::PooledConnection<'_>> {
        self.inner.get().await.map_err(Error::from)
    }
}

pub struct PoolBuilder(Builder<ConnectionManager>);

impl PoolBuilder {
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.0 = self.0.max_size(max_size);
        self
    }

    pub async fn connect(self, conn_str: &str) -> Result<Pool> {
        let manager = ConnectionManager::build(conn_str)?;
        let pool = self
            .0
            .build(manager)
            .await
            .map_err(|e| Error::PoolError(e.to_string()))?;
        Ok(Pool { inner: pool })
    }
}
