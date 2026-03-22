use crate::connection::IntoConfig;

/// The error container
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Tiberius(#[from] tiberius::error::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error)
}

pub type TcpStreamModifier =
    dyn Fn(&tokio::net::TcpStream) -> tokio::io::Result<()> + Send + Sync + 'static;

/// Implements `bb8::ManageConnection`
pub struct ConnectionManager {
    config: tiberius::Config,
    modify_tcp_stream: Box<TcpStreamModifier>,
}

impl ConnectionManager {
    /// Create a new `ConnectionManager`
    pub fn new(config: tiberius::Config) -> Self {
        Self {
            config,
            modify_tcp_stream: Box::new(|tcp_stream| tcp_stream.set_nodelay(true))
        }
    }

    /// Build a `ConnectionManager` from e.g. an ADO string
    pub fn build<I: IntoConfig>(config: I) -> Result<Self, Error> { Ok(config.into_config().map(Self::new)?) }

    pub fn config(&self) -> tiberius::Config { self.config.clone() }
}

pub mod rt {
    use super::*;

    /// The connection type
    pub type Client = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

    impl ConnectionManager {
        /// Perform some configuration on the TCP stream when generating connections
        pub fn with_modify_tcp_stream<F>(mut self, f: F) -> Self
        where
            F: Fn(&tokio::net::TcpStream) -> tokio::io::Result<()> + Send + Sync + 'static
        {
            self.modify_tcp_stream = Box::new(f);
            self
        }

        pub(crate) async fn connect_inner(&self) -> Result<Client, super::Error> {
            use tiberius::SqlBrowser;
            use tokio::net::TcpStream;
            use tokio_util::compat::TokioAsyncWriteCompatExt; //Tokio02AsyncWriteCompatExt;

            let tcp = TcpStream::connect_named(&self.config).await?;

            (self.modify_tcp_stream)(&tcp)?;

            let client = tiberius::Client::connect(self.config.clone(), tcp.compat_write()).await?;

            Ok(client)
        }
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for ConnectionManager {
    type Connection = rt::Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> { Ok(self.connect_inner().await?) }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool { false }
}
