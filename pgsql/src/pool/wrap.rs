use crate::connection::IntoConfig;

/// The error container
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Pg(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Implements `bb8::ManageConnection`
pub struct ConnectionManager {
    config: tokio_postgres::Config,
}

impl ConnectionManager {
    /// Create a new `ConnectionManager`
    pub fn new(config: tokio_postgres::Config) -> Self {
        Self { config }
    }

    /// Build a `ConnectionManager` from e.g. a connection string
    pub fn build<I: IntoConfig>(config: I) -> Result<Self, Error> {
        config.into_config().map(Self::new).map_err(|e| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                e.to_string(),
            ))
        })
    }

    pub fn config(&self) -> tokio_postgres::Config {
        self.config.clone()
    }
}

pub mod rt {
    use super::*;

    /// The connection type
    pub type Client = tokio_postgres::Client;

    impl ConnectionManager {
        pub(crate) async fn connect_inner(&self) -> Result<Client, super::Error> {
            use tokio_postgres::NoTls;

            let (client, connection) = self.config.connect(NoTls).await?;

            // Spawn the connection handling task
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            Ok(client)
        }
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for ConnectionManager {
    type Connection = rt::Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(self.connect_inner().await?)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
