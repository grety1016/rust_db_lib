use std::time::Duration;

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn preflight_timeout() -> Duration {
    std::env::var("DB_PREFLIGHT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(5))
}

fn preflight_iters() -> usize {
    std::env::var("DB_PREFLIGHT_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(20)
}

#[tokio::test]
#[ignore]
async fn preflight_mysql() -> Result<(), mysql::Error> {
    use mysql::prelude::*;

    let timeout = preflight_timeout();
    let iters = preflight_iters();
    let url = env_or_default("MYSQL_URL", "mysql://root:Kephi520!@127.0.0.1:3306/Salary");

    let pool = tokio::time::timeout(timeout, mysql::Pool::builder().max_size(10).connect(&url))
        .await
        .map_err(|_| mysql::Error::custom("preflight: mysql connect timeout"))??;
    let conn = tokio::time::timeout(timeout, pool.get())
        .await
        .map_err(|_| mysql::Error::custom("preflight: mysql pool.get timeout"))??;

    let table = format!("preflight_tmp_{}", std::process::id());
    let table_ident = mysql::sql_ident!(table.as_str());
    conn.exec(mysql::sql_format!(
        "CREATE TEMPORARY TABLE {} (id INT PRIMARY KEY, name VARCHAR(50))",
        table_ident
    ))
    .await?;
    conn.exec(mysql::sql_bind!(
        mysql::sql_format!("INSERT INTO {} (id, name) VALUES (?, ?)", table_ident),
        1,
        "ok"
    ))
    .await?;

    let mut rs = conn
        .query(mysql::sql_format!(
            "SELECT id, name FROM {} ORDER BY id",
            table_ident
        ))
        .await?;
    let row = rs.fetch().await?.unwrap();
    assert_eq!(row.try_get_i32(0)?.unwrap(), 1);
    assert_eq!(row.try_get_string(1)?.unwrap(), "ok");
    drop(rs);

    for _ in 0..iters {
        let _ = conn.query_scalar_i32("SELECT 1").await?.unwrap();
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn preflight_pgsql() -> Result<(), pgsql::Error> {
    use pgsql::prelude::*;

    let timeout = preflight_timeout();
    let iters = preflight_iters();
    let url = env_or_default(
        "PGSQL_URL",
        "postgresql://root:Kephi520!@localhost:5432/Salary",
    );

    let pool = tokio::time::timeout(timeout, pgsql::Pool::builder().max_size(10).connect(&url))
        .await
        .map_err(|_| pgsql::Error::custom("preflight: pgsql connect timeout"))??;
    let conn = tokio::time::timeout(timeout, pool.get())
        .await
        .map_err(|_| pgsql::Error::custom("preflight: pgsql pool.get timeout"))??;

    conn.exec("CREATE TEMP TABLE IF NOT EXISTS preflight_tmp (id INT PRIMARY KEY, name TEXT)")
        .await?;
    conn.exec("TRUNCATE TABLE preflight_tmp").await?;
    conn.exec(sql_bind!(
        "INSERT INTO preflight_tmp (id, name) VALUES ($1, $2)",
        1,
        "ok"
    ))
    .await?;

    let mut rs = conn
        .query("SELECT id, name FROM preflight_tmp ORDER BY id")
        .await?;
    let row = rs.fetch().await?.unwrap();
    assert_eq!(row.try_get_i32(0)?.unwrap(), 1);
    assert_eq!(row.try_get_string(1)?.unwrap(), "ok");
    drop(rs);

    for _ in 0..iters {
        let _ = conn.query_scalar_i32("SELECT 1").await?.unwrap();
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn preflight_mssql() -> Result<(), mssql::Error> {
    use mssql::prelude::*;

    let timeout = preflight_timeout();
    let iters = preflight_iters();
    let url = env_or_default(
        "MSSQL_URL",
        "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true",
    );

    let pool = tokio::time::timeout(timeout, mssql::Pool::builder().max_size(10).connect(&url))
        .await
        .map_err(|_| mssql::Error::custom("preflight: mssql connect timeout"))??;
    let conn = tokio::time::timeout(timeout, pool.get())
        .await
        .map_err(|_| mssql::Error::custom("preflight: mssql pool.get timeout"))??;

    let table = format!("preflight_tmp_{}", std::process::id());
    let table_ident = mssql::sql_ident!(table);

    conn.exec(mssql::sql_format!("DROP TABLE IF EXISTS {}", table_ident))
        .await?;
    conn.exec(mssql::sql_format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name NVARCHAR(50))",
        table_ident
    ))
    .await?;
    conn.exec(sql_bind!(
        mssql::sql_format!("INSERT INTO {} (id, name) VALUES (@P1, @P2)", table_ident),
        1,
        "ok"
    ))
    .await?;

    let mut rs = conn
        .query(mssql::sql_format!(
            "SELECT id, name FROM {} ORDER BY id",
            table_ident
        ))
        .await?;
    let row = rs.fetch().await?.unwrap();
    assert_eq!(row.try_get_i32(0)?.unwrap(), 1);
    assert_eq!(row.try_get_str(1)?.unwrap(), "ok");
    drop(rs);

    for _ in 0..iters {
        let _ = conn.query_scalar_i32("SELECT 1").await?.unwrap();
    }

    let _ = conn
        .exec(mssql::sql_format!("DROP TABLE IF EXISTS {}", table_ident))
        .await;
    Ok(())
}
