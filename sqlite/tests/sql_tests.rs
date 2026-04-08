use sqlite::prelude::*;

#[tokio::test]
async fn test_sql_bind_macro() -> Result<()> {
    let conn = Connection::connect(":memory:").await?;
    
    conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await?;
    
    // Test macro with multiple arguments (PostgreSQL style $1, $2)
    conn.exec(sql_bind!("INSERT INTO users (name, age) VALUES ($1, $2)", "Alice", 30i64)).await?;
    
    // Test macro with single argument
    let row: Row = conn.query_first_row(sql_bind!("SELECT * FROM users WHERE name = $1", "Alice")).await?.unwrap();
    
    assert_eq!(row.try_get_string("name")?.unwrap(), "Alice");
    assert_eq!(row.try_get_i64("age")?.unwrap(), 30);
    
    Ok(())
}

#[tokio::test]
async fn test_sql_builder() -> Result<()> {
    let conn = Connection::connect(":memory:").await?;
    conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await?;

    let mut sql = Sql::new("INSERT INTO users (name, age) VALUES (?, ?)");
    sql.bind("Bob").bind(25i64);
    conn.exec(sql).await?;

    let row: Row = conn.query_first_row(Sql::new("SELECT name FROM users WHERE age = ?").bind(25i64))
        .await?
        .unwrap();

    assert_eq!(row.try_get_string(0)?.unwrap(), "Bob");
    
    Ok(())
}

#[tokio::test]
async fn test_transactions() -> Result<()> {
    let conn = Connection::connect(":memory:").await?;
    conn.exec("CREATE TABLE accounts (name TEXT, balance INTEGER)").await?;

    // Success transaction
    conn.scoped_trans(|| async {
        conn.exec(sql_bind!("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Alice", 100i64)).await?;
        conn.exec(sql_bind!("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Bob", 50i64)).await?;
        Ok(())
    }).await?;

    let count: i64 = conn.query_scalar_i64("SELECT count(*) FROM accounts").await?.unwrap();
    assert_eq!(count, 2);

    // Rollback transaction
    let _: Result<()> = conn.scoped_trans(|| async {
        conn.exec(sql_bind!("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Charlie", 200i64)).await?;
        Err(Error::Custom("Force rollback".to_string()))
    }).await;

    let count: i64 = conn.query_scalar_i64("SELECT count(*) FROM accounts").await?.unwrap();
    assert_eq!(count, 2); // Charlie should not be there

    Ok(())
}

#[tokio::test]
async fn test_sandbox_trans() -> Result<()> {
    let conn = Connection::connect(":memory:").await?;
    conn.exec("CREATE TABLE sand (v TEXT)").await?;

    // Success in sandbox should still rollback
    conn.sandbox_trans(|| async {
        conn.exec(sql_bind!("INSERT INTO sand (v) VALUES ($1)", "ghost")).await?;
        Ok(())
    }).await?;

    let count: i64 = conn.query_scalar_i64("SELECT count(*) FROM sand").await?.unwrap();
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test]
async fn test_placeholder_conversion() -> Result<()> {
    let conn = Connection::connect(":memory:").await?;
    conn.exec("CREATE TABLE t (v TEXT)").await?;
    
    // Use PG style placeholders in SQLite!
    conn.exec(sql_bind!("INSERT INTO t (v) VALUES ($1)", "hello")).await?;
    
    let v: String = conn.query_scalar_string(sql_bind!("SELECT v FROM t WHERE v = $1", "hello")).await?.unwrap();
    assert_eq!(v, "hello");
    
    Ok(())
}
