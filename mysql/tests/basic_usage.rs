use mysql::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_mysql_full_functionality() -> Result<(), mysql::Error> {
    let url = "mysql://root:Kephi520!@127.0.0.1:3306/Salary";
    let pool = tokio::time::timeout(
        Duration::from_secs(3),
        mysql::Pool::builder().max_size(10).connect(url),
    )
    .await
    .map_err(|_| mysql::Error::custom("connect timeout"))??;

    let conn = tokio::time::timeout(Duration::from_secs(3), pool.get())
        .await
        .map_err(|_| mysql::Error::custom("get connection timeout"))??;

    let table = format!("test_full_{}", std::process::id());
    let table_ident = mysql::sql_ident!(table.as_str());

    // 1. 基础 CRUD 测试
    // 清理环境
    tokio::time::timeout(
        Duration::from_secs(5),
        conn.exec(mysql::sql_format!(
            "DROP TEMPORARY TABLE IF EXISTS {}",
            table_ident
        )),
    )
    .await
    .map_err(|_| mysql::Error::custom("drop table timeout"))??;
    tokio::time::timeout(
        Duration::from_secs(5),
        conn.exec(mysql::sql_format!(
            "CREATE TEMPORARY TABLE {} (id INT PRIMARY KEY, name VARCHAR(255), score DECIMAL(10,2), created_at DATETIME)",
            table_ident
        )),
    )
    .await
    .map_err(|_| mysql::Error::custom("create table timeout"))??;

    let sql = mysql::sql_bind!(
        mysql::sql_format!(
            "INSERT INTO {} (id, name, score, created_at) VALUES (?, ?, CAST('95.50' AS DECIMAL(10,2)), NOW())",
            table_ident
        ),
        1,
        "Alice"
    );
    tokio::time::timeout(Duration::from_secs(5), conn.exec(sql))
        .await
        .map_err(|_| mysql::Error::custom("insert timeout"))??;

    // 查询数据
    let sql = mysql::sql_bind!(
        mysql::sql_format!("SELECT * FROM {} WHERE id = ?", table_ident),
        1
    );
    let mut rs = tokio::time::timeout(Duration::from_secs(5), conn.query(sql))
        .await
        .map_err(|_| mysql::Error::custom("query(select by id) timeout"))??;
    if let Some(row) = rs.fetch().await? {
        assert_eq!(row.try_get_string("name")?.unwrap(), "Alice");
        assert_eq!(row.try_get_i64("id")?.unwrap(), 1);
        // 验证 Decimal
        let score = row.try_get_decimal("score")?.unwrap();
        assert_eq!(score.to_string(), "95.50");
    }
    assert!(tokio::time::timeout(Duration::from_secs(5), rs.fetch())
        .await
        .map_err(|_| mysql::Error::custom("fetch timeout"))??
        .is_none());

    // 2. SQL 注入安全性测试
    drop(rs);
    let malicious_input = "Alice' OR '1'='1";
    let sql = mysql::sql_bind!(
        mysql::sql_format!("SELECT * FROM {} WHERE name = ?", table_ident),
        malicious_input
    );
    let mut rs = tokio::time::timeout(Duration::from_secs(5), conn.query(sql))
        .await
        .map_err(|_| mysql::Error::custom("query(sql injection) timeout"))??;
    // 如果参数绑定正常，应该查不到数据（因为名字不匹配），而不是查出所有数据
    assert!(tokio::time::timeout(Duration::from_secs(5), rs.fetch())
        .await
        .map_err(|_| mysql::Error::custom("fetch timeout"))??
        .is_none());
    drop(rs);

    // 3. 标识符转义测试 (sql_ident!)
    let sql = mysql::sql_format!("SELECT count(*) FROM {}", table_ident);
    let mut rs = tokio::time::timeout(Duration::from_secs(5), conn.query(sql))
        .await
        .map_err(|_| mysql::Error::custom("query(count) timeout"))??;
    let count = tokio::time::timeout(Duration::from_secs(5), rs.scalar_i64())
        .await
        .map_err(|_| mysql::Error::custom("scalar timeout"))??
        .unwrap();
    assert_eq!(count, 1);

    println!("Full functionality and security tests passed!");
    Ok(())
}
