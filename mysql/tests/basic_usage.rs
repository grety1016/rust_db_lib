use mysql::prelude::*;

#[tokio::test]
async fn test_mysql_full_functionality() -> Result<(), mysql::Error> {
    let url = "mysql://root:Kephi520!@127.0.0.1:3306/Salary";
    let pool = mysql::Pool::builder().max_size(10).connect(url).await?;

    let conn = pool.get().await?;

    // 1. 基础 CRUD 测试
    // 清理环境
    conn.exec("DROP TABLE IF EXISTS test_full").await?;
    conn.exec("CREATE TABLE test_full (id INT PRIMARY KEY, name VARCHAR(255), score DECIMAL(10,2), created_at DATETIME)").await?;

    // 插入数据
    let now = chrono::Local::now().naive_local();
    let sql = mysql::sql_bind!(
        "INSERT INTO test_full (id, name, score, created_at) VALUES (?, ?, ?, ?)",
        1,
        "Alice",
        rust_decimal::Decimal::new(955, 1),
        now
    );
    conn.exec(sql).await?;

    // 查询数据
    let sql = mysql::sql_bind!("SELECT * FROM test_full WHERE id = ?", 1);
    let mut rs = conn.query(sql).await?;
    assert_eq!(rs.len(), 1);

    if let Some(row) = rs.fetch() {
        assert_eq!(row.try_get_string("name")?.unwrap(), "Alice");
        assert_eq!(row.try_get_i64("id")?.unwrap(), 1);
        // 验证 Decimal
        let score = row.try_get_decimal("score")?.unwrap();
        assert_eq!(score.to_string(), "95.50");
    }

    // 2. SQL 注入安全性测试
    let malicious_input = "Alice' OR '1'='1";
    let sql = mysql::sql_bind!("SELECT * FROM test_full WHERE name = ?", malicious_input);
    let rs = conn.query(sql).await?;
    // 如果参数绑定正常，应该查不到数据（因为名字不匹配），而不是查出所有数据
    assert_eq!(rs.len(), 0);

    // 3. 标识符转义测试 (sql_ident!)
    let table_name = mysql::sql_ident!("test_full");
    let sql = mysql::sql_format!("SELECT count(*) FROM {}", table_name);
    let mut rs = conn.query(sql).await?;
    let count = rs.scalar_i64().await?.unwrap();
    assert_eq!(count, 1);

    println!("Full functionality and security tests passed!");
    Ok(())
}
