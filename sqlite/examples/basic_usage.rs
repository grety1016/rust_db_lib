use sqlite::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 连接数据库 (使用本地文件持久化)
    let db_path = "sqlite.db";
    println!("Connecting to local database: {}", db_path);
    let conn = Connection::connect(db_path).await?;

    // 2. 创建表 (使用 IF NOT EXISTS 以支持多次运行)
    conn.exec(
        "CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY, 
            name TEXT, 
            price TEXT, 
            created_at TEXT, 
            uid TEXT, 
            active INTEGER
        )",
    ).await?;

    // 3. 使用 Sql 构建器和 sql_bind! 宏插入数据
    let now = DateTime::from_timestamp(1600000000, 0).unwrap().naive_utc();
    let uid = Uuid::new_v4();
    let price = Decimal::from_f64_retain(99.99).unwrap();

    // 方式 A: 使用 sql_bind! 宏 (对齐 pgsql 手感，支持 $1 占位符)
    conn.exec(sql_bind!(
        "INSERT INTO products (name, price, created_at, uid, active) VALUES ($1, $2, $3, $4, $5)",
        "Rust Book",
        price,
        now,
        uid,
        true
    )).await?;

    // 方式 B: 使用 Sql 构建器
    let mut sql = Sql::new("INSERT INTO products (name, price, created_at, uid, active) VALUES (?, ?, ?, ?, ?)");
    sql.bind("Magic Mouse")
        .bind(Decimal::from_f64_retain(79.0).unwrap())
        .bind(now)
        .bind(Uuid::new_v4())
        .bind(false);
    conn.exec(sql).await?;

    // 4. 查询多行 (对齐 pgsql query_collect_row)
    let products: Vec<Row> = conn.query_collect_row(sql_bind!("SELECT * FROM products WHERE active = $1", true))
        .await?;

    println!("Active products count: {}", products.len());
    for row in products {
        let name: String = row.try_get_string("name")?.unwrap_or_default();
        let price: Decimal = row.try_get_decimal("price")?.unwrap_or_default();
        let uid: Uuid = row.try_get_uuid("uid")?.unwrap_or_default();
        println!("Product: name={}, price={}, uid={}", name, price, uid);
    }

    // 5. 查询标量值 (对齐 pgsql query_scalar_*)
    let price: Decimal = conn.query_scalar_decimal(sql_bind!("SELECT price FROM products WHERE name = $1", "Rust Book"))
        .await?
        .unwrap_or_default();
    
    println!("Price of 'Rust Book': {}", price);

    // 6. 事务处理
    conn.scoped_trans(|| async {
        conn.exec(sql_bind!("UPDATE products SET active = $1 WHERE name = $2", false, "Rust Book")).await?;
        Ok(())
    }).await?;

    Ok(())
}
