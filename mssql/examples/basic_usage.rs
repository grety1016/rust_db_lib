//! MSSQL 数据库基本使用示例

use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 创建连接池
    let url = "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true";
    let pool = mssql::Pool::builder().max_size(10).connect(url).await?;

    // 获取连接
    let conn = pool.get().await?;

    // 1. 执行简单查询
    let result: i32 = conn.query_scalar_i32("SELECT 1").await?.unwrap();
    println!("Query result: {}", result);

    // 2. 使用参数化查询 (防止 SQL 注入)
    let user_id = 1;
    let sql = mssql::sql_bind!("SELECT * FROM users WHERE id = @P1", user_id);
    let users: Vec<User> = conn.query_collect(sql).await?;
    println!("Found {} users", users.len());

    // 3. 使用事务
    conn.scoped_trans(async {
        // 插入数据
        conn.exec("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')").await?;

        // 更新数据
        conn.exec("UPDATE users SET email = 'john.doe@example.com' WHERE name = 'John'").await?;

        Ok::<(), mssql::Error>(())
    })
    .await?;

    Ok(())
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct User {
    id: i32,
    name: String,
    email: String,
}
