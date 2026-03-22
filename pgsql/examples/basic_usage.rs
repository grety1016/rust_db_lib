//! PostgreSQL数据库基本使用示例

use pgsql::prelude::*;
use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 创建连接池
    let url = "postgresql://root:Kephi520!@localhost:5432/Salary";
    let pool = pgsql::Pool::builder().max_size(10).connect(url).await?;

    // 获取连接
    let conn = pool.get().await?;

    // 1. 基本连接信息
    println!("SPID: {}", conn.spid());
    println!("Current DB: {}", conn.current_db());

    // 2. 执行简单查询
    let result: i64 = conn.query_scalar_i64("SELECT 1").await?.unwrap();
    println!("Query 1 result: {}", result);

    // 3. 检查数据库/表是否存在
    if conn.db_exists("Salary").await? {
        println!("Database 'Salary' exists.");
    }

    // 4. 环境准备
    conn.exec("CREATE TABLE IF NOT EXISTS users_example (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))").await?;

    // 5. 使用参数化查询 (防止 SQL 注入)
    let user_name = "InitialUser";
    conn.exec(sql_bind!("INSERT INTO users_example (name, email) VALUES ($1, 'init@example.com')", user_name)).await?;
    
    let sql = sql_bind!("SELECT * FROM users_example WHERE name = $1", user_name);
    let users: Vec<User> = conn.query_collect(sql).await?;
    println!("Found {} users", users.len());

    // 6. 使用事务 (支持嵌套)
    conn.scoped_trans(async {
        // 插入数据
        conn.exec(sql_bind!("INSERT INTO users_example (name, email) VALUES ($1, 'john@example.com')", "John"))
            .await?;

        // 嵌套事务 (使用 SAVEPOINT)
        conn.scoped_trans(async {
            conn.exec(sql_bind!("UPDATE users_example SET email = $1 WHERE name = $2", "john.doe@example.com", "John"))
                .await?;
            Ok::<(), pgsql::Error>(())
        }).await?;

        Ok::<(), pgsql::Error>(())
    })
    .await?;

    // 7. 使用沙盒事务 (测试专用，执行后自动回滚)
    conn.sandbox_trans(async {
        conn.exec(sql_bind!("INSERT INTO users_example (name, email) VALUES ($1, 'sandbox@example.com')", "SandboxUser"))
            .await?;
        Ok::<(), pgsql::Error>(())
    }).await?;

    // 验证沙盒用户不存在
    let count: i64 = conn.query_scalar_i64(sql_bind!("SELECT count(*) FROM users_example WHERE name = $1", "SandboxUser")).await?.unwrap_or(0);
    println!("Sandbox user count (should be 0): {}", count);

    Ok(())
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct User {
    id: i32,
    name: String,
    email: String,
}
