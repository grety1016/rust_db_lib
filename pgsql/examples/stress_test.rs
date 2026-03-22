//! PostgreSQL数据库高并发压力测试

use pgsql::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::task;

const NUM_TASKS: usize = 100;
const NUM_OPS_PER_TASK: usize = 50;

#[derive(Clone, Copy, Debug)]
enum WorkloadMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug)]
enum ConnMode {
    PerOp,
    PerTask,
}

fn parse_workload_mode() -> WorkloadMode {
    match std::env::var("STRESS_WORKLOAD")
        .unwrap_or_else(|_| "read_write".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "read_only" | "ro" => WorkloadMode::ReadOnly,
        _ => WorkloadMode::ReadWrite,
    }
}

fn parse_conn_mode() -> ConnMode {
    match std::env::var("STRESS_CONN_MODE")
        .unwrap_or_else(|_| "per_op".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "per_task" | "task" => ConnMode::PerTask,
        _ => ConnMode::PerOp,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let workload = parse_workload_mode();
    let conn_mode = parse_conn_mode();
    let total_ops = NUM_TASKS * NUM_OPS_PER_TASK;
    let total_interactions = match workload {
        WorkloadMode::ReadOnly => total_ops,
        WorkloadMode::ReadWrite => total_ops * 2,
    };

    // 创建连接池
    let pool = Arc::new(
        pgsql::Pool::builder()
            .max_size(NUM_TASKS as u32)
            .connect("postgresql://root:Kephi520!@localhost:5432/Salary")
            .await?,
    );

    // 准备测试表
    let conn = pool.get().await?;
    conn.exec("DROP TABLE IF EXISTS stress_test_users").await?;
    conn.exec(
        "CREATE TABLE stress_test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL
        )",
    )
    .await?;
    conn.exec("CREATE INDEX idx_stress_test_users_email ON stress_test_users(email)")
        .await?;

    if matches!(workload, WorkloadMode::ReadOnly) {
        for i in 0..total_ops {
            let name = format!("user_{}", i);
            let email = format!("user_{}@example.com", i);
            conn.exec(sql_bind!(
                "INSERT INTO stress_test_users (name, email) VALUES ($1, $2)",
                name,
                email
            ))
            .await?;
        }
    }

    let start = Instant::now();
    let mut tasks = Vec::new();

    for i in 0..NUM_TASKS {
        let pool = Arc::clone(&pool);
        let task = task::spawn(async move {
            match conn_mode {
                ConnMode::PerTask => {
                    let conn = pool.get().await.unwrap();
                    for j in 0..NUM_OPS_PER_TASK {
                        let user_id = i * NUM_OPS_PER_TASK + j;
                        let name = format!("user_{}", user_id);
                        let email = format!("user_{}@example.com", user_id);

                        if matches!(workload, WorkloadMode::ReadWrite) {
                            conn.exec(sql_bind!(
                                "INSERT INTO stress_test_users (name, email) VALUES ($1, $2)",
                                name.clone(),
                                email.clone()
                            ))
                            .await
                            .unwrap();
                        }

                        let result: Option<String> = conn
                            .query_scalar_string(sql_bind!(
                                "SELECT name FROM stress_test_users WHERE email = $1",
                                email.clone()
                            ))
                            .await
                            .unwrap();
                        assert_eq!(result, Some(name));
                    }
                }
                ConnMode::PerOp => {
                    for j in 0..NUM_OPS_PER_TASK {
                        let conn = pool.get().await.unwrap();
                        let user_id = i * NUM_OPS_PER_TASK + j;
                        let name = format!("user_{}", user_id);
                        let email = format!("user_{}@example.com", user_id);

                        if matches!(workload, WorkloadMode::ReadWrite) {
                            conn.exec(sql_bind!(
                                "INSERT INTO stress_test_users (name, email) VALUES ($1, $2)",
                                name.clone(),
                                email.clone()
                            ))
                            .await
                            .unwrap();
                        }

                        let result: Option<String> = conn
                            .query_scalar_string(sql_bind!(
                                "SELECT name FROM stress_test_users WHERE email = $1",
                                email.clone()
                            ))
                            .await
                            .unwrap();
                        assert_eq!(result, Some(name));
                    }
                }
            }
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await?;
    }

    let duration = start.elapsed();
    let secs = duration.as_secs_f64();
    println!(
        "压力测试完成: workload={:?}, conn_mode={:?}, tasks={}, ops/task={}, 总 ops={}, 总交互={}",
        workload, conn_mode, NUM_TASKS, NUM_OPS_PER_TASK, total_ops, total_interactions
    );
    println!("总耗时: {:?}", duration);
    println!("业务 ops/sec: {:.2}", total_ops as f64 / secs);
    println!("数据库交互/sec: {:.2}", total_interactions as f64 / secs);

    // 验证数据
    let conn = pool.get().await?;
    let count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM stress_test_users")
        .await?
        .unwrap_or(0);
    assert_eq!(count, total_ops as i64);
    println!("数据验证成功: 数据库中共有 {} 条记录。", count);

    Ok(())
}
