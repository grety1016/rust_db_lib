use chrono::Local;
use pgsql::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

fn conn_str() -> String {
    std::env::var("PGSQL_DSN")
        .unwrap_or_else(|_| "postgresql://root:Kephi520!@localhost:5432/Salary".to_string())
}

#[tokio::test]
#[ignore]
async fn fair_bench_pgsql() -> Result<(), pgsql::Error> {
    let dsn = conn_str();
    let pool = Arc::new(pgsql::Pool::builder().max_size(100).connect(&dsn).await?);

    {
        let conn = pool.get().await?;
        conn.exec("DROP TABLE IF EXISTS bench_fair").await?;
        conn.exec(
            "CREATE TABLE bench_fair (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMP NOT NULL,
                is_active BOOLEAN NOT NULL,
                metadata TEXT NOT NULL
            )",
        )
        .await?;
    }

    let concurrent_tasks: i64 = std::env::var("BENCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let iterations_per_task: i64 = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let use_txn = std::env::var("BENCH_TXN")
        .ok()
        .map(|v| {
            let v = v.to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);

    println!(
        "Starting fair bench (pgsql): tasks={}, iterations={}, total={}",
        concurrent_tasks,
        iterations_per_task,
        concurrent_tasks * iterations_per_task
    );

    let start = Instant::now();
    let mut handles: Vec<JoinHandle<Result<(), pgsql::Error>>> =
        Vec::with_capacity(concurrent_tasks as usize);

    for task_id in 0..concurrent_tasks {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let base = task_id * iterations_per_task;
            let conn = pool.get().await?;
            if use_txn {
                conn.scoped_trans(async {
                    for i in 0..iterations_per_task {
                        let id = base + i + 1;
                        let name = format!("name-{}-{}", task_id, i);
                        let age = ((task_id + i) % 100) as i32;
                        let score = (task_id as f64 * 1.1) + i as f64;
                        let created_at = Local::now().naive_local();
                        let is_active = i % 2 == 0;
                        let metadata = "x".repeat(512);

                        conn.exec(sql_bind!(
                            "INSERT INTO bench_fair (id, name, age, score, created_at, is_active, metadata)
                             VALUES ($1, $2, $3, $4, $5, $6, $7)",
                            id,
                            name,
                            age,
                            score,
                            created_at,
                            is_active,
                            metadata
                        ))
                        .await?;

                        let _: i64 = conn
                            .query_scalar_i64(sql_bind!(
                                "SELECT id FROM bench_fair WHERE id = $1",
                                id
                            ))
                            .await?
                            .unwrap();

                        conn.exec(sql_bind!(
                            "UPDATE bench_fair SET score = score + 1 WHERE id = $1",
                            id
                        ))
                        .await?;
                    }
                    Ok::<(), pgsql::Error>(())
                })
                .await?;
            } else {
                for i in 0..iterations_per_task {
                    let id = base + i + 1;
                    let name = format!("name-{}-{}", task_id, i);
                    let age = ((task_id + i) % 100) as i32;
                    let score = (task_id as f64 * 1.1) + i as f64;
                    let created_at = Local::now().naive_local();
                    let is_active = i % 2 == 0;
                    let metadata = "x".repeat(512);

                    conn.exec(sql_bind!(
                        "INSERT INTO bench_fair (id, name, age, score, created_at, is_active, metadata)
                         VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        id,
                        name,
                        age,
                        score,
                        created_at,
                        is_active,
                        metadata
                    ))
                    .await?;

                    let _: i64 = conn
                        .query_scalar_i64(sql_bind!("SELECT id FROM bench_fair WHERE id = $1", id))
                        .await?
                        .unwrap();

                    conn.exec(sql_bind!(
                        "UPDATE bench_fair SET score = score + 1 WHERE id = $1",
                        id
                    ))
                    .await?;
                }
            }
            Ok(())
        }));
    }

    for h in handles {
        h.await.unwrap()?;
    }

    let elapsed = start.elapsed();
    let total_rows = concurrent_tasks * iterations_per_task;
    let total_ops = total_rows * 3;

    println!("Fair bench (pgsql) completed in {:.2?}", elapsed);
    println!(
        "Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    let conn = pool.get().await?;
    let count = conn
        .query_scalar_i64("SELECT COUNT(*) FROM bench_fair")
        .await?
        .unwrap();
    assert_eq!(count, total_rows);

    Ok(())
}
