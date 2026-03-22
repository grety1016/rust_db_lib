use chrono::Local;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

fn conn_str() -> String {
    std::env::var("MSSQL_DSN").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true"
            .to_string()
    })
}

#[tokio::test]
#[ignore]
async fn fair_bench_mssql() -> Result<(), mssql::Error> {
    let dsn = conn_str();
    let pool = Arc::new(mssql::Pool::builder().max_size(100).connect(&dsn).await?);

    {
        let conn = pool.get().await?;
        let _ = conn.exec("IF OBJECT_ID('bench_fair', 'U') IS NOT NULL DROP TABLE bench_fair").await;
        conn.exec(
            "CREATE TABLE bench_fair (
                id BIGINT NOT NULL PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                age INT NOT NULL,
                score FLOAT NOT NULL,
                created_at DATETIME2 NOT NULL,
                is_active BIT NOT NULL,
                metadata NVARCHAR(MAX) NOT NULL
            )",
        )
        .await?;
    }

    let concurrent_tasks: i64 =
        std::env::var("BENCH_CONCURRENCY").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    let iterations_per_task: i64 =
        std::env::var("BENCH_ITERATIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let use_txn = std::env::var("BENCH_TXN")
        .ok()
        .map(|v| {
            let v = v.to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);

    println!(
        "Starting fair bench (mssql): tasks={}, iterations={}, total={}",
        concurrent_tasks,
        iterations_per_task,
        concurrent_tasks * iterations_per_task
    );

    let start = Instant::now();
    let mut handles: Vec<JoinHandle<Result<(), mssql::Error>>> =
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

                        conn.exec(mssql::sql_bind!(
                            "INSERT INTO bench_fair (id, name, age, score, created_at, is_active, metadata)
                             VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7)",
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
                            .query_scalar_i64(mssql::sql_bind!(
                                "SELECT id FROM bench_fair WHERE id = @P1",
                                id
                            ))
                            .await?
                            .unwrap();

                        conn.exec(mssql::sql_bind!(
                            "UPDATE bench_fair SET score = score + 1 WHERE id = @P1",
                            id
                        ))
                        .await?;
                    }
                    Ok::<(), mssql::Error>(())
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

                    conn.exec(mssql::sql_bind!(
                        "INSERT INTO bench_fair (id, name, age, score, created_at, is_active, metadata)
                         VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7)",
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
                        .query_scalar_i64(mssql::sql_bind!("SELECT id FROM bench_fair WHERE id = @P1", id))
                        .await?
                        .unwrap();

                    conn.exec(mssql::sql_bind!("UPDATE bench_fair SET score = score + 1 WHERE id = @P1", id))
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

    println!("Fair bench (mssql) completed in {:.2?}", elapsed);
    println!("Throughput: {:.2} ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    let conn = pool.get().await?;
    let count = conn.query_scalar_i32("SELECT COUNT(*) FROM bench_fair").await?.unwrap();
    assert_eq!(count as i64, total_rows);

    Ok(())
}
