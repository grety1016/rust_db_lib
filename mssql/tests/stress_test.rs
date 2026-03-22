use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

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

#[tokio::test]
#[ignore]
async fn test_mssql_stress_realistic() -> Result<(), mssql::Error> {
    let workload = parse_workload_mode();
    let conn_mode = parse_conn_mode();
    let total_ops = NUM_TASKS * NUM_OPS_PER_TASK;
    let total_interactions = match workload {
        WorkloadMode::ReadOnly => total_ops,
        WorkloadMode::ReadWrite => total_ops * 2,
    };

    let conn_str =
        "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true";
    let pool = Arc::new(
        mssql::Pool::builder()
            .max_size(NUM_TASKS as u32)
            .connect(conn_str)
            .await?,
    );

    {
        let conn = pool.get().await?;
        let _ = conn
            .exec("IF OBJECT_ID('stress_test_users', 'U') IS NOT NULL DROP TABLE stress_test_users")
            .await;
        conn.exec(
            "CREATE TABLE stress_test_users (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(255) NOT NULL,
                email NVARCHAR(255) NOT NULL
            )",
        )
        .await?;
        conn.exec("CREATE INDEX idx_stress_test_users_email ON stress_test_users(email)")
            .await?;

        if matches!(workload, WorkloadMode::ReadOnly) {
            for i in 0..total_ops {
                let name = format!("user_{}", i);
                let email = format!("user_{}@example.com", i);
                conn.exec(mssql::sql_bind!(
                    "INSERT INTO stress_test_users (name, email) VALUES (@P1, @P2)",
                    name,
                    email
                ))
                .await?;
            }
        }
    }

    println!(
        "Starting stress test: workload={:?}, conn_mode={:?}, tasks={}, ops/task={}, total_ops={}",
        workload, conn_mode, NUM_TASKS, NUM_OPS_PER_TASK, total_ops
    );

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(NUM_TASKS);

    for task_id in 0..NUM_TASKS {
        let pool = Arc::clone(&pool);
        let handle: JoinHandle<Result<(), mssql::Error>> = tokio::spawn(async move {
            match conn_mode {
                ConnMode::PerTask => {
                    let conn = pool.get().await?;
                    for j in 0..NUM_OPS_PER_TASK {
                        let user_id = task_id * NUM_OPS_PER_TASK + j;
                        let name = format!("user_{}", user_id);
                        let email = format!("user_{}@example.com", user_id);

                        if matches!(workload, WorkloadMode::ReadWrite) {
                            conn.exec(mssql::sql_bind!(
                                "INSERT INTO stress_test_users (name, email) VALUES (@P1, @P2)",
                                name.clone(),
                                email.clone()
                            ))
                            .await?;
                        }

                        let fetched = conn
                            .query_scalar_string(mssql::sql_bind!(
                                "SELECT name FROM stress_test_users WHERE email = @P1",
                                email.clone()
                            ))
                            .await?;
                        if fetched != Some(name) {
                            return Err(mssql::Error::custom("name mismatch"));
                        }
                    }
                }
                ConnMode::PerOp => {
                    for j in 0..NUM_OPS_PER_TASK {
                        let conn = pool.get().await?;
                        let user_id = task_id * NUM_OPS_PER_TASK + j;
                        let name = format!("user_{}", user_id);
                        let email = format!("user_{}@example.com", user_id);

                        if matches!(workload, WorkloadMode::ReadWrite) {
                            conn.exec(mssql::sql_bind!(
                                "INSERT INTO stress_test_users (name, email) VALUES (@P1, @P2)",
                                name.clone(),
                                email.clone()
                            ))
                            .await?;
                        }

                        let fetched = conn
                            .query_scalar_string(mssql::sql_bind!(
                                "SELECT name FROM stress_test_users WHERE email = @P1",
                                email.clone()
                            ))
                            .await?;
                        if fetched != Some(name) {
                            return Err(mssql::Error::custom("name mismatch"));
                        }
                    }
                }
            }
            Ok(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap()?;
    }

    let elapsed = start_time.elapsed();
    println!("Stress test completed in {:.2?}", elapsed);
    println!("业务 ops/sec: {:.2}", total_ops as f64 / elapsed.as_secs_f64());
    println!(
        "数据库交互/sec: {:.2}",
        total_interactions as f64 / elapsed.as_secs_f64()
    );

    let conn = pool.get().await?;
    let final_count: i32 = conn
        .query_scalar_i32("SELECT count(*) FROM stress_test_users")
        .await?
        .unwrap();
    assert_eq!(final_count, total_ops as i32);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_mssql_concurrency_safety() -> Result<(), mssql::Error> {
    let conn_str = "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true";
    // We create a single standalone connection to test concurrency on it
    let conn = mssql::Connection::connect(conn_str).await?;
    let conn_arc = Arc::new(conn);
    let c1 = Arc::clone(&conn_arc);
    let c2 = Arc::clone(&conn_arc);

    let h1 = tokio::spawn(async move { c1.exec("WAITFOR DELAY '00:00:01'; SELECT 1").await });

    // Give h1 a tiny bit of time to start and set the pending flag
    tokio::time::sleep(Duration::from_millis(200)).await;

    let h2 = tokio::spawn(async move { c2.exec("SELECT 2").await });

    let res1 = h1.await.unwrap();
    let res2 = h2.await.unwrap();

    assert!(res1.is_ok());
    // The second call should fail because the first one is still running (WAITFOR)
    assert!(res2.is_err());
    let err_str = format!("{:?}", res2.unwrap_err());
    assert!(err_str.contains("PendingError"));

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_mssql_pool_saturation() -> Result<(), mssql::Error> {
    let conn_str = "server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true";
    let max_pool_size = 3;
    let pool = mssql::Pool::builder().max_size(max_pool_size).connect_timeout(2).connect(conn_str).await?;

    let mut connections = Vec::new();
    for _ in 0..max_pool_size {
        connections.push(pool.get().await?);
    }

    let start = Instant::now();
    let res = pool.get().await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    assert!(elapsed.as_secs() >= 2);

    // Release one connection
    drop(connections.pop());

    // Now we should be able to get one
    let res = pool.get().await;
    assert!(res.is_ok());

    Ok(())
}
