use pgsql::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ComplexRecord {
    id: Uuid,
    name: String,
    age: i32,
    score: f64,
    created_at: NaiveDateTime,
    is_active: bool,
    metadata: String,
}

#[tokio::test]
#[ignore]
async fn test_pgsql_stress_realistic() -> Result<(), pgsql::Error> {
    let conn_str = "postgresql://root:Kephi520!@localhost:5432/Salary";
    let pool = Arc::new(
        pgsql::Pool::builder()
            .max_size(100)
            .connect(conn_str)
            .await?,
    );

    // Schema setup
    {
        let conn = pool.get().await?;
        conn.exec("DROP TABLE IF EXISTS stress_complex").await?;
        conn.exec("CREATE TABLE stress_complex (
            id UUID PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            score DOUBLE PRECISION,
            created_at TIMESTAMP,
            is_active BOOLEAN,
            metadata TEXT
        )").await?;
    }

    let concurrent_tasks = 100;
    let iterations_per_task = 50;
    
    println!("Starting realistic stress test: {} tasks, {} iterations (Total {})", 
        concurrent_tasks, iterations_per_task, concurrent_tasks * iterations_per_task);

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(concurrent_tasks);

    for task_id in 0..concurrent_tasks {
        let pool = Arc::clone(&pool);
        let handle: JoinHandle<Result<(), pgsql::Error>> = tokio::spawn(async move {
            for i in 0..iterations_per_task {
                let conn = pool.get().await?;
                
                let record = ComplexRecord {
                    id: Uuid::new_v4(),
                    name: format!("Task-{}-Iter-{}", task_id, i),
                    age: (task_id * i) as i32 % 100,
                    score: (task_id as f64 * 1.5) + i as f64,
                    created_at: Local::now().naive_local(),
                    is_active: i % 2 == 0,
                    metadata: "{\"info\": \"some large metadata string to simulate realistic payload\", \"version\": \"1.0\"}".repeat(5),
                };

                // 1. Insert
                let sql = sql_bind!(
                    "INSERT INTO stress_complex (id, name, age, score, created_at, is_active, metadata) 
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    record.id, record.name.clone(), record.age, record.score, record.created_at, record.is_active, record.metadata.clone()
                );
                conn.exec(sql).await?;

                // 2. Query back
                let query_sql = sql_bind!("SELECT * FROM stress_complex WHERE id = $1", record.id);
                let fetched: ComplexRecord = conn.query_first(query_sql).await?;
                if fetched.id != record.id {
                    return Err(pgsql::Error::custom(format!("ID mismatch: expected {}, got {}", record.id, fetched.id)));
                }

                // 3. Update
                let update_sql = sql_bind!("UPDATE stress_complex SET score = score + 1 WHERE id = $1", record.id);
                conn.exec(update_sql).await?;

                // 4. Random transaction
                if i % 10 == 0 {
                    let r_id = record.id;
                    let r_name = record.name.clone();
                    let r_age = record.age;
                    let r_score = record.score;
                    let r_created_at = record.created_at;
                    let r_is_active = record.is_active;
                    let r_metadata = record.metadata.clone();

                    conn.scoped_trans(async {
                        conn.exec(sql_bind!("DELETE FROM stress_complex WHERE id = $1", r_id)).await?;
                        // Re-insert in transaction
                        let sql = sql_bind!(
                            "INSERT INTO stress_complex (id, name, age, score, created_at, is_active, metadata) 
                             VALUES ($1, $2, $3, $4, $5, $6, $7)",
                            r_id, r_name, r_age, r_score + 1.0, r_created_at, r_is_active, r_metadata
                        );
                        conn.exec(sql).await?;
                        Ok::<(), pgsql::Error>(())
                    }).await?;
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
    let total_ops = concurrent_tasks * iterations_per_task * 4; 
    println!("Realistic stress test completed in {:.2?}", elapsed);
    println!("Average throughput: {:.2} complex ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    // Final verification
    let conn = pool.get().await?;
    let final_count: i64 = conn.query_scalar_i64("SELECT count(*) FROM stress_complex").await?.unwrap_or(0);
    assert_eq!(final_count, (concurrent_tasks * iterations_per_task) as i64);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_pgsql_concurrency_safety() -> Result<(), pgsql::Error> {
    let conn_str = "postgresql://root:Kephi520!@localhost:5432/Salary";
    let conn = pgsql::Connection::connect(conn_str).await?;
    let conn_arc = Arc::new(conn);
    let c1 = Arc::clone(&conn_arc);
    let c2 = Arc::clone(&conn_arc);

    let h1 = tokio::spawn(async move {
        c1.exec("SELECT pg_sleep(1)").await
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let h2 = tokio::spawn(async move {
        c2.exec("SELECT 1").await
    });

    let res1 = h1.await.unwrap();
    let res2 = h2.await.unwrap();

    assert!(res1.is_ok());
    assert!(res2.is_err());
    let err_str = format!("{:?}", res2.unwrap_err());
    assert!(err_str.contains("PendingError"));

    Ok(())
}
