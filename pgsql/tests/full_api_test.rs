use pgsql::prelude::*;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug, PartialEq)]
struct TestUser {
    id: i32,
    name: String,
    age: Option<i32>,
}

#[tokio::test]
#[ignore]
async fn test_pgsql_full_api() -> Result<(), pgsql::Error> {
    let conn_str = "postgresql://root:Kephi520!@localhost:5432/Salary";
    let pool = pgsql::Pool::builder()
        .max_size(10)
        .connect(conn_str)
        .await?;

    let conn = pool.get().await?;

    // 1. Basic Connection & Info
    assert!(conn.is_connected().await);
    println!("SPID: {}", conn.spid());
    println!("Current DB: {}", conn.current_db());

    // 2. Database & Object existence
    assert!(conn.db_exists("Salary").await?);

    // 3. Schema setup for testing
    conn.exec("CREATE TABLE IF NOT EXISTS full_api_test (id INT PRIMARY KEY, name VARCHAR(50), age INT)")
        .await?;
    conn.exec("TRUNCATE TABLE full_api_test").await?;
    assert!(conn.object_exists("full_api_test").await?);
    assert!(conn.column_exists("full_api_test", "name").await?);

    // 4. Exec & Exec Timeout
    let rows = conn
        .exec(sql_bind!(
            "INSERT INTO full_api_test (id, name, age) VALUES ($1, $2, $3)",
            1,
            "Alice",
            30
        ))
        .await?;
    assert_eq!(rows, 1);

    let rows = conn
        .exec_timeout(
            sql_bind!(
                "INSERT INTO full_api_test (id, name, age) VALUES ($1, $2, $3)",
                2,
                "Bob",
                25
            ),
            Duration::from_secs(5),
        )
        .await?;
    assert_eq!(rows, 1);

    // 5. Query & ResultSet
    {
        let mut rs = conn
            .query("SELECT id, name, age FROM full_api_test ORDER BY id")
            .await?;
        let row1 = rs.fetch().await?.unwrap();
        assert_eq!(row1.column_count(), 3);
        assert_eq!(row1.try_get_i32(0)?.unwrap(), 1);
        assert_eq!(row1.try_get_string(1)?.unwrap(), "Alice");
    }

    // 6. Query Collect
    let users: Vec<TestUser> = conn
        .query_collect("SELECT id, name, age FROM full_api_test")
        .await?;
    assert_eq!(users.len(), 2);

    let rows: Vec<pgsql::Row> = conn
        .query_collect_row("SELECT * FROM full_api_test")
        .await?;
    assert_eq!(rows.len(), 2);

    // 7. Query First / First Row
    let first_user: TestUser = conn
        .query_first("SELECT id, name, age FROM full_api_test WHERE id = 1")
        .await?;
    assert_eq!(first_user.name, "Alice");

    let first_row = conn
        .query_first_row("SELECT * FROM full_api_test WHERE id = 2")
        .await?
        .unwrap();
    assert_eq!(first_row.try_get_string("name")?.unwrap(), "Bob");

    // 8. Scalars
    assert_eq!(
        conn.query_scalar_i32("SELECT count(*) FROM full_api_test")
            .await?
            .unwrap(),
        2
    );
    assert_eq!(
        conn.query_scalar_string("SELECT name FROM full_api_test WHERE id = 1")
            .await?
            .unwrap(),
        "Alice"
    );
    assert_eq!(
        conn.query_scalar_i16("SELECT CAST(10 AS SMALLINT)")
            .await?
            .unwrap(),
        10
    );
    assert_eq!(
        conn.query_scalar_u8("SELECT CAST(255 AS SMALLINT)")
            .await?
            .unwrap(),
        255
    );

    // 9. Transactions & Nested Transactions (Savepoints)
    conn.scoped_trans(async {
        conn.exec("INSERT INTO full_api_test (id, name, age) VALUES (3, 'Charlie', 40)")
            .await?;

        // Nested transaction
        conn.scoped_trans(async {
            conn.exec("INSERT INTO full_api_test (id, name, age) VALUES (4, 'Dave', 35)")
                .await?;
            Ok::<(), pgsql::Error>(())
        })
        .await?;

        // Test rollback in nested
        let res = conn
            .scoped_trans::<_, ()>(async {
                conn.exec("INSERT INTO full_api_test (id, name, age) VALUES (5, 'Eve', 28)")
                    .await?;
                Err(pgsql::Error::custom("Simulated failure"))
            })
            .await;
        assert!(res.is_err());

        Ok::<(), pgsql::Error>(())
    })
    .await?;

    // Verify results after transactions
    let count = conn
        .query_scalar_i32("SELECT count(*) FROM full_api_test")
        .await?
        .unwrap();
    assert_eq!(count, 4); // 1, 2, 3, 4 (5 was rolled back)

    // 10. Sandbox Transaction (Auto-rollback)
    let res: Result<(), pgsql::Error> = conn
        .sandbox_trans(async {
            conn.exec("INSERT INTO full_api_test (id, name, age) VALUES (6, 'Frank', 50)")
                .await?;
            Ok(())
        })
        .await;
    assert!(res.is_ok());
    let count_after_sandbox = conn
        .query_scalar_i32("SELECT count(*) FROM full_api_test")
        .await?
        .unwrap();
    assert_eq!(count_after_sandbox, 4); // 6 should be rolled back

    // 11. Reconnect
    conn.reconnect().await?;
    assert!(conn.is_connected().await);

    Ok(())
}
