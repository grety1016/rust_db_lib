#![allow(dead_code)]

mod prelude;
use prelude::*;
use std::time::Duration;
use tokio::time::sleep;
#[tokio::test]
async fn test_connection_recovery() {
    init();

    let pool = pgsql::Pool::builder()
        .max_size(2)
        .connect("postgresql://root:Kephi520!@localhost:5432/Salary")
        .await
        .unwrap();

    // 模拟数据库重启
    println!("模拟数据库重启，请在10秒内手动重启Docker中的PostgreSQL服务...");
    sleep(Duration::from_secs(10)).await;

    println!("尝试重新获取连接...");
    let conn_result = pool.get().await;
    match conn_result {
        Ok(conn) => {
            println!("成功获取连接！");
            // 使用 i64 读取，测试我们刚才修复的 try_get_i64 (兼容 i32)
            let result: i64 = conn.query_scalar_i64("SELECT 1").await.unwrap().unwrap();
            assert_eq!(result, 1);
            println!("连接恢复测试成功！");
        }
        Err(e) => {
            panic!("获取连接失败: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_nested_transactions() {
    init();

    let pool = pgsql::Pool::builder()
        .max_size(2)
        .connect("postgresql://root:Kephi520!@localhost:5432/Salary")
        .await
        .unwrap();

    let conn = pool.get().await.unwrap();

    conn.exec("DROP TABLE IF EXISTS nested_trans_test")
        .await
        .unwrap();
    conn.exec("CREATE TABLE nested_trans_test (id INT)")
        .await
        .unwrap();

    // 正常提交
    conn.scoped_trans(async {
        conn.exec("INSERT INTO nested_trans_test (id) VALUES (1)")
            .await?;
        conn.scoped_trans(async {
            conn.exec("INSERT INTO nested_trans_test (id) VALUES (2)")
                .await?;
            Ok::<(), pgsql::Error>(())
        })
        .await?;
        Ok::<(), pgsql::Error>(())
    })
    .await
    .unwrap();

    let count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM nested_trans_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 2);

    // 内部回滚
    conn.scoped_trans(async {
        conn.exec("INSERT INTO nested_trans_test (id) VALUES (3)")
            .await?;
        let res: Result<(), _> = conn
            .scoped_trans(async {
                conn.exec("INSERT INTO nested_trans_test (id) VALUES (4)")
                    .await?;
                // 手动返回错误来触发回滚
                Err(pgsql::Error::custom("inner rollback"))
            })
            .await;
        assert!(res.is_err());
        Ok::<(), pgsql::Error>(())
    })
    .await
    .unwrap();

    let count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM nested_trans_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 3, "内部事务回滚后，外部事务应依然可以提交");

    println!("嵌套事务测试成功！");
}
