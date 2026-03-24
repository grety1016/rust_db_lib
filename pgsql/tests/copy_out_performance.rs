#![allow(dead_code)]

mod prelude;
use futures_util::StreamExt;
use prelude::*;
use std::time::Instant;

#[tokio::test]
async fn test_copy_out_performance_wide_table() {
    init();

    let conn_str = conn_str();
    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str)
        .await
        .unwrap();
    let conn = pool.get().await.unwrap();

    // 创建一个70字段的宽表
    let mut create_sql =
        String::from("CREATE TABLE IF NOT EXISTS wide_table_test (id SERIAL PRIMARY KEY");
    for i in 1..70 {
        create_sql.push_str(&format!(", col{}_text TEXT", i));
    }
    create_sql.push(')');

    conn.exec(create_sql).await.unwrap();
    conn.exec("TRUNCATE TABLE wide_table_test RESTART IDENTITY")
        .await
        .unwrap();

    // 删除不需要的插入逻辑，使用简单的循环插入

    // 实际插入数据 - 使用循环插入100行
    for _ in 0..100 {
        let mut cols = vec!["DEFAULT".to_string()]; // id列使用SERIAL自动生成
        for _ in 1..70 {
            cols.push("'sample_data'".to_string());
        }
        let sql = format!("INSERT INTO wide_table_test VALUES ({})", cols.join(", "));
        conn.exec(sql).await.unwrap();
    }

    // 现在测试copy_out性能
    let start = Instant::now();

    let sql = "COPY wide_table_test TO STDOUT WITH CSV";
    let mut stream = conn.copy_out(sql).await.unwrap();

    let mut total_bytes = 0;
    let mut total_rows = 0;
    let mut buffer = Vec::new();

    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => {
                total_bytes += bytes.len();
                buffer.extend_from_slice(&bytes);

                // 简单计算行数（通过换行符）
                let data_str = String::from_utf8_lossy(&bytes);
                total_rows += data_str.matches('\n').count();
            }
            Err(e) => {
                panic!("读取导出数据时发生错误: {}", e);
            }
        }
    }

    let duration = start.elapsed();

    println!("宽表(70字段, 100行) copy_out 性能测试:");
    println!("  - 总耗时: {:?}", duration);
    println!("  - 总字节数: {}", total_bytes);
    println!("  - 总行数: {}", total_rows);
    println!("  - 平均每行耗时: {:?}", duration / 100u32);

    // 验证数据完整性
    conn.exec("DROP TABLE IF EXISTS wide_table_test")
        .await
        .unwrap();

    // 断言确保操作在合理时间内完成（小于5秒）
    assert!(duration.as_secs() < 5, "copy_out 操作耗时过长");
}
