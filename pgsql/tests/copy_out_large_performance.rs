#![allow(dead_code)]

mod prelude;
use futures_util::StreamExt;
use prelude::*;
use std::time::Instant;

#[tokio::test]
async fn test_copy_out_performance_1m_rows() {
    init();

    let conn_str = conn_str();
    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str)
        .await
        .unwrap();
    let conn = pool.get().await.unwrap();

    // 创建一个包含几个字段的表用于测试
    let create_sql = "CREATE TABLE IF NOT EXISTS large_table_test (
        id BIGSERIAL PRIMARY KEY,
        col1_text TEXT,
        col2_text TEXT,
        col3_text TEXT,
        col4_text TEXT,
        col5_text TEXT
    )";

    conn.exec(create_sql).await.unwrap();
    conn.exec("TRUNCATE TABLE large_table_test RESTART IDENTITY")
        .await
        .unwrap();

    // 使用COPY FROM快速插入大量数据
    // 创建测试数据流
    let total_rows = 1_000_000; // 100万行

    // 准备CSV格式的数据流
    use bytes::Bytes;
    use futures_util::stream;

    // 分批生成数据，每批10000行
    let batch_size = 10_000;
    let batches: Vec<_> = (0..total_rows)
        .step_by(batch_size)
        .map(|start_idx| {
            let mut csv_data = String::new();
            for i in start_idx..std::cmp::min(start_idx + batch_size, total_rows) {
                csv_data.push_str(&format!(
                    "{},'data_{}_1','data_{}_2','data_{}_3','data_{}_4','data_{}_5'\n",
                    i, i, i, i, i, i
                ));
            }
            Ok::<_, std::io::Error>(Bytes::from(csv_data))
        })
        .collect();

    let stream = stream::iter(batches);

    // 使用copy_in快速插入100万行数据
    println!("开始插入 {} 行测试数据...", total_rows);
    let insert_start = Instant::now();
    let sql = "COPY large_table_test (id, col1_text, col2_text, col3_text, col4_text, col5_text) FROM STDIN WITH CSV";
    let count = conn.copy_in(sql, stream).await.unwrap();
    println!(
        "数据插入完成，耗时: {:?}，插入行数: {}",
        insert_start.elapsed(),
        count
    );

    // 现在测试copy_out性能 - 导出100万行数据
    println!("开始测试 copy_out 性能...");
    let start = Instant::now();

    let sql = "COPY large_table_test TO STDOUT WITH CSV";
    let mut stream = conn.copy_out(sql).await.unwrap();

    let mut total_bytes = 0;
    let mut total_rows_exported = 0;

    // 逐块读取数据
    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => {
                total_bytes += bytes.len();

                // 计算行数（通过换行符）
                let data_str = String::from_utf8_lossy(&bytes);
                total_rows_exported += data_str.matches('\n').count();
            }
            Err(e) => {
                panic!("读取导出数据时发生错误: {}", e);
            }
        }
    }

    let duration = start.elapsed();

    println!("100万行 copy_out 性能测试结果:");
    println!("  - 总耗时: {:?}", duration);
    println!("  - 总字节数: {}", total_bytes);
    println!("  - 总导出行数: {}", total_rows_exported);
    println!(
        "  - 平均每行耗时: {:?}",
        duration / (total_rows_exported.max(1) as u32)
    );
    println!(
        "  - 吞吐量: {:.2} 行/秒",
        (total_rows_exported as f64) / duration.as_secs_f64()
    );

    // 验证数据完整性
    conn.exec("DROP TABLE IF EXISTS large_table_test")
        .await
        .unwrap();

    // 断言确保操作在合理时间内完成（小于120秒）
    assert!(
        duration.as_secs() < 120,
        "copy_out 100万行操作耗时过长: {:?}",
        duration
    );
}
