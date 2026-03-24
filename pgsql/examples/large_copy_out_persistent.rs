use futures_util::StreamExt;
use pgsql::Connection;
use std::time::Instant;

#[tokio::main]
async fn main() {
    // 从环境变量获取连接字符串，或者使用默认值
    let conn_str = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://root:Kephi520!@localhost:5432/Salary".to_string());

    // 直接建立连接
    let conn: pgsql::Connection = Connection::connect(&conn_str)
        .await
        .expect("Failed to connect to database");

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
    let total_rows = 1_000_000; // 100万行

    // 准备CSV格式的数据流
    use bytes::Bytes;
    use futures_util::stream;

    // 分批生成数据，每批10000行
    let batch_size = 10_000;
    let mut all_batches = Vec::new();

    for start_idx in (0..total_rows).step_by(batch_size) {
        let end_idx = std::cmp::min(start_idx + batch_size, total_rows);
        let mut csv_data = String::new();

        for i in start_idx..end_idx {
            csv_data.push_str(&format!(
                "'data_{}_1','data_{}_2','data_{}_3','data_{}_4','data_{}_5'\n",
                i, i, i, i, i
            ));
        }

        all_batches.push(Ok::<_, std::io::Error>(Bytes::from(csv_data)));
    }

    let stream = stream::iter(all_batches);

    // 使用copy_in快速插入100万行数据
    println!("开始插入 {} 行测试数据...", total_rows);
    let insert_start = Instant::now();
    let sql = "COPY large_table_test (col1_text, col2_text, col3_text, col4_text, col5_text) FROM STDIN WITH CSV";
    let count: u64 = conn.copy_in(sql, stream).await.unwrap();
    println!(
        "数据插入完成，耗时: {:?}，插入行数: {}",
        insert_start.elapsed(),
        count
    );

    // 验证实际插入了多少行
    let actual_count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM large_table_test")
        .await
        .unwrap()
        .unwrap();
    println!("数据库中实际行数: {}", actual_count);

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

    println!(
        "\n表 large_table_test 已创建并包含 {} 行数据。",
        actual_count
    );
    println!("此表现在保留在数据库中，您可以随时查询它。");
    println!("当您完成检查后，可以手动删除它或重新运行此程序以重新填充数据。");
}
