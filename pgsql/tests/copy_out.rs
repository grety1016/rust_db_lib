#![allow(dead_code)]

mod prelude;
use futures_util::StreamExt;
use prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_copy_out_basic() {
    init();

    let conn_str = conn_str();
    let pool = pgsql::Pool::builder().max_size(1).connect(&conn_str);
    let pool = tokio::time::timeout(Duration::from_secs(3), pool)
        .await
        .expect("connect timeout")
        .unwrap();

    let conn = tokio::time::timeout(Duration::from_secs(3), pool.get())
        .await
        .expect("get connection timeout")
        .unwrap();

    // 1. 准备测试数据
    conn.exec("CREATE TABLE IF NOT EXISTS test_copy_out_table (id INT, name TEXT)")
        .await
        .unwrap();
    conn.exec("TRUNCATE TABLE test_copy_out_table")
        .await
        .unwrap();

    // 插入一些测试数据
    conn.exec("INSERT INTO test_copy_out_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();

    // 2. 执行 copy_out
    let sql = "COPY test_copy_out_table (id, name) TO STDOUT WITH CSV";
    let mut stream = conn.copy_out(sql).await.unwrap();

    // 3. 收集导出的数据
    let mut exported_data = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => {
                exported_data.push(bytes);
            }
            Err(e) => {
                panic!("读取导出数据时发生错误: {}", e);
            }
        }
    }

    // 4. 验证导出的数据
    let all_data: Vec<u8> = exported_data.into_iter().flat_map(|b| b.to_vec()).collect();
    let exported_str = String::from_utf8(all_data).expect("导出的数据应该是有效的UTF-8");

    // 解析CSV内容并验证行数
    let lines: Vec<&str> = exported_str.trim().split('\n').collect();
    assert_eq!(lines.len(), 3); // 应该有3行数据

    // 验证每行的内容
    assert!(lines.iter().any(|line| line.starts_with("1,Alice")));
    assert!(lines.iter().any(|line| line.starts_with("2,Bob")));
    assert!(lines.iter().any(|line| line.starts_with("3,Charlie")));

    println!(
        "基础 copy_out 测试成功，导出了 {} 字节的数据",
        exported_str.len()
    );
}

#[tokio::test]
async fn test_copy_out_large_dataset() {
    init();

    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str())
        .await
        .unwrap();

    let conn = pool.get().await.unwrap();

    // 1. 准备大数据集
    conn.exec("CREATE TABLE IF NOT EXISTS large_copy_out_table (id INT, name TEXT, value DECIMAL)")
        .await
        .unwrap();
    conn.exec("TRUNCATE TABLE large_copy_out_table")
        .await
        .unwrap();

    // 插入大量测试数据
    let total_rows: i64 = std::env::var("PGSQL_COPY_OUT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    // 使用批量插入
    let mut values = Vec::new();
    for i in 0..total_rows {
        values.push(format!("({}, 'User_{}', {}.{})", i, i, i, i % 100));
    }
    let values_str = values.join(", ");
    let insert_sql = format!(
        "INSERT INTO large_copy_out_table (id, name, value) VALUES {}",
        values_str
    );
    conn.exec(insert_sql).await.unwrap();

    println!("开始数据 copy_out 拉取测试 ({} 条)...", total_rows);

    // 2. 执行 copy_out
    let sql = "COPY large_copy_out_table (id, name, value) TO STDOUT WITH CSV";
    let mut stream = conn.copy_out(sql).await.unwrap();

    // 3. 逐块读取数据并计算行数
    let mut row_count = 0;
    let mut total_bytes = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => {
                total_bytes += bytes.len();
                // 计算行数（简单地计算换行符数量）
                let data_str = String::from_utf8_lossy(&bytes);
                row_count += data_str.matches('\n').count();
            }
            Err(e) => {
                panic!("读取导出数据时发生错误: {}", e);
            }
        }
    }

    // 4. 验证结果
    assert_eq!(row_count, total_rows as usize);

    println!("数据拉取完成！");
    println!("总记录数: {}", row_count);
    println!("总字节数: {}", total_bytes);

    // 5. 验证数据库中的记录数
    let db_count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM large_copy_out_table")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db_count, total_rows);

    println!("数据库验证成功：记录数匹配 ({})", db_count);
}
