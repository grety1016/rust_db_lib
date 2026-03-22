#![allow(dead_code)]

mod prelude;
use bytes::Bytes;
use futures_util::stream;
use prelude::*;
use std::time::Instant;

#[tokio::test]
async fn test_copy_in_basic() {
    init();

    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str())
        .await
        .unwrap();

    let conn = pool.get().await.unwrap();

    // 1. 准备测试表
    conn.exec("DROP TABLE IF EXISTS test_copy_table")
        .await
        .unwrap();
    conn.exec("CREATE TABLE test_copy_table (id INT, name TEXT)")
        .await
        .unwrap();

    // 2. 准备 CSV 数据流 (id,name)
    let chunks = vec![
        Ok::<_, std::io::Error>(Bytes::from("1,Alice\n2,")),
        Ok::<_, std::io::Error>(Bytes::from("Bob\n3,Charlie\n4,David\n5,Eve\n")),
    ];
    let stream = stream::iter(chunks);

    // 3. 执行 copy_in
    let sql = "COPY test_copy_table FROM STDIN WITH CSV";
    let count = conn.copy_in(sql, stream).await.unwrap();

    assert_eq!(count, 5);

    // 4. 验证数据
    let rows: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM test_copy_table")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows, 5);

    println!("基础 copy_in 测试成功，导入了 {} 条记录", count);
}

#[tokio::test]
async fn test_copy_in_million_rows() {
    init();

    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str())
        .await
        .unwrap();

    let conn = pool.get().await.unwrap();

    // 1. 准备大表
    conn.exec("DROP TABLE IF EXISTS million_rows_table")
        .await
        .unwrap();
    conn.exec("CREATE TABLE million_rows_table (id INT, name TEXT, created_at TIMESTAMP)")
        .await
        .unwrap();

    let total_rows = 1_000_000;
    let batch_size = 10_000; // 每批推送 1万条

    // 2. 模拟内存流生成器 (不占用大量内存，按需生成)
    let chunks: Vec<Result<Bytes, std::io::Error>> = (0..(total_rows / batch_size))
        .map(|batch_idx| {
            let mut buf = String::with_capacity(batch_size * 50);
            for i in 0..batch_size {
                let id = batch_idx * batch_size + i;
                buf.push_str(&format!("{},User_{},2023-01-01 00:00:00\n", id, id));
            }
            Ok(Bytes::from(buf))
        })
        .collect();

    let stream = stream::iter(chunks);

    println!("开始百万级数据 copy_in 推送测试 ({} 条)...", total_rows);
    let start = Instant::now();

    // 3. 执行海量导入
    let sql = "COPY million_rows_table FROM STDIN WITH CSV";
    let count = conn.copy_in(sql, stream).await.unwrap();

    let duration = start.elapsed();
    println!("百万级数据推送完成！");
    println!("总记录数: {}", count);
    println!("总耗时: {:?}", duration);
    println!(
        "平均每秒导入: {} 条",
        (count as f64 / duration.as_secs_f64()) as u64
    );

    // 4. 验证数据一致性
    let db_count: i64 = conn
        .query_scalar_i64("SELECT COUNT(*) FROM million_rows_table")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db_count, total_rows as i64);

    println!("数据库验证成功：记录数匹配 ({})", db_count);
}
