//! MySQL 协议级 copy_in (LOAD DATA LOCAL INFILE) 使用示例

use bytes::Bytes;
use futures_util::stream;
use mysql::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 创建连接池
    let url = "mysql://root:Kephi520!@localhost:3306/Salary";
    let pool = mysql::Pool::builder().max_size(10).connect(url).await?;

    // 获取连接
    let conn = pool.get().await?;

    // 0. 尝试开启服务器端 local_infile (需要权限)
    conn.exec("SET GLOBAL local_infile = ON").await.ok();

    // 1. 准备更真实的测试表
    conn.exec("DROP TABLE IF EXISTS test_copy_in").await?;
    conn.exec(
        "CREATE TABLE test_copy_in (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            salary DECIMAL(10, 2),
            created_at DATETIME,
            note TEXT
        )",
    )
    .await?;

    // 2. 准备 100 万条 CSV 数据流 (分块产生以节省内存)
    let total_rows = 1_000_000;
    let num_chunks = 100;
    let rows_per_chunk = 10_000;

    let chunks = (0..num_chunks).map(move |i| {
        let mut buf = String::with_capacity(rows_per_chunk * 100);
        for j in 0..rows_per_chunk {
            let id = i * rows_per_chunk + j;
            let name = format!("User, \"{}\"", id);
            let salary = 3000.0 + (id as f64) * 0.1;
            let note = if id % 100 == 0 {
                "\\N"
            } else {
                "Bulk imported data"
            };

            buf.push_str(&format!(
                "{},\"{}\",{},{},{}\n",
                id,
                name.replace("\"", "\"\""),
                salary,
                "2026-03-21 12:00:00",
                note
            ));
        }
        Ok::<Bytes, std::io::Error>(Bytes::from(buf))
    });
    let stream = stream::iter(chunks);

    println!("🚀 开始 100 万条真实数据流式插入...");
    let start = Instant::now();

    // 3. 执行 copy_in
    let sql = "LOAD DATA LOCAL INFILE '{{STREAM}}' 
               INTO TABLE test_copy_in 
               FIELDS TERMINATED BY ',' 
               OPTIONALLY ENCLOSED BY '\"'
               LINES TERMINATED BY '\n'
               (id, name, salary, created_at, note)";

    let count = conn.copy_in(sql, stream).await?;

    let duration = start.elapsed();
    println!("流式插入完成！");
    println!("导入记录数: {}", count);
    println!("总耗时: {:?}", duration);

    // 4. 验证数据
    let mut rs = conn.query("SELECT COUNT(*) FROM test_copy_in").await?;
    let actual_count = rs.scalar_i64().await?.unwrap_or(0);
    assert_eq!(actual_count, total_rows as i64);
    println!("数据验证成功: 数据库中共有 {} 条记录。", actual_count);

    Ok(())
}
