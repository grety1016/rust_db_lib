#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn_str = "postgresql://root:Kephi520!@localhost:5432/Salary";
    let pool = pgsql::Pool::builder().max_size(1).connect(conn_str).await?;

    let conn = pool.get().await?;

    // 检查基础测试表
    if conn.table_exists("test_copy_table").await? {
        let count: i64 = conn
            .query_scalar_i64("SELECT COUNT(*) FROM test_copy_table")
            .await?
            .unwrap_or(0);
        println!("Table 'test_copy_table' count: {}", count);
    } else {
        println!("Table 'test_copy_table' does not exist.");
    }

    // 检查百万级测试表
    if conn.table_exists("million_rows_table").await? {
        let count: i64 = conn
            .query_scalar_i64("SELECT COUNT(*) FROM million_rows_table")
            .await?
            .unwrap_or(0);
        println!("Table 'million_rows_table' count: {}", count);
    } else {
        println!("Table 'million_rows_table' does not exist.");
    }

    Ok(())
}
