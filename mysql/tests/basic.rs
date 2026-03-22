use mysql::prelude::*;

#[tokio::test]
async fn test_mysql_connection() -> Result<(), mysql::Error> {
    let url = "mysql://root:Kephi520!@127.0.0.1:3306/Salary";
    let pool = mysql::Pool::builder().max_size(5).connect(url).await?;

    let conn = pool.get().await?;

    // 测试 sql_bind! 和参数绑定 (安全性)
    let name = "test_user";
    let sql = mysql::sql_bind!("SELECT ?", name);
    let mut rs = conn.query(sql).await?;

    let result_name = rs.scalar_string().await?.unwrap_or_default();
    assert_eq!(result_name, name);

    println!("MySQL connection and parameter binding verified!");
    Ok(())
}
