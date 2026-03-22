use mysql::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_mysql_connection() -> Result<(), mysql::Error> {
    let url = "mysql://root:Kephi520!@127.0.0.1:3306/Salary";
    let pool = tokio::time::timeout(
        Duration::from_secs(3),
        mysql::Pool::builder().max_size(5).connect(url),
    )
    .await
    .map_err(|_| mysql::Error::custom("connect timeout"))??;

    let conn = tokio::time::timeout(Duration::from_secs(3), pool.get())
        .await
        .map_err(|_| mysql::Error::custom("get connection timeout"))??;

    // 测试 sql_bind! 和参数绑定 (安全性)
    let name = "test_user";
    let sql = mysql::sql_bind!("SELECT ?", name);
    let mut rs = conn.query(sql).await?;

    let result_name = rs.scalar_string().await?.unwrap_or_default();
    assert_eq!(result_name, name);

    println!("MySQL connection and parameter binding verified!");
    Ok(())
}
