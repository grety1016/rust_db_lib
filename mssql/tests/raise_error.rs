#![allow(dead_code)]

pub mod prelude;
use prelude::*;

#[tokio::test]
async fn raise_error() {
    init();
    let conn = mssql::Connection::connect(&conn_str()).await.expect("connect");

    let result = conn
        .query_scalar_i32("DECLARE @admin_uid INT; RAISERROR('这是一个异常!',16,1); SELECT @admin_uid")
        .await;
    println!("result: {:?}", result);
}
