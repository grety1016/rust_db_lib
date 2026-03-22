#![allow(dead_code)]

mod prelude;
use prelude::*;

#[allow(clippy::type_complexity)]
#[tokio::test]
async fn serde_deserialize() {
    init();
    //let pool = mssql::Pool::builder().max_size(1).connect(conn_str).await.expect("connect");
    //let conn = pool.get().await.expect("get conn");
    let conn = mssql::Connection::connect(&conn_str()).await.expect("connect");

    let result: Vec<String> =
        conn.query_collect("SELECT 'aaaa' AS col1 UNION SELECT 'bbb' AS col1").await.unwrap();
    println!("result: {:?}", result);

    let result: Vec<(Option<String>, String)> = conn
        .query_collect("SELECT 'aaaa' AS col1, 'bbb' AS col2 UNION SELECT 'ccc' AS col1, 'ddd' AS col2")
        .await
        .unwrap();
    println!("result: {:?}", result);

    let result: String = conn.query_first("SELECT 'aaaa' AS col1").await.unwrap();
    println!("result: {}", result);

    let result: Option<String> = conn.query_first("SELECT 'aaaa' AS col1").await.unwrap();
    println!("result: {:?}", result);

    let result: Option<String> = conn.query_first("SELECT CAST(NULL AS VARCHAR(10)) AS col1").await.unwrap();
    println!("result: {:?}", result);

    #[derive(serde::Deserialize, Debug)]
    struct RowData1 {
        col1: String,
    }

    let result: Vec<RowData1> =
        conn.query_collect("SELECT 'aaaa' AS col1 UNION SELECT 'bbb' AS col1").await.unwrap();
    println!("result: {:?}", result);

    #[derive(serde::Deserialize, Debug)]
    struct RowData2 {
        col1: String,
        col2: Option<String>,
        col3: Decimal,
        col4: NaiveDateTime,
        col5: NaiveDate,
        col6: NaiveTime,
        col7: Uuid,
    }

    let result: Vec<RowData2> = conn
    .query_collect("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {:?}", result);

    let result: serde_json::Value = conn
    .query_collect("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {}", result);

    let result: RowData2 = conn
    .query_first("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {:?}", result);

    let result: (
        String,
        Option<String>,
        Decimal,
        NaiveDateTime,
        NaiveDate,
        NaiveTime,
        Uuid) = conn
    .query_first("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {:?}", result);

    let result: Option<(
        String,
        Option<String>,
        Decimal,
        NaiveDateTime,
        NaiveDate,
        NaiveTime,
        Uuid)> = conn
    .query_first("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {:?}", result);

    let result: serde_json::Value = conn
    .query_first("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {}", result);

    let result: serde_json::Value = conn.query_first("SELECT NULL as col1").await.unwrap();
    println!("result: {}", result);

    let result: Option<serde_json::Value> = conn
    .query_first("SELECT 'aaaa' AS col1,NULL AS col2,123.23 AS col3,GETDATE() AS col4,CAST(GETDATE() AS date) AS col5,CAST(GETDATE() AS time) AS col6,NewId() AS col7")
    .await
    .unwrap();
    println!("result: {:?}", result);
}
