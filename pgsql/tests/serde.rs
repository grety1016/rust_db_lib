#![allow(dead_code)]

mod prelude;
use pgsql::{sql_bind, sql_format, sql_ident};
use prelude::*;
use serde::Deserialize;

#[tokio::test]
async fn test_serde_deserialization() {
    init();
    let pool = pgsql::Pool::builder()
        .max_size(1)
        .connect(&conn_str())
        .await
        .expect("connect");
    let conn = pool.get().await.expect("get conn");

    // 1. 测试基础类型 Vec<String>
    let result: Vec<String> = conn
        .query_collect("SELECT 'aaaa' AS col1 UNION SELECT 'bbb' AS col1 ORDER BY col1")
        .await
        .unwrap();
    assert_eq!(result, vec!["aaaa".to_string(), "bbb".to_string()]);

    // 2. 测试元组 Vec<(Option<String>, String)>
    let result: Vec<(Option<String>, String)> = conn
        .query_collect(
            "SELECT 'aaaa' AS col1, 'bbb' AS col2 UNION SELECT NULL AS col1, 'ddd' AS col2",
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2);

    // 3. 测试单个 String (query_first)
    let result: String = conn.query_first("SELECT 'aaaa' AS col1").await.unwrap();
    assert_eq!(result, "aaaa");

    // 4. 测试结构体反序列化
    #[derive(Deserialize, Debug, PartialEq)]
    struct RowData1 {
        col1: String,
    }
    let result: Vec<RowData1> = conn
        .query_collect("SELECT 'aaaa' AS col1 UNION SELECT 'bbb' AS col1 ORDER BY col1")
        .await
        .unwrap();
    assert_eq!(result[0].col1, "aaaa");

    // 5. 测试复杂结构体 (含 Decimal, NaiveDateTime, Uuid 等)
    #[derive(Deserialize, Debug)]
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
        .query_collect(sql_bind!(
            "SELECT 'aaaa' AS col1, NULL AS col2, 123.23::numeric AS col3, 
            '2023-01-01 12:00:00'::timestamp AS col4, 
            '2023-01-01'::date AS col5, 
            '12:00:00'::time AS col6, 
            '550e8400-e29b-41d4-a716-446655440000'::uuid AS col7"
        ))
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].col1, "aaaa");
    assert_eq!(result[0].col3, Decimal::new(12323, 2));

    // 6. 测试 sql_format! 和 sql_ident!
    let table_name = "users";
    let result: Vec<serde_json::Value> = conn
        .query_collect(sql_format!("SELECT * FROM {}", sql_ident!(table_name)))
        .await
        .unwrap();
    println!("Users: {:?}", result);

    // 7. 测试 sql_bind! 与多个参数
    let name = "John";
    let id = 1;
    let result: Option<serde_json::Value> = conn
        .query_first(sql_bind!(
            "SELECT * FROM users WHERE id = $1 AND name = $2",
            id,
            name
        ))
        .await
        .unwrap();
    println!("User 1: {:?}", result);
}
