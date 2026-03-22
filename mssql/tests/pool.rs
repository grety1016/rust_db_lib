#![allow(dead_code)]

mod prelude;
use prelude::*;
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn serde_deserialize() {
    init();
    //let pool = mssql::Pool::builder().build(&conn_str()).unwrap();
    let _pool = mssql::Pool::builder()
        .max_size(10)
        .idle_timeout(60 * 10)
        .min_idle(2)
        .max_lifetime(60 * 60 * 2)
        .build(conn_str())
        .unwrap();
    // let conn = pool.get().await.unwrap();

    #[derive(Deserialize, Debug, Serialize)]
    struct Row {
        col1: Option<String>,
        col2: Option<String>,
    }

    // //////1、
    // conn.begin_trans().await.unwrap();
    // let result2: Option<(String,String)> =
    //     conn.query_first(sql_bind!("SELECT @P1 AS col1,@p2 as col2","aaaaa2","bbbbb2")).await.unwrap();
    // info!("result2: {:?}", result2);
    // conn.commit().await.unwrap();

    // // /////2、
    // let result2: Option<(String,String)> = conn.scoped_trans(async {
    //     let result2: Option<(String,String)> =
    //     conn.query_first(sql_bind!("SELECT @P1 AS col1,@p2 as col2","aaaaa2","bbbbb2")).await.unwrap();
    //     Ok(result2)     
    // }).await.unwrap();
    // info!("result2: {:?}", result2);

    /////////3、
    // let result2: Option<(String,String)> =
    //     conn.query_first(sql_bind!("SELECT @P1 AS col1,@p2 as col2","aaaaa2","bbbbb2")).await.unwrap();
    // info!("result2: {:?}", result2);

    ////////////////4、  
    // conn.begin_trans().await.unwrap();  
    //     let mut result =
    //         conn.query(sql_bind!("SELECT @P1 AS col1, @P2 AS col2 UNION ALL SELECT @P1 AS col1, @P2 AS col2","aaaaa","bbbb")).await.unwrap();
    //     while let Some(row) = result.fetch().await.unwrap()
    //     {
    //         info!("col1: {:?}, col2: {:?}", row.try_get_str(0).unwrap(),row.try_get_str(1).unwrap());
    //     } 

    //     let has_trans = conn.has_trans();        
    //     info!("result: {}", has_trans);
    //     drop(result);       

    // conn.commit().await.unwrap();
    //     let has_trans = conn.has_trans();        
    //     info!("result: {}", has_trans);


    ////////////////5、 
    // let result2: Option<(String,String)>=
    // conn.query_first(sql_format!("SELECT {} AS col1,{} as col2","aaaaa2","bbbbb2")).await.unwrap();
    // info!("result1: {:?}", result2);

    ////////////////6、 
    // #[derive(Deserialize, Debug,Serialize)]
    // struct PersontESTResult{
    //     firstName: String,
    //     lastName: String
    // }
    // let result2:Vec<PersontESTResult>=
    // conn.query_collect(sql_format!("SELECT firstName,lastName from {}",sql_ident!("PersontEST"))).await.unwrap();
    // let json = serde_json::to_string(&result2).unwrap();
    // info!("{}", json);
    
    ////////////////7、
    // let result2:Option<(String,String)> =
    // conn.query_first(sql_format!("SELECT {col1} as col1,{col2} as col2",col1 = "aaaa",col2 = "bbbb")).await.unwrap();    
    // info!("result1: {:?}", result2);
    ////////////////8、
    
}