#[allow(unused_imports)]
pub use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
#[allow(unused_imports)]
pub use rust_decimal::Decimal;
#[allow(unused_imports)]
pub use tiberius::Uuid;

pub fn init() {
    let subscriber =
        tracing_subscriber::FmtSubscriber::builder().with_max_level(tracing::Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

pub fn conn_str() -> String {
    let host = "localhost,1433";
    let database = "Salary";
    let user = "sa";
    let pwd = "Kephi520!";
    format!(
        "Server={host};Database={database};User Id={user};Password={pwd};Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true;"
    )
}
