#[allow(unused_imports)]
pub use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
#[allow(unused_imports)]
pub use pgsql::prelude::*;
#[allow(unused_imports)]
pub use rust_decimal::Decimal;
#[allow(unused_imports)]
pub use uuid::Uuid;

use std::sync::Once;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set global subscriber");
    });
}

pub fn conn_str() -> String {
    "postgresql://root:Kephi520!@localhost:5432/Salary".to_owned()
}
