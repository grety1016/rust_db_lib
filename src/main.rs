use std::path::Path;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{filter::Targets, fmt, prelude::*, registry};

use bytes::Bytes;
use mssql::Pool;
use mysql::Error as MySqlError;
use mysql::prelude::*;
use pgsql::Error as PgsqlError;

// ==========================================
// 第一部分：脱敏核心逻辑 (所有日志分流共享)
// ==========================================

/// 全局脱敏规则配置
///
/// 该函数通过正则表达式匹配日志中的敏感信息，并根据预设策略进行脱敏处理。
///
/// ### 正则表达式详解：
/// 1. `(?i)`: 开启大小写不敏感模式。例如：既能匹配 `password` 也能匹配 `Password` 或 `PWD`。
/// 2. `(password|pwd|secret|token)`: **捕获组 1**，包含所有需要“全脱敏”的关键字。
/// 3. `(phone|mobile|account|id_card)`: **捕获组 1**，包含所有需要“半脱敏”的关键字。
/// 4. `["\s:=]+`: 匹配关键字与值之间的分隔符。
///    - `"`: 匹配 JSON 格式中的双引号。
///    - `\s`: 匹配空格。
///    - `:` 或 `=`: 匹配赋值符号。
///    - `+`: 表示匹配上述符号的一个或多个。
///    - **例子**：能同时处理 `password: "123"`, `pwd=123`, `password : 123` 等各种书写风格。
/// 5. `([^"\s,;]+)`: **捕获组 2**，匹配真正的值内容。
///    - `[^ ... ]`: 表示匹配“不在括号内”的任意字符。
///    - `"\s,;`: 遇到引号、空格、逗号或分号就停止匹配。这能精准提取出值而不会误伤到日志的其他部分。
///
/// ### 示例场景：
/// - **全脱敏**：`"password":"Kephi520!"` -> 识别到关键字 `password`，将值替换为 `****`。
/// - **半脱敏**：`"phone":"13812345678"` -> 识别到 `phone`，提取出 `13812345678`，保留头尾显示为 `138***5678`。
fn apply_masking(input: &str) -> String {
    lazy_static::lazy_static! {
        // 1. 全脱敏正则: 匹配密码、密钥、令牌等。
        // 匹配逻辑：找到关键字后，将其后的具体值替换为 ****。
        static ref RE_FULL: regex::Regex = regex::Regex::new(r#"(?i)(password|pwd|secret|token)["\s:=]+([^"\s,;]+)"#).unwrap();

        // 2. 半脱敏正则: 匹配手机号、账号、身份证等。
        // 匹配逻辑：找到关键字后，提取值，保留前3位和后4位，中间加掩码。
        static ref RE_PARTIAL: regex::Regex = regex::Regex::new(r#"(?i)(phone|mobile|account|id_card)["\s:=]+([^"\s,;]+)"#).unwrap();
    }

    let mut output = input.to_string();

    // 执行全脱敏替换
    output = RE_FULL
        .replace_all(&output, |caps: &regex::Captures| {
            // caps[1] 是匹配到的关键字，如 password
            format!("{}:****", &caps[1])
        })
        .to_string();

    // 执行半脱敏替换
    output = RE_PARTIAL
        .replace_all(&output, |caps: &regex::Captures| {
            let key = &caps[1];
            let val = &caps[2];
            // 对值进行掩码处理
            let masked = if val.len() > 11 {
                format!("{}***{}", &val[..6], &val[val.len() - 4..])
            } else if val.len() > 7 {
                // 长度足够，保留前3后4，如: 13812345678 -> 138***5678
                format!("{}***{}", &val[..3], &val[val.len() - 4..])
            } else if val.len() > 2 {
                // 长度较短，只保留前2位
                format!("{}***", &val[..2])
            } else {
                // 极短内容，直接全部掩码
                "***".to_string()
            };
            format!("{}:{}", key, masked)
        })
        .to_string();

    output
}

/// 脱敏 Writer 包装器
///
/// 这是一个包装器，它实现了 `std::io::Write` 接口。
/// 当日志框架尝试写入数据时，它会先通过 `apply_masking` 处理后再真正写入底层 Writer（如文件）。
#[derive(Clone)]
struct MaskingWriter<W> {
    inner: W,
}

impl<W: std::io::Write> std::io::Write for MaskingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 1. 将待写入的原始字节转为字符串（由于日志通常是文本，这种转换是安全的）
        let s = String::from_utf8_lossy(buf);
        // 2. 调用全局脱敏规则
        let masked = apply_masking(&s);
        // 3. 将脱敏后的内容写入底层的真正 Writer（如 RollingFileAppender）
        self.inner.write_all(masked.as_bytes())?;
        // 4. 注意：必须返回原始数据的长度，否则日志框架会认为没写完而重试
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn init() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

pub fn init_dev() -> Vec<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::fmt::time::UtcTime;

    // 创建 logfiles 目录
    let log_dir = Path::new("logfiles");
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir).unwrap();
    }

    let mut guards = Vec::new();

    // 格式定义
    let format = fmt::format()
        .compact()
        .with_level(true)
        .with_target(true)
        .with_timer(UtcTime::rfc_3339())
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_source_location(false)
        .with_file(false)
        .with_line_number(true);

    // 1. MSSQL 专用日志 (mssql_dev.log)
    let mssql_appender = RollingFileAppender::new(Rotation::NEVER, "logfiles", "mssql_dev.log");
    // 将 MaskingWriter 放在 non_blocking 内部，这样脱敏逻辑就在后台线程执行了
    let mssql_masking = MaskingWriter {
        inner: mssql_appender,
    };
    let (mssql_nb, mssql_guard) = tracing_appender::non_blocking(mssql_masking);
    guards.push(mssql_guard);

    let mssql_filter = Targets::new()
        .with_target("mssql", LevelFilter::TRACE)
        .with_target("tiberius", LevelFilter::TRACE); // 调高 tiberius 级别，减少握手细节
    let mssql_layer = fmt::layer()
        .event_format(format.clone())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // 在像 axum 这类框架中建议开启；这里主要用于数据库测试观察效果
        .with_ansi(false)
        .with_writer(mssql_nb)
        .with_filter(mssql_filter);

    // 2. PGSQL 专用日志 (pgsql_dev.log)
    let pgsql_appender = RollingFileAppender::new(Rotation::NEVER, "logfiles", "pgsql_dev.log");
    let pgsql_masking = MaskingWriter {
        inner: pgsql_appender,
    };
    let (pgsql_nb, pgsql_guard) = tracing_appender::non_blocking(pgsql_masking);
    guards.push(pgsql_guard);

    let pgsql_filter = Targets::new()
        .with_target("pgsql", LevelFilter::TRACE)
        .with_target("tokio_postgres", LevelFilter::TRACE)
        .with_target("postgres", LevelFilter::TRACE)
        .with_target("bb8", LevelFilter::TRACE);
    let pgsql_layer = fmt::layer()
        .event_format(format.clone())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // 在像 axum 这类框架中建议开启；这里主要用于数据库测试观察效果
        .with_ansi(false)
        .with_writer(pgsql_nb)
        .with_filter(pgsql_filter);

    // 2.5. MYSQL 专用日志 (mysql_dev.log)
    let mysql_appender = RollingFileAppender::new(Rotation::NEVER, "logfiles", "mysql_dev.log");
    let mysql_masking = MaskingWriter {
        inner: mysql_appender,
    };
    let (mysql_nb, mysql_guard) = tracing_appender::non_blocking(mysql_masking);
    guards.push(mysql_guard);

    let mysql_filter = Targets::new()
        .with_target("mysql", LevelFilter::TRACE)
        .with_target("mysql_async", LevelFilter::TRACE);
    let mysql_layer = fmt::layer()
        .event_format(format.clone())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // 在像 axum 这类框架中建议开启；这里主要用于数据库测试观察效果
        .with_ansi(false)
        .with_writer(mysql_nb)
        .with_filter(mysql_filter);

    // 3. 应用日志 (dev.log)
    let app_appender = RollingFileAppender::new(Rotation::NEVER, "logfiles", "dev.log");
    let app_masking = MaskingWriter {
        inner: app_appender,
    };
    let (app_nb, app_guard) = tracing_appender::non_blocking(app_masking);
    guards.push(app_guard);

    let app_filter = Targets::new()
        .with_target("rustdemo", LevelFilter::TRACE)
        .with_default(LevelFilter::INFO);
    let app_layer = fmt::layer()
        .event_format(format)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // 在像 axum 这类框架中建议开启；这里主要用于数据库测试观察效果
        .with_ansi(false)
        .with_writer(app_nb)
        .with_filter(app_filter);

    // ---------------------------------------------------------
    // [模板] 如果后续需要新增其它包的日志分流，请复制以下完整模型：
    // ---------------------------------------------------------
    /*
    // 1. 定义底层文件 (文件名、轮转策略)
    let other_appender = RollingFileAppender::new(Rotation::NEVER, "logfiles", "other_dev.log");

    // 2. 包装脱敏 (注意：放在 non_blocking 之前，确保脱敏在后台线程执行)
    let other_masking = MaskingWriter { inner: other_appender };

    // 3. 异步化处理
    let (other_nb, other_guard) = tracing_appender::non_blocking(other_masking);
    guards.push(other_guard); // 必须存入 guards 数组，否则日志会失效

    // 4. 定义过滤器 (支持多个 target)
    let other_filter = Targets::new()
        .with_target("package_name", LevelFilter::TRACE)
        .with_target("sub_package", LevelFilter::TRACE);

    // 5. 创建完整图层 (格式、禁用ANSI、写入器、过滤器)
    let other_layer = fmt::layer()
        .event_format(format.clone())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // 在像 axum 这类框架中建议开启；这里主要用于数据库测试观察效果
        .with_ansi(false)
        .with_writer(other_nb)
        .with_filter(other_filter);
    */
    // ---------------------------------------------------------

    registry()
        .with(mssql_layer)
        .with(pgsql_layer)
        .with(mysql_layer)
        .with(app_layer)
        // .with(other_layer) // 6. 在这里注册新图层
        .init();

    guards
}

pub fn init_prod() -> Vec<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_appender::rolling::Rotation;

    // 创建 logfiles 目录
    let log_dir = Path::new("logfiles");
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir).unwrap();
    }

    let mut guards = Vec::new();

    // 1. MSSQL 专用日志 (mssql.log)
    let mssql_appender = RollingFileAppender::new(Rotation::DAILY, "logfiles", "mssql.log");
    let mssql_masking = MaskingWriter {
        inner: mssql_appender,
    };
    let (mssql_nb, mssql_guard) = tracing_appender::non_blocking(mssql_masking);
    guards.push(mssql_guard);

    let mssql_filter = Targets::new()
        .with_target("mssql", LevelFilter::INFO)
        .with_target("tiberius", LevelFilter::WARN);
    let mssql_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(mssql_nb)
        .with_filter(mssql_filter);

    // 2. PGSQL 专用日志 (pgsql.log)
    let pgsql_appender = RollingFileAppender::new(Rotation::DAILY, "logfiles", "pgsql.log");
    let pgsql_masking = MaskingWriter {
        inner: pgsql_appender,
    };
    let (pgsql_nb, pgsql_guard) = tracing_appender::non_blocking(pgsql_masking);
    guards.push(pgsql_guard);

    let pgsql_filter = Targets::new()
        .with_target("pgsql", LevelFilter::INFO)
        .with_target("tokio_postgres", LevelFilter::WARN)
        .with_target("bb8", LevelFilter::WARN);
    let pgsql_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(pgsql_nb)
        .with_filter(pgsql_filter);

    // 2.5. MYSQL 专用日志 (mysql.log)
    let mysql_appender = RollingFileAppender::new(Rotation::DAILY, "logfiles", "mysql.log");
    let mysql_masking = MaskingWriter {
        inner: mysql_appender,
    };
    let (mysql_nb, mysql_guard) = tracing_appender::non_blocking(mysql_masking);
    guards.push(mysql_guard);

    let mysql_filter = Targets::new()
        .with_target("mysql", LevelFilter::INFO)
        .with_target("mysql_async", LevelFilter::WARN);
    let mysql_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(mysql_nb)
        .with_filter(mysql_filter);

    // 3. 应用日志 (app.log)
    let app_appender = RollingFileAppender::new(Rotation::DAILY, "logfiles", "app.log");
    let app_masking = MaskingWriter {
        inner: app_appender,
    };
    let (app_nb, app_guard) = tracing_appender::non_blocking(app_masking);
    guards.push(app_guard);

    let app_filter = Targets::new()
        .with_target("rustdemo", LevelFilter::INFO)
        .with_default(LevelFilter::WARN);
    let app_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(app_nb)
        .with_filter(app_filter);

    // ---------------------------------------------------------
    // [模板] 生产环境新增包日志分流，请复制以下完整模型：
    // ---------------------------------------------------------
    /*
    // 1. 定义底层文件 (文件名、按天轮转)
    let other_appender = RollingFileAppender::new(Rotation::DAILY, "logfiles", "other.log");

    // 2. 包装脱敏
    let other_masking = MaskingWriter { inner: other_appender };

    // 3. 异步化
    let (other_nb, other_guard) = tracing_appender::non_blocking(other_masking);
    guards.push(other_guard);

    // 4. 定义过滤器
    let other_filter = Targets::new()
        .with_target("package_name", LevelFilter::INFO);

    // 5. 创建图层 (注意：生产环境使用 .json() 格式)
    let other_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(other_nb)
        .with_filter(other_filter);
    */
    // ---------------------------------------------------------

    registry()
        .with(mssql_layer)
        .with(pgsql_layer)
        .with(mysql_layer)
        .with(app_layer)
        // .with(other_layer) // 6. 在这里注册
        .init();

    guards
}

async fn init_mysql() -> Result<(), MySqlError> {
    let connection_string = "mysql://root:Kephi520!@localhost:3306/Salary";
    info!("正在尝试连接到MySQL数据库...");

    let pool = mysql::Pool::builder()
        .max_size(10)
        .connect(connection_string)
        .await?;
    let mut conn = pool.get().await?;
    conn.set_log_category("mysql");
    conn.set_log_db_name("Salary");
    let _ = conn.is_connected().await;
    let _ = conn.db_exists("Salary").await?;
    let _ = conn
        .query_scalar_string(mysql::sql_bind!("SELECT ?", "password=Kephi520!"))
        .await?;
    let _ = conn
        .query_scalar_string(mysql::sql_bind!("SELECT ?", "token=Bearer abc.def.ghi"))
        .await?;
    let _ = conn
        .query_scalar_string(mysql::sql_bind!("SELECT ?", "phone=13812345678"))
        .await?;
    let _ = conn
        .query_scalar_string(mysql::sql_bind!("SELECT ?", "id_card=11010519491231002X"))
        .await?;
    let _ = conn
        .query_scalar_string(mysql::sql_bind!("SELECT ?", "account=grety@example.com"))
        .await?;

    conn.exec("DROP TABLE IF EXISTS demo_users").await?;
    conn.exec("CREATE TABLE demo_users (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        .await?;
    let _ = conn.object_exists("demo_users").await?;
    let _ = conn.column_exists("demo_users", "name").await?;

    let _ = conn
        .exec_timeout(
            mysql::sql_bind!(
                "INSERT INTO demo_users (id, name, age) VALUES (?, ?, ?)",
                1,
                "Alice",
                30
            ),
            std::time::Duration::from_secs(5),
        )
        .await?;

    let mut rs = conn
        .query(mysql::sql_bind!(
            mysql::sql_format!(
                "SELECT * FROM {} WHERE id = ? AND 1 = ?",
                mysql::sql_ident!("demo_users")
            ),
            1,
            1
        ))
        .await?;
    while let Some(row) = rs.fetch().await? {
        let _ = row.try_get_i32("id")?;
        let _ = row.try_get_string("name")?;
        let _ = row.try_get_i32("age")?;
    }
    drop(rs);

    let _rows: Vec<mysql::Row> = conn.query_collect_row("SELECT * FROM demo_users").await?;
    let _first_row = conn
        .query_first_row(mysql::sql_bind!("SELECT * FROM demo_users WHERE id = ?", 1))
        .await?;

    let _ = conn.query_scalar_i32("SELECT 1").await?;
    let _ = conn.query_scalar_i64("SELECT 1").await?;
    let _ = conn.query_scalar_string("SELECT 'abc'").await?;
    let _ = conn.query_scalar_i16("SELECT CAST(10 AS SIGNED)").await?;
    let _ = conn.query_scalar_u8("SELECT CAST(255 AS UNSIGNED)").await?;
    let _ = conn.query_scalar_f32("SELECT CAST(1.25 AS FLOAT)").await?;
    let _ = conn.query_scalar_f64("SELECT CAST(1.25 AS DOUBLE)").await?;
    let _ = conn.query_scalar_bool("SELECT TRUE").await?;
    let _ = conn
        .query_scalar_dec("SELECT CAST('123.45' AS DECIMAL(10,2))")
        .await?;
    let _ = conn.query_scalar_datetime("SELECT NOW(6)").await?;
    let _ = conn.query_scalar_date("SELECT DATE('2023-01-01')").await?;
    let _ = conn
        .query_scalar_time("SELECT CAST('12:34:56' AS TIME)")
        .await?;
    let _ = conn
        .query_scalar_uuid("SELECT '550e8400-e29b-41d4-a716-446655440000'")
        .await?;

    conn.exec("DROP TABLE IF EXISTS demo_ai").await?;
    conn.exec("CREATE TABLE demo_ai (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        .await?;
    conn.exec(mysql::sql_bind!(
        "INSERT INTO demo_ai (name) VALUES (?)",
        "auto"
    ))
    .await?;
    let _ = conn.last_identity_opt().await?;
    let _ = conn.last_identity().await?;

    conn.begin_trans().await?;
    conn.exec(mysql::sql_bind!(
        "INSERT INTO demo_users (id, name, age) VALUES (?, ?, ?)",
        2,
        "Bob",
        20
    ))
    .await?;
    conn.rollback().await?;

    conn.scoped_trans(async {
        conn.exec(mysql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES (?, ?, ?)",
            3,
            "Carol",
            22
        ))
        .await?;
        conn.scoped_trans(async {
            conn.exec(mysql::sql_bind!(
                "UPDATE demo_users SET age = age + 1 WHERE id = ?",
                3
            ))
            .await?;
            Ok::<(), mysql::Error>(())
        })
        .await?;
        Ok::<(), mysql::Error>(())
    })
    .await?;

    conn.sandbox_trans(async {
        conn.exec(mysql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES (?, ?, ?)",
            999,
            "Sandbox",
            1
        ))
        .await?;
        Ok::<(), mysql::Error>(())
    })
    .await?;

    conn.exec("DROP TABLE IF EXISTS demo_copy_in").await?;
    conn.exec("CREATE TABLE demo_copy_in (id INT, name VARCHAR(100))")
        .await?;
    let chunks = vec![Ok::<_, std::io::Error>(Bytes::from("1,Alice\n2,Bob\n"))];
    let stream = futures::stream::iter(chunks);
    let _ = conn
        .copy_in(
            "LOAD DATA LOCAL INFILE '{{STREAM}}' INTO TABLE demo_copy_in FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (id,name)",
            stream,
        )
        .await?;

    conn.change_db("Salary").await?;
    let _ = conn.current_db();
    conn.restore_db().await?;
    conn.reconnect().await?;

    Ok(())
}

async fn init_pgsql() -> Result<(), PgsqlError> {
    let connection_string = conn_str_pgsql();
    info!(
        "连接字符串: Server={}, Database={}",
        "localhost,14333", "Salary"
    ); // 不包含密码

    let pool = pgsql::Pool::builder()
        .max_size(100)
        .min_idle(2)
        .max_lifetime(1800)
        .idle_timeout(300)
        .connect_timeout(30)
        .build(&connection_string) // ✔ 同步返回 Result<Pool>
        .expect("build pool");
    info!("成功创建连接池！");

    info!("成功获取数据库连接！");
    let mut conn = pool.get().await.expect("get conn");
    conn.set_log_category("pgsql");
    conn.set_log_db_name("Salary");
    let _ = conn.is_connected().await;
    let _ = conn.is_closed();
    let _ = conn.db_exists("Salary").await?;
    let _ = conn
        .query_scalar_string(pgsql::sql_bind!("SELECT $1::text", "password=Kephi520!"))
        .await?;
    let _ = conn
        .query_scalar_string(pgsql::sql_bind!(
            "SELECT $1::text",
            "token=Bearer abc.def.ghi"
        ))
        .await?;
    let _ = conn
        .query_scalar_string(pgsql::sql_bind!("SELECT $1::text", "phone=13812345678"))
        .await?;
    let _ = conn
        .query_scalar_string(pgsql::sql_bind!(
            "SELECT $1::text",
            "id_card=11010519491231002X"
        ))
        .await?;
    let _ = conn
        .query_scalar_string(pgsql::sql_bind!(
            "SELECT $1::text",
            "account=grety@example.com"
        ))
        .await?;

    conn.exec("DROP TABLE IF EXISTS demo_users").await?;
    conn.exec("CREATE TABLE demo_users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)")
        .await?;
    let _ = conn.object_exists("demo_users").await?;
    let _ = conn.column_exists("demo_users", "name").await?;

    let _ = conn
        .exec_timeout(
            pgsql::sql_bind!(
                "INSERT INTO demo_users (id, name, age) VALUES ($1, $2, $3)",
                1,
                "Alice",
                30
            ),
            std::time::Duration::from_secs(5),
        )
        .await?;

    let mut rs = conn
        .query(pgsql::sql_bind!(
            "SELECT id, name, age FROM demo_users WHERE id = $1",
            1
        ))
        .await?;
    while let Some(row) = rs.fetch().await? {
        let _ = row.try_get_i32(0)?;
        let _ = row.try_get_string(1)?;
        let _ = row.try_get_i32(2)?;
    }
    drop(rs);
    let _rows: Vec<pgsql::Row> = conn.query_collect_row("SELECT * FROM demo_users").await?;
    let _first_row = conn
        .query_first_row("SELECT * FROM demo_users WHERE id = 1")
        .await?;

    let _ = conn.query_scalar_i32("SELECT 1").await?;
    let _ = conn.query_scalar_i64("SELECT 1").await?;
    let _ = conn.query_scalar_string("SELECT 'abc'").await?;
    let _ = conn.query_scalar_i16("SELECT CAST(10 AS SMALLINT)").await?;
    let _ = conn.query_scalar_u8("SELECT CAST(255 AS SMALLINT)").await?;
    let _ = conn.query_scalar_f32("SELECT CAST(1.25 AS REAL)").await?;
    let _ = conn
        .query_scalar_f64("SELECT CAST(1.25 AS DOUBLE PRECISION)")
        .await?;
    let _ = conn.query_scalar_bool("SELECT TRUE").await?;
    let _ = conn
        .query_scalar_dec("SELECT CAST('123.45' AS NUMERIC(10,2))")
        .await?;
    let _ = conn.query_scalar_datetime("SELECT NOW()").await?;
    let _ = conn.query_scalar_date("SELECT DATE '2023-01-01'").await?;
    let _ = conn.query_scalar_time("SELECT TIME '12:34:56'").await?;
    let _ = conn
        .query_scalar_uuid("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid")
        .await?;

    conn.begin_trans().await?;
    conn.exec(pgsql::sql_bind!(
        "INSERT INTO demo_users (id, name, age) VALUES ($1, $2, $3)",
        2,
        "Bob",
        20
    ))
    .await?;
    conn.rollback().await?;

    conn.scoped_trans(async {
        conn.exec(pgsql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES ($1, $2, $3)",
            3,
            "Carol",
            22
        ))
        .await?;
        conn.scoped_trans(async {
            conn.exec(pgsql::sql_bind!(
                "UPDATE demo_users SET age = age + 1 WHERE id = $1",
                3
            ))
            .await?;
            Ok::<(), pgsql::Error>(())
        })
        .await?;
        Ok::<(), pgsql::Error>(())
    })
    .await?;

    conn.sandbox_trans(async {
        conn.exec(pgsql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES ($1, $2, $3)",
            999,
            "Sandbox",
            1
        ))
        .await?;
        Ok::<(), pgsql::Error>(())
    })
    .await?;

    conn.exec("DROP TABLE IF EXISTS demo_copy_in").await?;
    conn.exec("CREATE TABLE demo_copy_in (id INT, name TEXT)")
        .await?;
    let chunks = vec![Ok::<_, std::io::Error>(Bytes::from("1,Alice\n2,Bob\n"))];
    let stream = futures::stream::iter(chunks);
    let _ = conn
        .copy_in(
            "COPY demo_copy_in (id, name) FROM STDIN WITH (FORMAT csv)",
            stream,
        )
        .await?;

    conn.reconnect().await?;
    Ok(())
}
#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct User {
    id: Option<i32>,
    name: Option<String>,
    age: Option<i32>,
}

async fn init_mssql() -> Result<(), mssql::Error> {
    let connection_string = conn_str_mssql();
    info!("正在尝试连接到MSSQL数据库...");
    info!(
        "连接字符串: Server={}, Database={}",
        "localhost,14333", "Salary"
    ); // 不包含密码

    let pool = Pool::builder()
        .max_size(100) // 最大连接数
        .min_idle(2) // 最小空闲连接数
        .max_lifetime(1800) // 连接最大生命周期（秒）= 30 分钟
        .idle_timeout(300) // 空闲连接超时（秒）= 5 分钟
        .connect_timeout(30) // 连接超时（秒）
        .build(&connection_string)?; // 构建连接池

    info!("成功创建连接池！");

    info!("正在获取数据库连接...");
    let _conn = pool.get().await?;
    info!("成功获取数据库连接！");
    let mut conn = pool.get().await.expect("get conn");
    conn.set_log_category("mssql");
    conn.set_log_db_name("Salary");
    let _ = conn.is_connected().await;

    conn.exec("IF OBJECT_ID('demo_users', 'U') IS NOT NULL DROP TABLE demo_users")
        .await?;
    conn.exec("CREATE TABLE demo_users (id INT NOT NULL PRIMARY KEY, name NVARCHAR(255) NOT NULL, age INT NULL)")
        .await?;
    let _ = conn.db_exists("Salary").await?;
    let _ = conn
        .query_scalar_string(mssql::sql_bind!("SELECT @P1", "password=Kephi520!"))
        .await?;
    let _ = conn
        .query_scalar_string(mssql::sql_bind!("SELECT @P1", "token=Bearer abc.def.ghi"))
        .await?;
    let _ = conn
        .query_scalar_string(mssql::sql_bind!("SELECT @P1", "phone=13812345678"))
        .await?;
    let _ = conn
        .query_scalar_string(mssql::sql_bind!("SELECT @P1", "id_card=11010519491231002X"))
        .await?;
    let _ = conn
        .query_scalar_string(mssql::sql_bind!("SELECT @P1", "account=grety@example.com"))
        .await?;
    let _ = conn.object_exists("demo_users").await?;
    let _ = conn.column_exists("demo_users", "name").await?;

    let _ = conn
        .exec_timeout(
            mssql::sql_bind!(
                "INSERT INTO demo_users (id, name, age) VALUES (@P1, @P2, @P3)",
                1,
                "Alice",
                30
            ),
            std::time::Duration::from_secs(5),
        )
        .await?;

    let mut rs = conn
        .query(mssql::sql_bind!(
            "SELECT id, name, age FROM demo_users WHERE id = @P1",
            1
        ))
        .await?;
    if let Some(row) = rs.fetch().await? {
        let _ = row.try_get_i32(0)?;
        let _ = row.try_get_str(1)?;
        let _ = row.try_get_i32(2)?;
    }
    drop(rs);

    let _rows: Vec<mssql::Row> = conn.query_collect_row("SELECT * FROM demo_users").await?;
    let _first_row = conn
        .query_first_row(mssql::sql_bind!(
            "SELECT * FROM demo_users WHERE id = @P1",
            1
        ))
        .await?;

    let _ = conn.query_scalar_i32("SELECT 1").await?;
    let _ = conn.query_scalar_i64("SELECT 1").await?;
    let _ = conn.query_scalar_string("SELECT 'abc'").await?;
    let _ = conn.query_scalar_i16("SELECT CAST(10 AS SMALLINT)").await?;
    let _ = conn.query_scalar_u8("SELECT CAST(255 AS SMALLINT)").await?;
    let _ = conn.query_scalar_f32("SELECT CAST(1.25 AS REAL)").await?;
    let _ = conn.query_scalar_f64("SELECT CAST(1.25 AS FLOAT)").await?;
    let _ = conn
        .query_scalar_dec("SELECT CAST('123.45' AS DECIMAL(10,2))")
        .await?;
    let _ = conn.query_scalar_datetime("SELECT SYSDATETIME()").await?;
    let _ = conn
        .query_scalar_date("SELECT CAST('2023-01-01' AS DATE)")
        .await?;
    let _ = conn
        .query_scalar_time("SELECT CAST('12:34:56' AS TIME)")
        .await?;
    let _ = conn
        .query_scalar_uuid(
            "SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UNIQUEIDENTIFIER)",
        )
        .await?;
    let _ = conn.query_scalar_any("SELECT CAST(123 AS INT)").await?;

    conn.exec("IF OBJECT_ID('demo_ai', 'U') IS NOT NULL DROP TABLE demo_ai")
        .await?;
    conn.exec(
        "CREATE TABLE demo_ai (id BIGINT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100) NOT NULL)",
    )
    .await?;
    conn.exec(mssql::sql_bind!(
        "INSERT INTO demo_ai (name) VALUES (@P1)",
        "auto"
    ))
    .await?;
    let _ = conn.last_identity().await?;

    conn.begin_trans().await?;
    conn.exec(mssql::sql_bind!(
        "INSERT INTO demo_users (id, name, age) VALUES (@P1, @P2, @P3)",
        2,
        "Bob",
        20
    ))
    .await?;
    conn.rollback().await?;

    conn.scoped_trans(async {
        conn.exec(mssql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES (@P1, @P2, @P3)",
            3,
            "Carol",
            22
        ))
        .await?;
        conn.scoped_trans(async {
            conn.exec(mssql::sql_bind!(
                "UPDATE demo_users SET age = age + 1 WHERE id = @P1",
                3
            ))
            .await?;
            Ok::<(), mssql::Error>(())
        })
        .await?;
        Ok::<(), mssql::Error>(())
    })
    .await?;
    conn.sandbox_trans(async {
        conn.exec(mssql::sql_bind!(
            "INSERT INTO demo_users (id, name, age) VALUES (@P1, @P2, @P3)",
            999,
            "Sandbox",
            1
        ))
        .await?;
        Ok::<(), mssql::Error>(())
    })
    .await?;

    conn.change_db("Salary").await?;
    let _ = conn.current_db();
    conn.restore_db().await?;
    conn.reconnect().await?;
    Ok(())
}
pub fn conn_str_mssql() -> String {
    let host = "localhost,1433";
    let database = "Salary";
    let user = "sa";
    let pwd = "Kephi520!";
    format!(
        "Server={host};Database={database};User Id={user};Password={pwd};Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true;"
    )
}
pub fn conn_str_pgsql() -> String {
    "postgresql://root:Kephi520!@localhost:5432/Salary".to_owned()
}

/// 为 MaskingWriter 实现 MakeWriter trait
///
/// 采用最简单的实现方式：让 MaskingWriter 自身作为 Writer 返回。
/// 这要求底层 Writer W 必须支持 Clone（NonBlocking 支持 Clone）。
impl<'a, W> tracing_subscriber::fmt::MakeWriter<'a> for MaskingWriter<W>
where
    W: std::io::Write + Clone + 'static,
{
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _span = tracing::info_span!("db_query", sql = %"SELECT * FROM userxxx").entered();

    let _guards = init_dev();
    init_mysql().await?;
    init_pgsql().await?;
    init_mssql().await?;
    Ok(())
}
