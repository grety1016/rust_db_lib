use crate::{
    row::{ColumnData, Row},
    Error, Result,
};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// 结果集
pub struct ResultSet<'a> {
    stream: Option<futures_util::stream::BoxStream<'a, Result<tokio_postgres::Row>>>,
    alive: &'a AtomicBool,
    _guard: tokio::sync::MutexGuard<'a, tokio_postgres::Client>,
}

impl<'a> Stream for ResultSet<'a> {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Poll::Ready(None),
        };

        match Pin::new(stream).poll_next(cx) {
            Poll::Ready(Some(res)) => Poll::Ready(Some(res.map(Row::new))),
            Poll::Ready(None) => {
                self.alive.store(false, Ordering::SeqCst);
                self.stream = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a> ResultSet<'a> {
    pub(crate) fn new(
        stream: futures_util::stream::BoxStream<'a, Result<tokio_postgres::Row>>,
        alive: &'a AtomicBool,
        guard: tokio::sync::MutexGuard<'a, tokio_postgres::Client>,
    ) -> Self {
        alive.store(true, Ordering::SeqCst);
        Self {
            stream: Some(stream),
            alive,
            _guard: guard,
        }
    }

    /// 获取一行数据移动到下一行
    pub async fn fetch(&mut self) -> Result<Option<Row>> {
        self.next().await.transpose()
    }

    /// 获取第一行
    pub async fn first_row(&mut self) -> Result<Option<Row>> {
        self.fetch().await
    }

    /// 收集所有行数据
    pub async fn collect_row(&mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        while let Some(row) = self.fetch().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// 获取标量值(字符串)
    pub async fn scalar_string(&mut self) -> Result<Option<String>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_string(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(u8)
    pub async fn scalar_u8(&mut self) -> Result<Option<u8>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_u8(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(i16)
    pub async fn scalar_i16(&mut self) -> Result<Option<i16>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_i16(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(i32)
    pub async fn scalar_i32(&mut self) -> Result<Option<i32>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_i32(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(i64)
    pub async fn scalar_i64(&mut self) -> Result<Option<i64>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_i64(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(f32)
    pub async fn scalar_f32(&mut self) -> Result<Option<f32>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_f32(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(f64)
    pub async fn scalar_f64(&mut self) -> Result<Option<f64>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_f64(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(bool)
    pub async fn scalar_bool(&mut self) -> Result<Option<bool>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_bool(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(Decimal)
    pub async fn scalar_dec(&mut self) -> Result<Option<rust_decimal::Decimal>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_decimal(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(DateTime)
    pub async fn scalar_datetime(&mut self) -> Result<Option<chrono::NaiveDateTime>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_datetime(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(Date)
    pub async fn scalar_date(&mut self) -> Result<Option<chrono::NaiveDate>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_date(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(Time)
    pub async fn scalar_time(&mut self) -> Result<Option<chrono::NaiveTime>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_time(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(UUID)
    pub async fn scalar_uuid(&mut self) -> Result<Option<uuid::Uuid>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_uuid(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 获取标量值(任意类型)
    pub async fn scalar_any(&mut self) -> Result<Option<ColumnData>> {
        self.first_row()
            .await?
            .map(|r| r.try_get_any(0))
            .transpose()
            .map(|v| v.flatten())
    }

    /// 查询并反序列化为指定类型
    pub async fn collect<R>(mut self) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let rows = self.collect_row().await?;
        let de = crate::serde::RowCollection::new(rows);
        R::deserialize(de).map_err(|e| Error::Deserialization(e.to_string()))
    }

    /// 查询首行并反序列化为指定类型
    pub async fn first<R>(&mut self) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let row = self.first_row().await?;
        let de = crate::serde::RowOptional::new(row);
        R::deserialize(de).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

impl<'a> Drop for ResultSet<'a> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}
