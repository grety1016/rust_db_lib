use crate::{connection::RawGuard, row::Row, Result};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// 结果集
pub struct ResultSet<'a> {
    stream: Option<futures_util::stream::BoxStream<'a, Result<mysql_async::Row>>>,
    alive: &'a AtomicBool,
    _guard: RawGuard<'a>,
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
        stream: futures_util::stream::BoxStream<'a, Result<mysql_async::Row>>,
        alive: &'a AtomicBool,
        guard: RawGuard<'a>,
    ) -> Self {
        alive.store(true, Ordering::SeqCst);
        Self {
            stream: Some(stream),
            alive,
            _guard: guard,
        }
    }

    pub async fn fetch(&mut self) -> Result<Option<Row>> {
        self.next().await.transpose()
    }

    pub async fn first_row(&mut self) -> Result<Option<Row>> {
        self.fetch().await
    }

    pub async fn collect_row(&mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        while let Some(row) = self.fetch().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub async fn collect(mut self) -> Result<Vec<Row>> {
        self.collect_row().await
    }

    pub async fn scalar_string(&mut self) -> Result<Option<String>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_string(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i64(&mut self) -> Result<Option<i64>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i64(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i32(&mut self) -> Result<Option<i32>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i32(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_i16(&mut self) -> Result<Option<i16>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_i16(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_u8(&mut self) -> Result<Option<u8>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_u8(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_f64(&mut self) -> Result<Option<f64>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_f64(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_f32(&mut self) -> Result<Option<f32>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_f32(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_bool(&mut self) -> Result<Option<bool>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_bool(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_decimal(&mut self) -> Result<Option<rust_decimal::Decimal>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_decimal(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_datetime(&mut self) -> Result<Option<chrono::NaiveDateTime>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_datetime(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_date(&mut self) -> Result<Option<chrono::NaiveDate>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_date(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_time(&mut self) -> Result<Option<chrono::NaiveTime>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_time(0)
        } else {
            Ok(None)
        }
    }

    pub async fn scalar_uuid(&mut self) -> Result<Option<uuid::Uuid>> {
        if let Some(row) = self.first_row().await? {
            row.try_get_uuid(0)
        } else {
            Ok(None)
        }
    }
}

impl<'a> Drop for ResultSet<'a> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}
