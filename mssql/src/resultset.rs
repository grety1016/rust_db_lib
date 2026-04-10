//!
//! 查询结果集
//! 字段下标从0开始
//!

use crate::{Error, Result, Row};
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub struct ResultSet<'con> {
    rs: Option<tiberius::QueryStream<'con>>,
    meta: tiberius::ResultMetadata,
    alive: &'con AtomicBool,
}

impl<'con> Stream for ResultSet<'con> {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let rs = match self.rs.as_mut() {
            Some(s) => s,
            None => return Poll::Ready(None),
        };

        loop {
            match Pin::new(&mut *rs).poll_next(cx) {
                Poll::Ready(Some(Ok(tiberius::QueryItem::Row(row)))) => {
                    return Poll::Ready(Some(Ok(Row::new(row))));
                },
                Poll::Ready(Some(Ok(_))) => {
                    continue;
                },
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(Error::QueryError(e))));
                },
                Poll::Ready(None) => {
                    self.alive.store(false, Ordering::SeqCst);
                    self.rs = None;
                    return Poll::Ready(None);
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<'con> ResultSet<'con> {
    pub(crate) async fn new(
        mut rs: tiberius::QueryStream<'con>,
        alive: &'con AtomicBool,
    ) -> Result<ResultSet<'con>> {
        alive.store(true, Ordering::SeqCst);
        if let Some(tiberius::QueryItem::Metadata(meta)) = rs.try_next().await? {
            Ok(ResultSet {
                rs: Some(rs),
                meta,
                alive,
            })
        } else {
            alive.store(false, Ordering::SeqCst);
            Err(Error::InvalidMeta)
        }
    }
}

impl<'con> ResultSet<'con> {
    #[inline]
    pub fn column_count(&self) -> u16 {
        self.meta.columns().len() as u16
    }
    #[inline]
    pub fn column_name(&self, idx: u16) -> &str {
        self.meta.columns()[idx as usize].name()
    }
    #[inline]
    pub fn column_index(&self, name: &str) -> Option<u16> {
        self.meta.columns().iter().position(|c| name.eq_ignore_ascii_case(c.name())).map(|i| i as u16)
    }
    #[inline]
    pub fn column_exists(&self, name: &str) -> bool {
        self.column_index(name).is_some()
    }
    #[inline]
    pub async fn fetch(&mut self) -> Result<Option<Row>> {
        self.next().await.transpose()
    }
    #[inline]
    pub async fn first_row(self) -> Result<Option<Row>> {
        //FIXME 需要获取完整结果集，因为如果发生`RAISE ERROR`没有被捕获，则会在流(`QueryStream`)的后面
        self.collect().await.map(|mut r| {
            if r.is_empty() {
                None
            } else {
                Some(r.remove(0))
            }
        })
    }
    #[inline]
    pub async fn collect(mut self) -> Result<Vec<Row>> {
        if let Some(rs) = self.rs.take() {
            rs.into_first_result()
                .await
                .map(|r| r.into_iter().map(Row::new).collect())
                .map_err(Error::QueryError)
        } else {
            Ok(vec![])
        }
    }
}

impl<'con> Drop for ResultSet<'con> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}
