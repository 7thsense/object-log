//! Bounded concurrent create-only uploads for one append.
//! Descriptors are content hashes, so everything one append references is
//! encoded first and stored together. Nothing is referenced by a head before
//! `finish` returns, and each source keeps its byte reservation until its
//! upload has completed.
use super::{OwnedDataKey, PageStore, PendingPage};
use crate::{ObjectLogError, native_v2::data::DataFrame};
use std::{future::Future, pin::Pin, task::Poll};

/// Most create-only uploads in flight at once for one append.
pub(in crate::native_v2) const UPLOAD_CONCURRENCY: usize = 8;

pub(in crate::native_v2) enum Upload {
    Page(PendingPage),
    Data { key: OwnedDataKey, frame: DataFrame },
}
impl Upload {
    async fn run(&mut self, pages: &PageStore) -> Result<(), ObjectLogError> {
        match self {
            Self::Page(page) => {
                // The reservation stays in the batch after the bytes are handed over.
                let raw = std::mem::take(&mut page.raw);
                pages.write_immutable(raw, &page.hash, &page.charge).await
            }
            Self::Data { key, frame } => pages.upload_data(key.as_str(), frame.bytes()).await,
        }
    }
}

#[derive(Default)]
pub(in crate::native_v2) struct UploadBatch {
    pending: Vec<Upload>,
}
impl UploadBatch {
    pub fn new() -> Self {
        Self::default()
    }
    /// Queue an upload, first completing the queued ones if the bound is reached.
    pub async fn push(&mut self, pages: &PageStore, upload: Upload) -> Result<(), ObjectLogError> {
        self.pending.push(upload);
        if self.pending.len() >= UPLOAD_CONCURRENCY {
            self.flush(pages).await?;
        }
        Ok(())
    }
    /// Complete every queued upload. All run to completion even if one fails;
    /// the first failure is returned.
    pub async fn finish(mut self, pages: &PageStore) -> Result<(), ObjectLogError> {
        self.flush(pages).await
    }
    async fn flush(&mut self, pages: &PageStore) -> Result<(), ObjectLogError> {
        let mut batch = std::mem::take(&mut self.pending);
        let results = join_all(batch.iter_mut().map(|upload| upload.run(pages))).await;
        // Sources and their reservations are released only now.
        drop(batch);
        results.into_iter().collect()
    }
}

async fn join_all<'a, T>(
    futures: impl Iterator<Item = impl Future<Output = T> + Send + 'a>,
) -> Vec<T> {
    let mut futures: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>> =
        futures.map(|future| Box::pin(future) as _).collect();
    let mut outputs: Vec<Option<T>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut waiting = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => waiting = true,
                }
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    outputs
        .into_iter()
        .map(|output| output.expect("completed upload"))
        .collect()
}
