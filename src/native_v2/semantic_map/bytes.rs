//! Canonical inline/overflow bytes; missing chunks never become absent values.
use super::{
    WorkBudget,
    codec::{self, ByteRef, CHUNK_BYTES},
};
use crate::{
    ObjectLogError,
    native_v2::page_store::{Charge, PageStore},
};
use std::sync::Arc;

pub(in crate::native_v2) struct OwnedBytes {
    bytes: Vec<u8>,
    _charge: Arc<Charge>,
}
impl OwnedBytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
    pub fn copy(
        pages: &PageStore,
        value: &[u8],
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        pages.require_operation()?;
        work.hydrate(value.len() as u64)?;
        let allocation = value
            .len()
            .checked_add(std::mem::size_of::<Self>())
            .ok_or_else(|| {
                ObjectLogError::BudgetExceeded("semantic byte allocation overflow".into())
            })?;
        let charge = pages.charge_output(allocation)?;
        Ok(Self {
            bytes: value.to_vec(),
            _charge: charge,
        })
    }
}

pub(super) async fn store(
    pages: &PageStore,
    value: &[u8],
    work: &mut WorkBudget,
) -> Result<ByteRef, ObjectLogError> {
    pages.require_operation()?;
    if value.len() <= 32 {
        return ByteRef::inline(value);
    }
    let mut next = ByteRef::inline(&[])?;
    for chunk in value.chunks(CHUNK_BYTES).rev() {
        if next.len() == 0 && chunk.len() <= 32 {
            next = ByteRef::inline(chunk)?;
        } else {
            work.page()?;
            let (encoded, reference) = codec::blob(pages, chunk, next)?;
            pages.write_scoped_bytes(encoded).await?;
            next = reference;
        }
    }
    Ok(next)
}

pub(super) async fn load(
    pages: &PageStore,
    reference: ByteRef,
    work: &mut WorkBudget,
) -> Result<OwnedBytes, ObjectLogError> {
    pages.require_operation()?;
    let len = usize::try_from(reference.len())
        .map_err(|_| codec::invalid("byte-reference platform length"))?;
    let allocation = len
        .checked_add(std::mem::size_of::<OwnedBytes>())
        .ok_or_else(|| {
            ObjectLogError::BudgetExceeded("semantic byte allocation overflow".into())
        })?;
    work.hydrate(reference.len())?;
    let charge = pages.charge_output(allocation)?;
    let mut result = OwnedBytes {
        bytes: Vec::with_capacity(len),
        _charge: charge,
    };
    let mut cursor = reference;
    while let Some(hash) = cursor.hash() {
        work.page()?;
        let page = pages
            .read_scoped_bytes(&hash, cursor.encoded_page_len()?)
            .await?;
        let (chunk, next) = codec::blob_view(page.bytes(), pages.scope(), cursor)?;
        result.bytes.extend_from_slice(chunk);
        cursor = next;
    }
    result
        .bytes
        .extend_from_slice(cursor.inline_bytes().expect("canonical inline terminal"));
    if result.bytes.len() != len {
        return Err(codec::invalid("semantic byte result length"));
    }
    Ok(result)
}
