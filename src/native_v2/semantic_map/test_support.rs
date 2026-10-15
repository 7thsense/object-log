use super::codec::PAGE_BYTES;
use crate::{
    MetadataLimits,
    native_v2::{
        codec::Scope,
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    },
};
use std::sync::Arc;
mod gated_store;
pub(in crate::native_v2) use gated_store::GatedStore;
pub(super) fn scope() -> Scope {
    Scope {
        namespace: [41; 32],
        incarnation: [43; 32],
    }
}
pub(super) fn fixture<S: crate::native_v2::page_store::NativeStore + 'static>(
    local: Arc<S>,
    resources: &Arc<Resources>,
) -> Arc<PageStore> {
    PageStore::new(
        local,
        "map".into(),
        scope(),
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap()
}
pub(super) fn resources() -> Arc<Resources> {
    Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 2,
        bytes: 4 << 20,
    })
    .unwrap()
}
