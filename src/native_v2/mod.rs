//! Work-in-progress append-only captured-root index; not a public backend.
pub(crate) mod codec;
mod data;
pub(crate) mod head;
mod index;
mod operation;
mod page_store;
