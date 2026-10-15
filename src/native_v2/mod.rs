//! Bounded captured-root append publication and authenticated semantic maps.
pub(crate) mod codec;
mod data;
pub(crate) mod head;
mod index;
mod operation;
mod page_store;
mod provision;
mod semantic_map;
mod submission;

#[cfg(feature = "native")]
pub(crate) mod facade;
