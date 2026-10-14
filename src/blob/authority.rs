//! Strict metadata authority. Legacy whole-object operations are not fallbacks.
use super::*;
use crate::AppendOwner;
use std::os::unix::fs::MetadataExt;

#[cfg(test)]
pub(crate) struct LocalMetadataHook {
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    panic: bool,
}
#[cfg(test)]
impl LocalBlobStore {
    pub(crate) fn pause_metadata_swap(
        &self,
        panic: bool,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, started) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        *self.authority_hook.lock().unwrap() = Some(Arc::new(LocalMetadataHook {
            entered,
            release: Mutex::new(wait),
            panic,
        }));
        (started, release)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalVersion {
    device: u64,
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl From<&std::fs::Metadata> for LocalVersion {
    fn from(value: &std::fs::Metadata) -> Self {
        Self {
            device: value.dev(),
            inode: value.ino(),
            size: value.len(),
            modified: (value.mtime(), value.mtime_nsec()),
            changed: (value.ctime(), value.ctime_nsec()),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MetadataVersion {
    Local(Option<LocalVersion>),
    #[cfg(feature = "s3")]
    S3(Option<String>),
}
/// An opaque observation binding one adapter instance, literal key and version.
/// Payload ownership remains with the snapshot; callers reserve payload, key,
/// token and transport scratch before invoking the physical operation.
#[derive(Clone)]
pub struct MetadataSnapshot {
    pub(crate) owner: Arc<()>,
    pub(crate) key: String,
    pub(crate) bytes: Option<Bytes>,
    pub(crate) version: MetadataVersion,
    // Last: release payload/key/version allocations before their lifetime owner.
    pub(crate) _physical: AppendOwner,
}
impl std::fmt::Debug for MetadataSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSnapshot")
            .field("key", &self.key)
            .field("payload_bytes", &self.bytes.as_ref().map(Bytes::len))
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}
impl MetadataSnapshot {
    /// Exact borrowed bytes, or absence. Cloning the snapshot retains its owner;
    /// raw Bytes aliases cannot escape that lifetime through this accessor.
    ///
    /// ```
    /// use object_log::MetadataSnapshot;
    /// fn borrow(snapshot: &MetadataSnapshot) -> Option<&[u8]> { snapshot.bytes() }
    /// ```
    /// ```compile_fail
    /// use object_log::MetadataSnapshot;
    /// fn escape(snapshot: MetadataSnapshot) -> bytes::Bytes {
    ///     snapshot.bytes().unwrap().clone()
    /// }
    /// ```
    pub fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
    pub(crate) fn validate(&self, owner: &Arc<()>, key: &str) -> Result<(), ObjectLogError> {
        if !Arc::ptr_eq(&self.owner, owner) || self.key != key {
            return Err(ObjectLogError::InvalidConfig(
                "metadata token belongs to another provider or literal key".into(),
            ));
        }
        Ok(())
    }
}
/// A definite metadata conditional-write outcome. Transport errors can follow
/// durable publication and must never be interpreted as definite rejection.
#[derive(Debug)]
pub enum MetadataSwap {
    /// The proposed bytes were durably stored.
    Stored,
    /// The observed version did not match; contains a bounded fresh observation.
    Conflict(MetadataSnapshot),
}
/// Required bounded authority operations, independent of legacy BlobStore.
/// There are deliberately no default implementations. `max_bytes` bounds every
/// retained payload, including conflict observations; transport/header and
/// operation ownership require separate caller admission. Cancellation does not
/// prove a conditional write had no effects. Owners must drive and drain it.
/// Input/output source buffers retain the supplied lifetime owner. This does not
/// attest to all SDK transport allocations or provider-side effects after an
/// ambiguous request; those require separate transport/durability qualification.
#[async_trait]
pub trait BoundedMetadataStore: Send + Sync {
    /// Read one complete object within the admitted bound, with an opaque CAS
    /// token. Oversize/malformed responses fail before whole-body collection.
    async fn read_metadata(
        &self,
        key: &str,
        max_bytes: usize,
        owner: AppendOwner,
    ) -> Result<MetadataSnapshot, ObjectLogError>;
    /// Conditional durable replacement of exactly the observed provider/key/
    /// version. No automatic conditional-write retry. Oversized input is rejected
    /// before I/O; failed conflict rereads are errors, never definite rejection.
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        new_value: Bytes,
        max_bytes: usize,
        owner: AppendOwner,
    ) -> Result<MetadataSwap, ObjectLogError>;
}
pub(crate) fn validate_budget(max_bytes: usize, size: usize) -> Result<(), ObjectLogError> {
    if max_bytes == 0 {
        return Err(ObjectLogError::InvalidConfig(
            "metadata budget is zero".into(),
        ));
    }
    if size > max_bytes {
        return Err(ObjectLogError::BudgetExceeded(
            "metadata exceeds admitted payload bound".into(),
        ));
    }
    Ok(())
}
fn read_local(
    path: &Path,
    owner: Arc<()>,
    key: String,
    max_bytes: usize,
    physical: AppendOwner,
) -> Result<MetadataSnapshot, ObjectLogError> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MetadataSnapshot {
                _physical: physical,
                owner,
                key,
                bytes: None,
                version: MetadataVersion::Local(None),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(ObjectLogError::StorageUnavailable(
            "metadata is not a file".into(),
        ));
    }
    let size = usize::try_from(metadata.len())
        .map_err(|_| ObjectLogError::BudgetExceeded("metadata exceeds address space".into()))?;
    validate_budget(max_bytes, size)?;
    let version = LocalVersion::from(&metadata);
    let mut bytes = vec![0; size];
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 || LocalVersion::from(&file.metadata()?) != version {
        return Err(ObjectLogError::StorageUnavailable(
            "metadata changed during read".into(),
        ));
    }
    Ok(MetadataSnapshot {
        _physical: physical,
        owner,
        key,
        bytes: Some(Bytes::from(bytes)),
        version: MetadataVersion::Local(Some(version)),
    })
}
#[async_trait]
impl BoundedMetadataStore for LocalBlobStore {
    async fn read_metadata(
        &self,
        key: &str,
        max_bytes: usize,
        owner: AppendOwner,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        validate_budget(max_bytes, 0)?;
        let path = self.path_for(key)?;
        let physical = owner;
        let owner = Arc::clone(&self.authority_identity);
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let _physical = physical;
            let path = path;
            read_local(&path, owner, key, max_bytes, Arc::clone(&_physical))
        })
        .await
        .map_err(|error| ObjectLogError::StorageUnavailable(error.to_string()))?
    }
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        new_value: Bytes,
        max_bytes: usize,
        owner: AppendOwner,
    ) -> Result<MetadataSwap, ObjectLogError> {
        observed.validate(&self.authority_identity, key)?;
        validate_budget(max_bytes, new_value.len())?;
        validate_budget(max_bytes, observed.bytes.as_ref().map_or(0, Bytes::len))?;
        let path = self.path_for(key)?;
        let physical = owner;
        let owner = Arc::clone(&self.authority_identity);
        let key = key.to_owned();
        let size = new_value.len() as u64;
        #[cfg(test)]
        let hook = self.authority_hook.lock().unwrap().take();
        let result =
            tokio::task::spawn_blocking(move || -> Result<MetadataSwap, ObjectLogError> {
                let _physical = physical;
                let path = path;
                let key = key;
                let observed = observed;
                let new_value = new_value;
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut lock_name = path.as_os_str().to_os_string();
                lock_name.push(".caslock");
                let lock = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(lock_name)?;
                flock_exclusive(&lock)?;
                // Closing the file releases flock on every error/panic path.
                let current =
                    read_local(&path, owner, key.clone(), max_bytes, Arc::clone(&_physical))?;
                if current.version != observed.version || current.bytes != observed.bytes {
                    return Ok(MetadataSwap::Conflict(current));
                }
                #[cfg(test)]
                if let Some(hook) = hook {
                    hook.entered.send(()).unwrap();
                    hook.release.lock().unwrap().recv().unwrap();
                    assert!(!hook.panic, "injected physical metadata panic");
                }
                LocalBlobStore::durable_publish_chunks(path, vec![new_value], &key)?;
                Ok(MetadataSwap::Stored)
            })
            .await
            .map_err(|error| ObjectLogError::StorageUnavailable(error.to_string()))??;
        if matches!(result, MetadataSwap::Stored) {
            self.record_put(size);
        }
        Ok(result)
    }
}
