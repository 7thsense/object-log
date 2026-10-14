//! Metadata responses are validated before SDK deserialization/body collection.
use super::*;
use crate::blob::authority::{MetadataVersion, validate_budget};
use crate::{AppendOwner, BoundedMetadataStore, MetadataSnapshot, MetadataSwap};
use std::sync::{Arc, atomic::AtomicU16};

// Hyper can retain the source body after the SDK response future is dropped.
// The bytes precede their owner so source payload cleanup releases it last.
struct OwnedMetadataBody {
    bytes: Bytes,
    _physical: AppendOwner,
}
impl AsRef<[u8]> for OwnedMetadataBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
struct MetadataResponse {
    status: Arc<AtomicU16>,
    max_bytes: usize,
    reading: bool,
}
impl aws_sdk_s3::config::Intercept for MetadataResponse {
    fn name(&self) -> &'static str {
        "BoundedMetadataResponse"
    }
    fn read_before_transmit(
        &self,
        _context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &aws_sdk_s3::config::RuntimeComponents,
        _config: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.status.store(0, Ordering::Relaxed);
        Ok(())
    }
    fn read_before_deserialization(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef<'_>,
        _runtime: &aws_sdk_s3::config::RuntimeComponents,
        _config: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = context.response();
        let status = response.status().as_u16();
        self.status.store(status, Ordering::Relaxed);
        if (self.reading && status == 404) || (!self.reading && status == 412) {
            // Classify by status without letting error XML collect a body.
            return Err(Box::new(unavailable("metadata conditional status")));
        }
        if response.headers().get("transfer-encoding").is_some()
            || response.headers().get("content-encoding").is_some()
        {
            return Err(Box::new(unavailable(
                "unsupported metadata response framing",
            )));
        }
        let length = response
            .headers()
            .get("content-length")
            .and_then(|s| s.parse::<usize>().ok());
        let expected_status = if self.reading {
            status == 200
        } else {
            status == 200 || status == 409
        };
        // Only a bounded 409 error may enter SDK parsing (to distinguish its
        // exact ConditionalRequestConflict code from unrelated conflicts).
        let limit = if status == 409 { 4096 } else { self.max_bytes };
        if !expected_status || length.is_none_or(|n| n > limit) {
            return Err(Box::new(unavailable(
                "invalid or oversized metadata response",
            )));
        }
        if self.reading && response.headers().get("etag").is_none_or(str::is_empty) {
            return Err(Box::new(unavailable("metadata response lacks version")));
        }
        Ok(())
    }
}
impl S3BlobStore {
    async fn read_metadata_inner(
        &self,
        key: &str,
        max_bytes: usize,
        physical: AppendOwner,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        validate_budget(max_bytes, 0)?;
        let status = Arc::new(AtomicU16::new(0));
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .customize()
            .config_override(
                aws_sdk_s3::config::Builder::new()
                    .retry_config(
                        aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                    )
                    .interceptor(MetadataResponse {
                        status: Arc::clone(&status),
                        max_bytes,
                        reading: true,
                    }),
            );
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(_) if status.load(Ordering::Relaxed) == 404 => {
                return Ok(MetadataSnapshot {
                    _physical: Arc::clone(&physical),
                    owner: Arc::clone(&self.authority_identity),
                    key: key.to_owned(),
                    bytes: None,
                    version: MetadataVersion::S3(None),
                });
            }
            Err(error) => return Err(unavailable(error)),
        };
        let size = response
            .content_length()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| unavailable("invalid metadata length"))?;
        validate_budget(max_bytes, size)?;
        let etag = response
            .e_tag()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| unavailable("metadata version missing"))?
            .to_owned();
        let mut bytes = Vec::with_capacity(size);
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk.map_err(unavailable)?;
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|n| n > size)
            {
                return Err(unavailable("metadata body exceeds declared bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != size {
            return Err(unavailable("metadata body is truncated"));
        }
        Ok(MetadataSnapshot {
            _physical: Arc::clone(&physical),
            owner: Arc::clone(&self.authority_identity),
            key: key.to_owned(),
            bytes: Some(Bytes::from(bytes)),
            version: MetadataVersion::S3(Some(etag)),
        })
    }
}
#[async_trait]
impl BoundedMetadataStore for S3BlobStore {
    async fn read_metadata(
        &self,
        key: &str,
        max_bytes: usize,
        physical: AppendOwner,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        self.read_metadata_inner(key, max_bytes, Arc::clone(&physical))
            .await
    }
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        new_value: Bytes,
        max_bytes: usize,
        physical: AppendOwner,
    ) -> Result<MetadataSwap, ObjectLogError> {
        observed.validate(&self.authority_identity, key)?;
        validate_budget(max_bytes, new_value.len())?;
        validate_budget(max_bytes, observed.bytes.as_ref().map_or(0, Bytes::len))?;
        let MetadataVersion::S3(etag) = observed.version else {
            return Err(ObjectLogError::InvalidConfig(
                "non-S3 metadata token".into(),
            ));
        };
        let size = new_value.len() as u64;
        let request =
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(Bytes::from_owner(OwnedMetadataBody {
                    bytes: new_value,
                    _physical: Arc::clone(&physical),
                })));
        let request = match etag {
            Some(etag) => request.if_match(etag),
            None => request.if_none_match("*"),
        };
        let status = Arc::new(AtomicU16::new(0));
        let request = request.customize().config_override(
            aws_sdk_s3::config::Builder::new()
                .retry_config(
                    aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                )
                .interceptor(MetadataResponse {
                    status: Arc::clone(&status),
                    max_bytes: 0,
                    reading: false,
                }),
        );
        let result = if self.disable_payload_signing {
            request.disable_payload_signing().send().await
        } else {
            request.send().await
        };
        match result {
            Ok(_) => {
                self.record_media(1, size);
                Ok(MetadataSwap::Stored)
            }
            Err(error)
                if status.load(Ordering::Relaxed) == 412 || s3_precondition_failed(&error) =>
            {
                // Failure/oversize here remains unknown, never a head rejection.
                Ok(MetadataSwap::Conflict(
                    self.read_metadata_inner(key, max_bytes, Arc::clone(&physical))
                        .await?,
                ))
            }
            Err(error) => Err(unavailable(error)),
        }
    }
}
