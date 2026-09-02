//! S3 implementation of [`ObjectStore`] on the official AWS SDK for Rust.
//!
//! The [`aws_sdk_s3::Client`] is constructed exactly once at startup and
//! shared immutably for the whole process lifetime — no credentials or HTTP
//! client per upload (the SDK client is already backed by shared
//! connectors).

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::error::StorageError;
use crate::storage::ObjectStore;

/// S3-backed object store with a process-lifetime SDK client.
pub struct S3Store {
    client: Client,
    bucket: String,
    /// Required key prefix, normalized to have no leading/trailing `/`.
    prefix: String,
    /// Precomputed `s3://<bucket>/<prefix>` for cheap error context.
    location: String,
}

impl S3Store {
    /// Builds the shared client from the default AWS config chain
    /// (env → profile → SSO → IMDS/ECS) with the configured region.
    ///
    /// Credentials resolve lazily on the first request, so construction is
    /// infallible; misconfiguration surfaces as a [`StorageError`] on the
    /// first upload instead of a startup panic.
    pub async fn connect(
        bucket: impl Into<String>,
        path: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.into()))
            .load()
            .await;
        let bucket = bucket.into();
        // `path` is validated and slash-normalized at config time (see
        // `parse_storage_provider`), so the normalized prefix is always
        // present; the trim here is an idempotent guard for direct callers.
        let prefix = path.into().trim_matches('/').to_owned();
        let location = format!("s3://{bucket}/{prefix}");
        Self {
            client: Client::new(&shared),
            bucket,
            prefix,
            location,
        }
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn put_object(&self, key: &str, body: Bytes) -> Result<(), StorageError> {
        let size = body.len();
        // The backend owns its own addressing: the required prefix is
        // prepended here, keeping `backfill::object_key` backend-agnostic.
        let full_key = format!("{}/{}", self.prefix, key);
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(ByteStream::from(body))
            .send()
            .await;
        match sent {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::PutObject {
                // `key` (not `full_key`): `location` already carries the
                // prefix, so the error composes to
                // "s3://<bucket>/<prefix>/<key>" exactly once.
                location: self.location.clone(),
                key: key.to_owned(),
                size,
                source: Box::new(err),
            }),
        }
    }
}
