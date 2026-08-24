//! Provider-agnostic object-storage seam.
//!
//! [`ObjectStore`] is the only type the orchestrator knows about; the S3
//! implementation lives in [`s3`]. Implementations are expected to be cheap
//! to share (built once, reused for every upload).

pub mod s3;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::config::{Config, StorageProvider};
use crate::error::StorageError;

/// Minimal write-only handle to object storage.
///
/// `body` is passed as [`Bytes`] so callers can hand over an in-memory
/// buffer without an extra copy.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Uploads `body` under `key`, overwriting any existing object.
    async fn put_object(&self, key: &str, body: Bytes) -> Result<(), StorageError>;
}

/// Builds the store selected by [`Config::storage_provider`].
///
/// There is exactly one provider today; each new [`StorageProvider`] variant
/// wires its implementation in here, leaving `main` and the orchestrator
/// untouched.
pub async fn from_config(config: &Config) -> Arc<dyn ObjectStore> {
    match config.storage_provider {
        StorageProvider::Aws => Arc::new(
            s3::S3Store::connect(config.cloud_bucket.clone(), config.aws_region.clone()).await,
        ),
    }
}
