//! Provider-agnostic object-storage seam.
//!
//! [`ObjectStore`] is the only type the orchestrator knows about; the
//! implementations live in [`filesystem`], [`s3`], and [`gcs`], each
//! holding its process-lifetime SDK client. Construction is fallible
//! ([`StorageError::Init`]): a backend that can't write fails at startup
//! rather than on the first upload.

pub mod filesystem;
pub mod gcs;
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

/// Builds the store selected by [`Config::storage`].
///
/// Construction performs backend-specific I/O (FileSystem mkdir, GCS
/// credential load), which is why it returns a [`Result`]: a backend that
/// can't initialize fails fast here. Each new [`StorageProvider`] variant
/// wires its implementation in here, leaving `main` and the orchestrator
/// untouched.
pub async fn from_config(config: &Config) -> Result<Arc<dyn ObjectStore>, StorageError> {
    match &config.storage {
        StorageProvider::FileSystem { root } => {
            Ok(Arc::new(filesystem::FileSystemStore::connect(root).await?))
        }
        StorageProvider::Aws {
            bucket,
            path,
            region,
        } => Ok(Arc::new(
            s3::S3Store::connect(bucket.clone(), path.clone(), region.clone()).await,
        )),
        StorageProvider::Gcs {
            bucket,
            credentials_path,
        } => Ok(Arc::new(
            gcs::GcsStore::connect(bucket, credentials_path).await?,
        )),
    }
}
