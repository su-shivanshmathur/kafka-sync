//! Google Cloud Storage implementation of [`ObjectStore`] on the
//! `gcloud-storage` crate (`Client` / `upload_object`), authenticated with
//! an explicit service-account JSON key file.
//!
//! Like the S3 backend, the authenticated client is built exactly once in
//! [`GcsStore::connect`] and shared immutably for the whole process
//! lifetime: the client owns the underlying HTTP client and refreshes
//! expired tokens internally, so uploads never rebuild either.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use gcloud_auth::credentials::CredentialsFile;
use gcloud_storage::client::{Client, ClientConfig};
use gcloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

use crate::error::StorageError;
use crate::storage::ObjectStore;

/// GCS-backed object store with a process-lifetime, pre-authenticated
/// client.
pub struct GcsStore {
    client: Client,
    bucket: String,
    /// Precomputed `gs://<bucket>` for cheap error context.
    location: String,
}

impl GcsStore {
    /// Builds the client from an explicit service-account JSON file. Any
    /// failure (unreadable/invalid key file, auth setup) surfaces as a
    /// startup [`StorageError::Init`] instead of a first-upload surprise.
    pub async fn connect(bucket: &str, credentials_path: &Path) -> Result<Self, StorageError> {
        // `new_from_file` takes a String path; config paths originate from
        // UTF-8 TOML/env, but fail loudly on a non-UTF-8 path rather than
        // lossily convert.
        let path = credentials_path
            .to_str()
            .ok_or_else(|| StorageError::Init {
                provider: "GCS",
                source: format!("non-UTF-8 credentials path {credentials_path:?}").into(),
            })?;
        let creds = CredentialsFile::new_from_file(path.to_owned())
            .await
            .map_err(|source| StorageError::Init {
                provider: "GCS",
                source: Box::new(source),
            })?;
        let config = ClientConfig::default()
            .with_credentials(creds)
            .await
            .map_err(|source| StorageError::Init {
                provider: "GCS",
                source: Box::new(source),
            })?;
        Ok(Self {
            client: Client::new(config),
            bucket: bucket.to_owned(),
            location: format!("gs://{bucket}"),
        })
    }
}

#[async_trait]
impl ObjectStore for GcsStore {
    /// Single-request (simple) media upload; the object name is our key.
    async fn put_object(&self, key: &str, body: Bytes) -> Result<(), StorageError> {
        // `body` is moved into the uploader below; `size` is needed on the
        // error path, so compute it *before* the move.
        let size = body.len();
        let request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };
        let media = Media::new(key.to_owned());
        self.client
            .upload_object(&request, body, &UploadType::Simple(media))
            .await
            .map(|_object| ())
            .map_err(|source| StorageError::PutObject {
                location: self.location.clone(),
                key: key.to_owned(),
                size,
                source: Box::new(source),
            })
    }
}
