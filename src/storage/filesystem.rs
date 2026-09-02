//! Filesystem implementation of [`ObjectStore`]: the zero-config local
//! backend (default when no cloud provider is configured).
//!
//! Writes honour the same "clean overwrite on retry" contract as S3:
//! bytes land in a sibling temp file first and are then atomically renamed
//! into place, so a crash mid-write can never leave a torn object behind.
//! Object keys are internally generated but embed the topic name (external
//! input), so every key passes through [`safe_join`] before touching disk.

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::error::StorageError;
use crate::storage::ObjectStore;

/// Per-write sequence for temp-file names (`<final>.<pid>.<seq>.tmp`):
/// concurrent writes to the same final path never share a temp file, so
/// write+rename stays atomic by construction.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Local-filesystem object store rooted at a configured directory.
pub struct FileSystemStore {
    root: PathBuf,
    /// Precomputed `file://<root>` for cheap error context.
    location: String,
}

impl FileSystemStore {
    /// Creates the store, materializing `root` (and any missing parents).
    /// Failing fast here — instead of on the first upload — is what lets
    /// `main` report a bad/missing mount as a startup error.
    pub async fn connect(root: &Path) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(root)
            .await
            .map_err(|source| StorageError::Init {
                provider: "FileSystem",
                source: Box::new(source),
            })?;
        Ok(Self {
            root: root.to_path_buf(),
            location: format!("file://{}", root.display()),
        })
    }
}

/// Joins `key` onto `root` without ever escaping it: only a non-empty
/// all-`Normal` key is accepted (`Path::join` with an absolute component
/// discards the base, and `..` walks out of the root).
fn safe_join(root: &Path, key: &str) -> Result<PathBuf, StorageError> {
    let valid = !key.is_empty()
        && Path::new(key)
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
    match valid {
        true => Ok(root.join(key)),
        false => Err(StorageError::InvalidKey {
            key: key.to_owned(),
        }),
    }
}

#[async_trait]
impl ObjectStore for FileSystemStore {
    async fn put_object(&self, key: &str, body: Bytes) -> Result<(), StorageError> {
        // `InvalidKey` is permanent by design: an unsafe key fails every
        // retry deterministically (logged per window, isolated per topic)
        // rather than ever escaping the root.
        let final_path = safe_join(&self.root, key)?;
        // `body` is moved into the writer below, so anything the error
        // paths still need (here: `size`) is computed *before* the move.
        let size = body.len();
        let put_error = |source: std::io::Error| StorageError::PutObject {
            location: self.location.clone(),
            key: key.to_owned(),
            size,
            source: Box::new(source),
        };

        if let Some(parent) = final_path.parent() {
            // Concurrent topics share date prefixes; `create_dir_all` treats
            // AlreadyExists as success, so the parallel window tasks don't
            // fight.
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(&put_error)?;
        }

        // Temp sibling + atomic rename == S3's all-or-nothing PUT; the
        // per-write name (see `TMP_SEQ`) keeps a stale `.tmp` from ever
        // racing a new write.
        let mut tmp = final_path.clone().into_os_string();
        tmp.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let tmp = PathBuf::from(tmp);

        let mut file = tokio::fs::File::create(&tmp).await.map_err(&put_error)?;
        file.write_all(&body).await.map_err(&put_error)?;
        drop(file);

        // Atomic on a single filesystem.
        tokio::fs::rename(&tmp, &final_path)
            .await
            .map_err(put_error)
    }
}
