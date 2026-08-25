//! Filesystem implementation of [`ObjectStore`]: the zero-config local
//! backend (default when no cloud provider is configured).
//!
//! Writes honour the same "clean overwrite on retry" contract as S3:
//! bytes land in a sibling temp file first and are then atomically renamed
//! into place, so a crash mid-write can never leave a torn object behind.
//! Object keys are internally generated but embed the topic name (external
//! input), so every key passes through [`safe_join`] before touching disk.

use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::error::StorageError;
use crate::storage::ObjectStore;

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

/// Joins `key` onto `root` without ever escaping it.
///
/// `Path::join` with an absolute component *discards the base*
/// (`root.join("/etc/passwd") == /etc/passwd`), and `..` walks out of the
/// root — so any key that isn't made of plain `Normal` (or `.`) components
/// is rejected outright instead of sanitized into something surprising.
fn safe_join(root: &Path, key: &str) -> Result<PathBuf, StorageError> {
    let rel = Path::new(key);
    let traversal = rel
        .components()
        .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir));
    match traversal {
        true => Err(StorageError::InvalidKey {
            key: key.to_owned(),
        }),
        false => Ok(root.join(rel)),
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

        // Temp sibling + atomic rename == S3's all-or-nothing PUT. The
        // `<final>.<pid>.tmp` name needs no `tempfile` runtime dep: windows
        // are sequential and each (topic, window) yields one distinct key,
        // so no two in-flight puts target the same temp path (and
        // `File::create` truncates, making a stale `.tmp` from a prior
        // crash a harmless overwrite).
        let mut tmp = final_path.clone().into_os_string();
        tmp.push(format!(".{}.tmp", std::process::id()));
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

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    async fn store() -> Result<(tempfile::TempDir, FileSystemStore), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let store = FileSystemStore::connect(dir.path()).await?;
        Ok((dir, store))
    }

    #[tokio::test]
    async fn put_object_writes_exact_bytes_under_root() -> TestResult {
        let (dir, store) = store().await?;
        store
            .put_object("a/b/c.json", Bytes::from_static(b"payload"))
            .await?;
        let bytes = std::fs::read(dir.path().join("a/b/c.json"))?;
        assert_eq!(bytes, b"payload");
        Ok(())
    }

    #[tokio::test]
    async fn put_object_overwrites_existing_key() -> TestResult {
        let (dir, store) = store().await?;
        store
            .put_object("k.json", Bytes::from_static(b"first"))
            .await?;
        // Retry semantics: same key, replaced content, no error.
        store
            .put_object("k.json", Bytes::from_static(b"second"))
            .await?;
        let bytes = std::fs::read(dir.path().join("k.json"))?;
        assert_eq!(bytes, b"second");
        Ok(())
    }

    #[tokio::test]
    async fn traversal_keys_are_rejected_and_write_nothing() -> TestResult {
        let (dir, store) = store().await?;
        for key in ["../escape.json", "/abs.json", "a/../../escape.json"] {
            let outcome = store.put_object(key, Bytes::from_static(b"x")).await;
            assert!(
                matches!(outcome, Err(StorageError::InvalidKey { key: ref rejected }) if rejected == key),
                "key {key:?} must be rejected as InvalidKey, got {outcome:?}"
            );
        }
        // Nothing escaped the root, and no stray files were left inside it.
        let parent = dir.path().parent().ok_or("tempdir has no parent")?;
        assert!(!parent.join("escape.json").exists());
        assert!(std::fs::read_dir(dir.path())?.next().is_none());
        Ok(())
    }

    #[test]
    fn safe_join_accepts_the_real_window_key_shape() -> TestResult {
        let root = Path::new("/root");
        let joined = safe_join(root, "2026/08/26/events/Par1-Off42.json")?;
        assert_eq!(joined, root.join("2026/08/26/events/Par1-Off42.json"));
        let dot_prefixed = safe_join(root, "./events/x.json")?;
        assert_eq!(dot_prefixed, root.join("./events/x.json"));
        Ok(())
    }
}
