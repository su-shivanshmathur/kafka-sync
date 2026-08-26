//! Environment-variable names and built-in defaults for optional settings.

pub const ENV_RUN_ENV: &str = "RUN_ENV";
pub const ENV_BROKERS: &str = "KAFKA_BROKERS";
pub const ENV_TOPICS: &str = "KAFKA_TOPICS";
pub const ENV_CONSUMER_GROUP: &str = "KAFKA_CONSUMER_GROUP";
pub const ENV_SYNC_DURATION: &str = "SYNC_DURATION";
pub const ENV_MAX_WINDOW_BYTES: &str = "KAFKA_MAX_WINDOW_BYTES";
pub const ENV_OBJECT_STORAGE: &str = "OBJECT_STORAGE";
pub const ENV_BUCKET: &str = "CLOUD_BUCKET";
pub const ENV_AWS_REGION: &str = "AWS_REGION";
/// Root directory of the FileSystem storage backend.
pub const ENV_STORAGE_ROOT: &str = "STORAGE_ROOT";
/// Service-account JSON key file for the GCS backend.
pub const ENV_GCS_CREDENTIALS_PATH: &str = "GCS_CREDENTIALS_PATH";
/// Required S3 key prefix (objects land under `s3://<bucket>/<path>/…`).
pub const ENV_CLOUD_PATH: &str = "CLOUD_PATH";

/// Selects `config/<RUN_ENV>.toml` and enables `.env` loading.
pub const DEFAULT_RUN_ENV: &str = "development";

pub const DEFAULT_SYNC_DURATION_SECS: u64 = 10;
pub const DEFAULT_CONSUMER_GROUP: &str = "kafka-sync";
/// Bounds the in-memory payload buffer of one topic window (256 MiB).
pub const DEFAULT_MAX_WINDOW_BYTES: usize = 256 * 1024 * 1024;
/// Root used by the FileSystem backend when no `STORAGE_ROOT` is configured.
/// Consumed by both the parser's FileSystem arm and
/// `StorageProvider::default()`; the default *provider* is the `Default`
/// impl's chosen variant, not a string constant.
pub const DEFAULT_STORAGE_ROOT: &str = "./logs";
