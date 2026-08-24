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

/// Selects `config/<RUN_ENV>.toml` and enables `.env` loading.
pub const DEFAULT_RUN_ENV: &str = "development";

pub const DEFAULT_SYNC_DURATION_SECS: u64 = 10;
pub const DEFAULT_CONSUMER_GROUP: &str = "kafka-sync";
/// Bounds the in-memory payload buffer of one topic window (256 MiB).
pub const DEFAULT_MAX_WINDOW_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_OBJECT_STORAGE: &str = "AWS";
