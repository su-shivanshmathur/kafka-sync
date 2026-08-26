//! Centralized, fail-fast application configuration.
//!
//! Configuration values are layered in the following priority order
//! (highest wins), mirroring the hyperswitch `Settings` convention:
//!
//! 1. **Process environment variables** (and `.env` entries, which `dotenvy`
//!    pre-merges for `RUN_ENV=development`).
//! 2. **`config/<RUN_ENV>.toml`** — sectioned TOML selected by the `RUN_ENV`
//!    variable (defaults to `config/development.toml`); environment-specific
//!    overrides live under `config/deployments/`. The file deserializes
//!    *directly* into the typed [`FileSettings`] tree, so the file structure
//!    mirrors the Rust types — there is no stringly-typed key mapping.
//! 3. **Built-in defaults** from the constants below.
//!
//! The raw [`FileSettings`] keeps every field `Option`, so absence stays
//! distinguishable from presence until validation converts it into the
//! sealed [`Config`]/[`StorageProvider`] domain types in
//! [`Config::from_layered`]. Parsing is pure (no I/O, no panics); the
//! layering lives in [`Config::from_env`]. Anything addressing data or
//! identity (brokers, topics, consumer group) is required, and each storage
//! provider requires exactly its own settings: the service fails fast
//! instead of starting pointed at the wrong infrastructure. When no
//! provider is configured at all, the backend defaults to the local
//! filesystem under `./logs`.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::consts::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_MAX_WINDOW_BYTES, DEFAULT_RUN_ENV, DEFAULT_STORAGE_ROOT,
    DEFAULT_SYNC_DURATION_SECS, ENV_AWS_REGION, ENV_BROKERS, ENV_BUCKET, ENV_CLOUD_PATH,
    ENV_CONSUMER_GROUP, ENV_GCS_CREDENTIALS_PATH, ENV_MAX_WINDOW_BYTES, ENV_OBJECT_STORAGE,
    ENV_RUN_ENV, ENV_STORAGE_ROOT, ENV_SYNC_DURATION, ENV_TOPICS,
};
use crate::error::ConfigError;

/// Fully-validated storage backend + its settings. Each variant carries
/// exactly what its backend requires, so "provider without its config" is
/// unrepresentable (matches `OBJECT_STORAGE` / `[storage] provider`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageProvider {
    /// Local filesystem. No auth. `root` defaults to `./logs`.
    FileSystem { root: PathBuf },
    /// AWS S3 via the official SDK credential chain. `path` is a required
    /// key prefix (objects written under `s3://<bucket>/<path>/…`).
    Aws {
        bucket: String,
        path: String,
        region: String,
    },
    /// Google Cloud Storage via a service-account JSON key file.
    Gcs {
        bucket: String,
        credentials_path: PathBuf,
    },
}

impl Default for StorageProvider {
    /// The "everything absent" backend: local files under `./logs`.
    ///
    /// This is *not* the parser's fallback shortcut — the parser resolves
    /// the root inside its FileSystem arm so an explicit root is honoured
    /// even when no provider is set. `default()` merely equals the
    /// all-absent parse result.
    fn default() -> Self {
        Self::FileSystem {
            root: PathBuf::from(DEFAULT_STORAGE_ROOT),
        }
    }
}

impl std::fmt::Display for StorageProvider {
    /// Provider-agnostic target string for the startup log line. Prints
    /// only the bucket/root — never credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileSystem { root } => write!(f, "file://{}", root.display()),
            Self::Aws { bucket, path, .. } => write!(f, "s3://{bucket}/{path}"),
            Self::Gcs { bucket, .. } => write!(f, "gs://{bucket}"),
        }
    }
}

/// Fully validated application configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub run_env: String,
    pub kafka_brokers: Vec<String>,
    pub kafka_topics: Vec<String>,
    pub kafka_consumer_group: String,
    pub sync_duration: Duration,
    /// Upper bound on the in-memory payload buffer of a single topic window.
    pub max_window_bytes: usize,
    /// The single stored source of truth for a validated storage backend.
    pub storage: StorageProvider,
}

impl Config {
    /// Layers process env over `config/<RUN_ENV>.toml` over built-in
    /// defaults and validates the result (see module docs).
    ///
    /// A `.env` file is consulted when `RUN_ENV` is `development` (the
    /// default); `dotenvy` never overwrites variables already present in the
    /// process environment. A missing config file is fine (env-only
    /// deployment); an unreadable or invalid one fails fast.
    pub fn from_env() -> Result<Self, ConfigError> {
        let env_lookup = |name: &str| std::env::var(name).ok();
        let run_env =
            optional(&env_lookup, ENV_RUN_ENV).unwrap_or_else(|| DEFAULT_RUN_ENV.to_owned());
        if run_env == DEFAULT_RUN_ENV {
            drop(dotenvy::dotenv());
        }
        let file = load_config_file(&run_env)?.unwrap_or_default();
        Self::from_layered(&env_lookup, &file)
    }

    /// Builds a [`Config`] from an arbitrary env lookup and a parsed config
    /// file. This is the pure core of [`Config::from_env`]: no I/O, no
    /// panics, and no access to the process environment, which keeps the
    /// validation rules fully testable.
    pub fn from_layered(
        env: &impl Fn(&str) -> Option<String>,
        file: &FileSettings,
    ) -> Result<Config, ConfigError> {
        let run_env = optional(env, ENV_RUN_ENV).unwrap_or_else(|| DEFAULT_RUN_ENV.to_owned());

        // Data-path and identity settings are required: silently falling
        // back would start the service pointed at the wrong infrastructure.
        let kafka_brokers = required_list(
            pick(env, ENV_BROKERS, file.kafka.brokers.as_deref()),
            ENV_BROKERS,
        )?;
        let kafka_topics = required_list(
            pick(env, ENV_TOPICS, file.kafka.topics.as_deref()),
            ENV_TOPICS,
        )?;
        let kafka_consumer_group = pick(
            env,
            ENV_CONSUMER_GROUP,
            file.kafka.consumer_group.as_deref(),
        )
        .unwrap_or_else(|| DEFAULT_CONSUMER_GROUP.to_owned());

        let sync_duration = match layer_u64(env, ENV_SYNC_DURATION, file.kafka.sync_duration_secs)?
        {
            Some(0) => {
                return Err(ConfigError::InvalidValue {
                    name: ENV_SYNC_DURATION,
                    value: "0".to_owned(),
                    reason: "duration must be at least 1 second",
                });
            }
            Some(secs) => Duration::from_secs(secs),
            None => Duration::from_secs(DEFAULT_SYNC_DURATION_SECS),
        };

        let max_window_bytes =
            match layer_usize(env, ENV_MAX_WINDOW_BYTES, file.kafka.max_window_bytes)? {
                Some(0) => {
                    return Err(ConfigError::InvalidValue {
                        name: ENV_MAX_WINDOW_BYTES,
                        value: "0".to_owned(),
                        reason: "value must be at least 1",
                    });
                }
                Some(bytes) => bytes,
                None => DEFAULT_MAX_WINDOW_BYTES,
            };

        let storage = parse_storage_provider(env, &file.storage)?;

        Ok(Config {
            run_env,
            kafka_brokers,
            kafka_topics,
            kafka_consumer_group,
            sync_duration,
            max_window_bytes,
            storage,
        })
    }
}

/// Raw shape of `config/<RUN_ENV>.toml`, deserialized directly by serde:
/// each TOML section is a typed struct and each TOML key a field, in the
/// hyperswitch `Settings` style.
///
/// Every field stays `Option` — "absent" is distinguishable from "present"
/// until the fail-fast validation layer ([`Config::from_layered`]) runs,
/// where missing values either fall back to defaults or error out.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct FileSettings {
    pub kafka: KafkaSettings,
    pub storage: StorageSettings,
}

/// `[kafka]` — comma-separated lists stay `String` here so the file and the
/// env vars share the exact same splitting semantics.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct KafkaSettings {
    pub brokers: Option<String>,
    pub topics: Option<String>,
    pub consumer_group: Option<String>,
    pub sync_duration_secs: Option<u64>,
    pub max_window_bytes: Option<usize>,
}

/// `[storage]` — the provider tag plus one self-contained sub-table per
/// backend. A file may carry several providers' settings at once;
/// `provider` picks which table is validated and used.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct StorageSettings {
    pub provider: Option<String>,
    pub filesystem: FileSystemSettings,
    pub aws: AwsSettings,
    pub gcs: GcsSettings,
}

/// `[storage.filesystem]`
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct FileSystemSettings {
    /// Local root for archived objects; falls back to `./logs`.
    pub path: Option<String>,
}

/// `[storage.aws]`
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AwsSettings {
    pub bucket: Option<String>,
    /// Required key prefix; objects land under `s3://<bucket>/<path>/…`.
    pub path: Option<String>,
    pub region: Option<String>,
}

/// `[storage.gcs]`
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct GcsSettings {
    pub bucket: Option<String>,
    /// Service-account JSON key file. The secret stays on disk — only the
    /// path ever appears in config.
    pub credentials_path: Option<String>,
}

/// The raw provider tag. The configured `provider` string is parsed into
/// this exactly once (via [`FromStr`] — the single place strings are
/// matched); every downstream branch matches exhaustively on the tag, so an
/// unknown provider can only fail at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderTag {
    FileSystem,
    Aws,
    Gcs,
}

impl std::str::FromStr for ProviderTag {
    type Err = ConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.to_ascii_lowercase().as_str() {
            "filesystem" => Ok(Self::FileSystem),
            "aws" => Ok(Self::Aws),
            "gcs" => Ok(Self::Gcs),
            other => Err(ConfigError::InvalidValue {
                name: ENV_OBJECT_STORAGE,
                value: other.to_owned(),
                reason: "supported providers: FileSystem, AWS, GCS",
            }),
        }
    }
}

/// Branches on the provider tag, then demands only that variant's fields.
/// Pure: credentials/root paths are parsed as [`PathBuf`] but never touched
/// on disk here (file access is deferred to store construction).
fn parse_storage_provider(
    env: &impl Fn(&str) -> Option<String>,
    storage: &StorageSettings,
) -> Result<StorageProvider, ConfigError> {
    // The provider tag defaults to FileSystem when the key is absent.
    // Crucially the tag only selects the *arm*: the FileSystem root is
    // resolved inside that arm, so an explicit root is honoured even with no
    // provider set — we never short-circuit to `StorageProvider::default()`,
    // which would skip STORAGE_ROOT / `[storage.filesystem] path`.
    let tag = match pick(env, ENV_OBJECT_STORAGE, storage.provider.as_deref()) {
        Some(raw) => raw.parse::<ProviderTag>()?,
        None => ProviderTag::FileSystem,
    };
    match tag {
        ProviderTag::FileSystem => Ok(StorageProvider::FileSystem {
            root: pick(env, ENV_STORAGE_ROOT, storage.filesystem.path.as_deref())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_ROOT)),
        }),
        ProviderTag::Aws => {
            let bucket = required(
                pick(env, ENV_BUCKET, storage.aws.bucket.as_deref()),
                ENV_BUCKET,
            )?;
            // Required key prefix. `required` rejects whitespace-blank, but
            // a slash-*only* value is non-blank yet normalizes to nothing —
            // so it is slash-normalized right here and rejected if empty:
            // the stored value, `Display`, and the S3 keyspace all agree on
            // one form.
            let raw_path = required(
                pick(env, ENV_CLOUD_PATH, storage.aws.path.as_deref()),
                ENV_CLOUD_PATH,
            )?;
            let path = match raw_path.trim_matches('/') {
                "" => {
                    return Err(ConfigError::InvalidValue {
                        name: ENV_CLOUD_PATH,
                        value: raw_path,
                        reason: "must contain at least one non-'/' segment",
                    });
                }
                normalized => normalized.to_owned(),
            };
            let region = required(
                pick(env, ENV_AWS_REGION, storage.aws.region.as_deref()),
                ENV_AWS_REGION,
            )?;
            Ok(StorageProvider::Aws {
                bucket,
                path,
                region,
            })
        }
        ProviderTag::Gcs => Ok(StorageProvider::Gcs {
            bucket: required(
                pick(env, ENV_BUCKET, storage.gcs.bucket.as_deref()),
                ENV_BUCKET,
            )?,
            credentials_path: PathBuf::from(required(
                pick(
                    env,
                    ENV_GCS_CREDENTIALS_PATH,
                    storage.gcs.credentials_path.as_deref(),
                ),
                ENV_GCS_CREDENTIALS_PATH,
            )?),
        }),
    }
}

/// Reads `config/<run_env>.toml` if present. The file name is sanitized to
/// plain `[a-zA-Z0-9_-]` so an odd `RUN_ENV` can't turn into path traversal.
fn load_config_file(run_env: &str) -> Result<Option<FileSettings>, ConfigError> {
    let safe_name = run_env
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    match safe_name {
        false => Ok(None),
        true => {
            let path = format!("config/{run_env}.toml");
            match std::fs::read_to_string(&path) {
                Ok(contents) => toml::from_str(&contents)
                    .map(Some)
                    .map_err(|source| ConfigError::FileUnparseable { path, source }),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(source) => Err(ConfigError::FileUnreadable { path, source }),
            }
        }
    }
}

/// Resolves one scalar setting layer-by-layer: environment wins over the
/// config file, and unset/blank in either channel counts as absent.
fn pick(env: &impl Fn(&str) -> Option<String>, name: &str, file: Option<&str>) -> Option<String> {
    optional(env, name).or_else(|| {
        let trimmed = file?.trim();
        match trimmed.is_empty() {
            true => None,
            false => Some(trimmed.to_owned()),
        }
    })
}

/// Env wins over the file value; an env-provided string must parse. When
/// both are absent, `None` flows through so the caller applies its default.
fn layer_u64(
    env: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    file: Option<u64>,
) -> Result<Option<u64>, ConfigError> {
    match optional(env, name) {
        Some(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ConfigError::InvalidValue {
                name,
                value: raw.to_owned(),
                reason: "expected a positive integer",
            }),
        None => Ok(file),
    }
}

/// Same as [`layer_u64`], for `usize` knobs.
fn layer_usize(
    env: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    file: Option<usize>,
) -> Result<Option<usize>, ConfigError> {
    match optional(env, name) {
        Some(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|_| ConfigError::InvalidValue {
                name,
                value: raw.to_owned(),
                reason: "expected a positive integer",
            }),
        None => Ok(file),
    }
}

/// Looks an env var up, treating missing and blank values identically.
fn optional(env: &impl Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    env(name).and_then(|value| {
        let trimmed = value.trim();
        match trimmed.is_empty() {
            true => None,
            false => Some(trimmed.to_owned()),
        }
    })
}

/// Fails fast when a layered scalar resolved to nothing.
fn required(value: Option<String>, name: &'static str) -> Result<String, ConfigError> {
    value.ok_or(ConfigError::Missing { name })
}

/// Fails fast when no non-empty entries remain after splitting and
/// trimming a comma-separated layered scalar.
fn required_list(value: Option<String>, name: &'static str) -> Result<Vec<String>, ConfigError> {
    match value.map(|raw| parse_list(&raw)) {
        Some(entries) if !entries.is_empty() => Ok(entries),
        _ => Err(ConfigError::Missing { name }),
    }
}

/// Splits a comma-separated variable into trimmed, non-empty entries.
fn parse_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(String::from)
        .collect()
}
