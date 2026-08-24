//! Centralized, fail-fast application configuration.
//!
//! Configuration values are layered in the following priority order
//! (highest wins), mirroring the hyperswitch `Settings` convention:
//!
//! 1. **Process environment variables** (and `.env` entries, which `dotenvy`
//!    pre-merges for `RUN_ENV=development`).
//! 2. **`config/<RUN_ENV>.toml`** — sectioned TOML selected by the `RUN_ENV`
//!    variable (defaults to `config/development.toml`); environment-specific
//!    overrides live under `config/deployments/`.
//! 3. **Built-in defaults** from the constants below.
//!
//! Parsing is pure (no I/O, no panics) via [`Config::from_lookup`]; the
//! layering lives in [`Config::from_env`]. Anything addressing data or
//! identity (brokers, topics, consumer group, bucket, region) is required:
//! the service fails fast instead of starting pointed at the wrong
//! infrastructure.

use std::time::Duration;

use crate::consts::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_MAX_WINDOW_BYTES, DEFAULT_OBJECT_STORAGE, DEFAULT_RUN_ENV,
    DEFAULT_SYNC_DURATION_SECS, ENV_AWS_REGION, ENV_BROKERS, ENV_BUCKET, ENV_CONSUMER_GROUP,
    ENV_MAX_WINDOW_BYTES, ENV_OBJECT_STORAGE, ENV_RUN_ENV, ENV_SYNC_DURATION, ENV_TOPICS,
};
use crate::error::ConfigError;

/// Supported object-storage providers (matches `OBJECT_STORAGE` /
/// `storage.provider` in the config file).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    Aws,
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
    pub storage_provider: StorageProvider,
    pub cloud_bucket: String,
    pub aws_region: String,
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
        let table = load_config_file(&run_env)?;
        let layered = |name: &str| env_lookup(name).or_else(|| lookup_toml(&table, name));
        Self::from_lookup(layered)
    }

    /// Builds a [`Config`] from arbitrary key/value pairs. This is the pure
    /// core of [`Config::from_env`] and never touches the process
    /// environment, which keeps the validation rules fully testable.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let run_env = optional(&lookup, ENV_RUN_ENV).unwrap_or_else(|| DEFAULT_RUN_ENV.to_owned());

        // Data-path and identity settings are required: silently falling
        // back would start the service pointed at the wrong infrastructure.
        let kafka_brokers = required_list(&lookup, ENV_BROKERS)?;
        let kafka_topics = required_list(&lookup, ENV_TOPICS)?;
        let kafka_consumer_group = required_or(&lookup, ENV_CONSUMER_GROUP, DEFAULT_CONSUMER_GROUP);

        let raw_duration = match optional(&lookup, ENV_SYNC_DURATION) {
            Some(value) => value,
            None => DEFAULT_SYNC_DURATION_SECS.to_string(),
        };
        let sync_duration = parse_duration_secs(ENV_SYNC_DURATION, &raw_duration)?;

        let raw_max_bytes = match optional(&lookup, ENV_MAX_WINDOW_BYTES) {
            Some(value) => value,
            None => DEFAULT_MAX_WINDOW_BYTES.to_string(),
        };
        let max_window_bytes = parse_positive_usize(ENV_MAX_WINDOW_BYTES, &raw_max_bytes)?;

        let raw_provider = required_or(&lookup, ENV_OBJECT_STORAGE, DEFAULT_OBJECT_STORAGE);
        let storage_provider = parse_storage_provider(&raw_provider)?;

        // Infrastructure-specific: no defaults baked into the binary.
        let cloud_bucket = required(&lookup, ENV_BUCKET)?;
        let aws_region = required(&lookup, ENV_AWS_REGION)?;

        Ok(Config {
            run_env,
            kafka_brokers,
            kafka_topics,
            kafka_consumer_group,
            sync_duration,
            max_window_bytes,
            storage_provider,
            cloud_bucket,
            aws_region,
        })
    }
}

/// Maps an env-var name to its `(section, key)` in the TOML file.
/// `RUN_ENV` intentionally has no mapping: it selects the file itself.
fn toml_key(env_name: &str) -> Option<(&'static str, &'static str)> {
    match env_name {
        ENV_BROKERS => Some(("kafka", "brokers")),
        ENV_TOPICS => Some(("kafka", "topics")),
        ENV_CONSUMER_GROUP => Some(("kafka", "consumer_group")),
        ENV_SYNC_DURATION => Some(("kafka", "sync_duration_secs")),
        ENV_MAX_WINDOW_BYTES => Some(("kafka", "max_window_bytes")),
        ENV_OBJECT_STORAGE => Some(("storage", "provider")),
        ENV_BUCKET => Some(("storage", "bucket")),
        ENV_AWS_REGION => Some(("storage", "region")),
        _ => None,
    }
}

/// Reads `config/<run_env>.toml` if present. The file name is sanitized to
/// plain `[a-zA-Z0-9_-]` so an odd `RUN_ENV` can't turn into path traversal.
fn load_config_file(run_env: &str) -> Result<Option<toml::Table>, ConfigError> {
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

/// Resolves `env_name` from the parsed TOML table, accepting strings and
/// (for the numeric knobs) integers.
fn lookup_toml(table: &Option<toml::Table>, env_name: &str) -> Option<String> {
    let (section, key) = toml_key(env_name)?;
    table
        .as_ref()
        .and_then(|table| table.get(section))
        .and_then(|section| section.get(key))
        .and_then(|value| match value {
            toml::Value::String(text) => Some(text.clone()),
            toml::Value::Integer(number) => Some(number.to_string()),
            _ => None,
        })
}

/// Looks `name` up, treating missing and blank values identically.
fn optional(lookup: &impl Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name).and_then(|value| {
        let trimmed = value.trim();
        match trimmed.is_empty() {
            true => None,
            false => Some(trimmed.to_owned()),
        }
    })
}

/// Looks `name` up, failing fast when missing or blank.
fn required(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
) -> Result<String, ConfigError> {
    match optional(lookup, name) {
        Some(value) => Ok(value),
        None => Err(ConfigError::Missing { name }),
    }
}

/// Looks a comma-separated variable up, failing fast when no non-empty
/// entries remain after splitting and trimming.
fn required_list(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
) -> Result<Vec<String>, ConfigError> {
    match optional(lookup, name).map(|raw| parse_list(&raw)) {
        Some(entries) if !entries.is_empty() => Ok(entries),
        _ => Err(ConfigError::Missing { name }),
    }
}

/// Looks `name` up, falling back to `default` when missing or blank.
fn required_or(lookup: &impl Fn(&str) -> Option<String>, name: &str, default: &str) -> String {
    optional(lookup, name).unwrap_or_else(|| default.to_owned())
}

/// Splits a comma-separated variable into trimmed, non-empty entries.
fn parse_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(String::from)
        .collect()
}

fn parse_positive_usize(name: &'static str, raw: &str) -> Result<usize, ConfigError> {
    raw.parse::<usize>()
        .map_err(|_| ConfigError::InvalidValue {
            name,
            value: raw.to_owned(),
            reason: "expected a positive integer",
        })
        .and_then(|value| match value {
            0 => Err(ConfigError::InvalidValue {
                name,
                value: raw.to_owned(),
                reason: "value must be at least 1",
            }),
            valid => Ok(valid),
        })
}

fn parse_duration_secs(name: &'static str, raw: &str) -> Result<Duration, ConfigError> {
    raw.parse::<u64>()
        .map_err(|_| ConfigError::InvalidValue {
            name,
            value: raw.to_owned(),
            reason: "expected a positive integer number of seconds",
        })
        .and_then(|secs| match secs {
            0 => Err(ConfigError::InvalidValue {
                name,
                value: raw.to_owned(),
                reason: "duration must be at least 1 second",
            }),
            valid => Ok(Duration::from_secs(valid)),
        })
}

fn parse_storage_provider(raw: &str) -> Result<StorageProvider, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "aws" => Ok(StorageProvider::Aws),
        _ => Err(ConfigError::InvalidValue {
            name: ENV_OBJECT_STORAGE,
            value: raw.to_owned(),
            reason: "supported providers: AWS",
        }),
    }
}
