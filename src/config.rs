//! Centralized, fail-fast application configuration.
//!
//! Every setting is read exactly once in [`Config::from_env`]; the rest of the
//! program consumes a typed [`Config`] value. Parsing is pure (no I/O, no
//! panics) via [`Config::from_lookup`], which keeps validation unit-testable
//! without mutating the process environment.

use std::time::Duration;

use crate::error::ConfigError;

const ENV_ENVIRONMENT: &str = "ENVIRONMENT";
const ENV_BROKERS: &str = "KAFKA_BROKERS";
const ENV_TOPICS: &str = "KAFKA_TOPICS";
const ENV_CONSUMER_GROUP: &str = "KAFKA_CONSUMER_GROUP";
const ENV_SYNC_DURATION: &str = "SYNC_DURATION";
const ENV_MAX_WINDOW_BYTES: &str = "KAFKA_MAX_WINDOW_BYTES";
const ENV_OBJECT_STORAGE: &str = "OBJECT_STORAGE";
const ENV_BUCKET: &str = "CLOUD_BUCKET";
const ENV_AWS_REGION: &str = "AWS_REGION";

const DEFAULT_ENVIRONMENT: &str = "local";
const DEFAULT_BROKERS: &str = "localhost:9092";
const DEFAULT_CONSUMER_GROUP: &str = "kafka-sync";
const DEFAULT_SYNC_DURATION_SECS: u64 = 10;
/// Bounds the in-memory payload buffer of one topic window (256 MiB).
const DEFAULT_MAX_WINDOW_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_OBJECT_STORAGE: &str = "AWS";
const DEFAULT_BUCKET: &str = "eu-kafka-backfill-bucket";
const DEFAULT_AWS_REGION: &str = "eu-central-1";

/// Environment name that enables loading a `.env` file at startup.
pub const LOCAL_ENVIRONMENT: &str = "local";

/// Supported object-storage providers (matches the `OBJECT_STORAGE` env var).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    Aws,
}

/// Fully validated application configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub environment: String,
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
    /// Loads the configuration from the process environment.
    ///
    /// A `.env` file is consulted first when the environment is `local`
    /// (the default when `ENVIRONMENT` is unset); `dotenv` never overwrites
    /// variables already present in the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        // Use the same blank-is-missing normalization as the rest of the
        // parsing, so `ENVIRONMENT=" "` doesn't silently suppress `.env`
        // loading while later being treated as "local".
        let env_lookup = |name: &str| std::env::var(name).ok();
        let preloaded = optional(&env_lookup, ENV_ENVIRONMENT)
            .unwrap_or_else(|| DEFAULT_ENVIRONMENT.to_owned());
        if preloaded == LOCAL_ENVIRONMENT {
            drop(dotenvy::dotenv());
        }
        Self::from_lookup(env_lookup)
    }

    /// Builds a [`Config`] from arbitrary key/value pairs. This is the pure
    /// core of [`Config::from_env`] and never touches the process
    /// environment, which keeps the validation rules fully testable.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let environment =
            optional(&lookup, ENV_ENVIRONMENT).unwrap_or_else(|| DEFAULT_ENVIRONMENT.to_owned());

        let kafka_brokers = parse_list(&required_or(&lookup, ENV_BROKERS, DEFAULT_BROKERS));
        let kafka_topics = parse_list(&env_or_empty(&lookup, ENV_TOPICS));
        let kafka_topics = match kafka_topics.is_empty() {
            true => Err(ConfigError::Missing { name: ENV_TOPICS }),
            false => Ok(kafka_topics),
        }?;

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

        let cloud_bucket = required_or(&lookup, ENV_BUCKET, DEFAULT_BUCKET);
        let aws_region = required_or(&lookup, ENV_AWS_REGION, DEFAULT_AWS_REGION);

        Ok(Config {
            environment,
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

/// Looks `name` up, falling back to `default` when missing or blank.
fn required_or(lookup: &impl Fn(&str) -> Option<String>, name: &str, default: &str) -> String {
    optional(lookup, name).unwrap_or_else(|| default.to_owned())
}

fn env_or_empty(lookup: &impl Fn(&str) -> Option<String>, name: &str) -> String {
    optional(lookup, name).unwrap_or_default()
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
