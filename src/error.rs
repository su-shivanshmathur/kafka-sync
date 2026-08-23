//! Error types for the application, one enum per module.
//!
//! Library-style modules ([`crate::config`], [`crate::kafka`],
//! [`crate::storage`], [`crate::telemetry`], [`crate::backfill`]) define
//! concrete, matchable errors with [`thiserror`]. The application boundary
//! ([`main`]) converts them into [`anyhow::Error`] for uniform reporting.
//!
//! No constructor in this module ever panics: sources are stored verbatim so
//! callers can inspect the full causal chain.

use thiserror::Error;

/// Failures while loading or validating the process configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required environment variable is unset or empty.
    #[error("environment variable {name} must be set to a non-empty value")]
    Missing { name: &'static str },
    /// An environment variable holds a value that failed validation.
    #[error("environment variable {name} has invalid value {value:?}: {reason}")]
    InvalidValue {
        name: &'static str,
        value: String,
        reason: &'static str,
    },
}

/// Failures while installing the logging/telemetry pipeline.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// The default log filter could not be parsed.
    #[error("invalid built-in log filter directive")]
    InvalidFilter(#[from] tracing_subscriber::filter::ParseError),
    /// A global tracing subscriber is already installed.
    #[error("failed to install the global tracing subscriber")]
    SetGlobalDefault(#[from] tracing::dispatcher::SetGlobalDefaultError),
}

/// Failures reported by the Kafka ingestion layer.
#[derive(Debug, Error)]
pub enum KafkaError {
    /// The `StreamConsumer` could not be created from the client config.
    #[error("failed to create consumer for topic {topic:?}")]
    Create {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    /// Subscribing to the topic failed.
    #[error("failed to subscribe to topic {topic:?}")]
    Subscribe {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    /// A partition/offset pair could not be added to the commit list.
    #[error("failed to build the offset commit list for topic {topic:?}")]
    CommitList {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    /// The broker rejected or failed the offset commit.
    #[error("failed to commit consumed offsets for topic {topic:?}")]
    Commit {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    /// The blocking offset-commit task did not complete normally.
    #[error("blocking commit task for topic {topic:?} failed to join")]
    Join {
        topic: String,
        #[source]
        source: tokio::task::JoinError,
    },
}

/// Failures reported by the object-storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Uploading an object to the bucket failed.
    #[error("failed to put object s3://{bucket}/{key} ({size} bytes)")]
    PutObject {
        bucket: String,
        key: String,
        size: usize,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Failures while orchestrating a single backfill window.
#[derive(Debug, Error)]
pub enum BackfillError {
    /// The Kafka stage of the window failed.
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    /// The object-storage stage of the window failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}
