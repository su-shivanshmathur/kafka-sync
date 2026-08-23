//! Kafka ingestion built on `rdkafka`'s async [`StreamConsumer`].
//!
//! `rdkafka` drives its I/O on librdkafka's own background threads and
//! integrates with Tokio through wakeups, so the executor is never blocked.
//! The only call that can park on the network is the broker-acknowledged
//! offset commit, which is therefore wrapped in
//! [`tokio::task::spawn_blocking`].
//!
//! Delivery semantics are at-least-once: auto-commit is disabled and offsets
//! are committed explicitly by the orchestrator *after* a successful upload.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::time::{timeout_at, Instant as TokioInstant};
use tracing::{debug, warn};

use crate::error::KafkaError;

/// Backoff applied after a consumption error so a broken broker does not
/// turn into a busy loop for the rest of the window.
const CONSUME_ERROR_BACKOFF: Duration = Duration::from_millis(250);

/// Messages accumulated during one backfill window plus the highest consumed
/// offset per partition (iteration order is deterministic).
#[derive(Debug, Default)]
pub struct ConsumedBatch {
    /// Newline-delimited raw message values.
    pub payload: Vec<u8>,
    /// Number of consumed records (tombstones and empty values included).
    pub message_count: usize,
    /// Partition → highest consumed offset.
    pub partition_offsets: BTreeMap<i32, i64>,
}

/// Owns the consumer for a single topic and time window.
///
/// A fresh consumer is created per window: group membership, rebalances, and
/// recovery from broker failures are then entirely librdkafka's problem,
/// while remaining crash-safe (uncommitted offsets are simply re-consumed).
pub struct KafkaConsumer {
    topic: String,
    consumer: Arc<StreamConsumer>,
}

impl KafkaConsumer {
    /// Creates and subscribes the consumer. Construction does not perform
    /// blocking network I/O; librdkafka resolves the brokers on its
    /// background threads.
    pub fn connect(brokers: &[String], group: &str, topic: &str) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .set("group.id", group)
            .set("client.id", "kafka-sync")
            .set("enable.partition.eof", "false")
            // Offsets are committed explicitly after a successful upload.
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            // Start from the beginning when no committed offset exists.
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|source| KafkaError::Create {
                topic: topic.to_owned(),
                source,
            })?;
        consumer
            .subscribe(&[topic])
            .map_err(|source| KafkaError::Subscribe {
                topic: topic.to_owned(),
                source,
            })?;
        Ok(Self {
            topic: topic.to_owned(),
            consumer: Arc::new(consumer),
        })
    }

    /// Drains the topic until `window` elapses, accumulating raw payloads.
    ///
    /// Consumption errors are logged and retried until the window closes
    /// rather than failing the window outright: at-least-once semantics keep
    /// any committed-safe data re-consumable on the next window.
    pub async fn drain_window(&self, window: Duration) -> ConsumedBatch {
        let deadline = TokioInstant::now() + window;
        let mut batch = ConsumedBatch::default();
        loop {
            let received = timeout_at(deadline, self.consumer.recv()).await;
            match received {
                Err(_elapsed) => break,
                Ok(Err(err)) => {
                    warn!(
                        topic = self.topic.as_str(),
                        error = %err,
                        "message consumption error; retrying until the window closes"
                    );
                    tokio::time::sleep(CONSUME_ERROR_BACKOFF).await;
                }
                Ok(Ok(message)) => {
                    if let Some(value) = message.payload() {
                        batch.payload.extend_from_slice(value);
                        batch.payload.push(b'\n');
                    }
                    batch.message_count += 1;
                    batch
                        .partition_offsets
                        .entry(message.partition())
                        .and_modify(|offset| *offset = (*offset).max(message.offset()))
                        .or_insert(message.offset());
                }
            }
        }
        debug!(
            topic = self.topic.as_str(),
            messages = batch.message_count,
            bytes = batch.payload.len(),
            "window drained"
        );
        batch
    }

    /// Commits the *next* offset (highest consumed + 1) for every consumed
    /// partition and waits for the broker to acknowledge.
    ///
    /// The synchronous commit may park on the network, so it runs on the
    /// blocking thread pool; `librdkafka` clients are thread-safe, so sharing
    /// the consumer is sound.
    pub async fn commit_offsets(
        &self,
        partition_offsets: &BTreeMap<i32, i64>,
    ) -> Result<(), KafkaError> {
        match partition_offsets.is_empty() {
            true => Ok(()),
            false => {
                let commit_list = build_commit_list(&self.topic, partition_offsets)?;
                let consumer = Arc::clone(&self.consumer);
                let topic = self.topic.clone();
                let joined = tokio::task::spawn_blocking(move || {
                    consumer.commit(&commit_list, CommitMode::Sync)
                })
                .await
                .map_err(|source| KafkaError::Join {
                    topic: topic.clone(),
                    source,
                })?;
                joined.map_err(|source| KafkaError::Commit { topic, source })
            }
        }
    }
}

fn build_commit_list(
    topic: &str,
    partition_offsets: &BTreeMap<i32, i64>,
) -> Result<TopicPartitionList, KafkaError> {
    let mut commit_list = TopicPartitionList::new();
    let mut failure = Ok(());
    for (partition, offset) in partition_offsets {
        if failure.is_ok() {
            // Committed offsets are the *next* message to consume.
            failure = commit_list
                .add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
                .map_err(|source| KafkaError::CommitList {
                    topic: topic.to_owned(),
                    source,
                });
        }
    }
    failure.map(|()| commit_list)
}
