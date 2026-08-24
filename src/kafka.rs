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

/// Modest initial payload buffer: avoids the doubling chain of
/// reallocations for small/medium windows without committing `max_bytes`
/// worth of memory up front.
const INITIAL_PAYLOAD_CAPACITY: usize = 64 * 1024;

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

/// Owns the long-lived `StreamConsumer` for a single topic.
///
/// One consumer per topic is kept for the process lifetime: staying joined
/// to the group between windows avoids a join/rebalance storm per window,
/// and librdkafka's background polling keeps membership and rebalances
/// flowing even while no window is draining. Closing is explicit via
/// [`KafkaConsumer::shutdown`] because dropping can busy-poll.
pub struct KafkaConsumer {
    topic: String,
    consumer: Arc<StreamConsumer>,
}

impl KafkaConsumer {
    /// Creates and subscribes the consumer. Construction does not perform
    /// blocking network I/O; librdkafka resolves the brokers on its
    /// background threads.
    pub fn connect(brokers: &[String], group: &str, topic: &str) -> Result<Self, KafkaError> {
        // Build the list once; `ClientConfig::set` takes `&str` values.
        let bootstrap_servers = brokers.join(",");
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers.as_str())
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

    /// Drains the topic until `window` elapses or `max_bytes` of payload has
    /// accumulated, whichever comes first. The byte cap bounds process memory
    /// on high-throughput topics; a capped window simply closes early and the
    /// *already consumed* offsets are uploaded and committed, so the next
    /// window resumes exactly where this one stopped.
    ///
    /// Consumption errors are logged and retried until the window closes
    /// rather than failing the window outright: at-least-once semantics keep
    /// any committed-safe data re-consumable on the next window.
    pub async fn drain_window(&self, window: Duration, max_bytes: usize) -> ConsumedBatch {
        let deadline = TokioInstant::now() + window;
        let mut batch = ConsumedBatch {
            payload: Vec::with_capacity(INITIAL_PAYLOAD_CAPACITY),
            ..ConsumedBatch::default()
        };
        loop {
            if batch.payload.len() >= max_bytes {
                warn!(
                    topic = self.topic.as_str(),
                    bytes = batch.payload.len(),
                    max_bytes,
                    "window payload cap reached; closing the window early"
                );
                break;
            }
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
    /// partition that is **still assigned** to this consumer, and waits for
    /// the broker to acknowledge.
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
                let commit_list =
                    build_commit_list(&self.consumer, &self.topic, partition_offsets)?;
                let consumer = Arc::clone(&self.consumer);
                let topic = self.topic.clone();
                match tokio::task::spawn_blocking(move || {
                    consumer.commit(&commit_list, CommitMode::Sync)
                })
                .await
                {
                    Ok(result) => result.map_err(|source| KafkaError::Commit { topic, source }),
                    Err(source) => Err(KafkaError::Join { topic, source }),
                }
            }
        }
    }

    /// Closes the consumer off the async runtime.
    ///
    /// `librdkafka` completes the group-leave handshake by *polling*, so
    /// dropping a `StreamConsumer` busy-polls for at least one ~100 ms cycle.
    /// Doing that on a Tokio worker would stall every task it shares the
    /// worker with; the blocking pool exists precisely for this.
    pub async fn shutdown(self) {
        let _closed = tokio::task::spawn_blocking(move || drop(self.consumer)).await;
    }
}

/// Builds the commit list from the consumed offsets, intersected with the
/// **current** assignment: a partition revoked mid-window now belongs to
/// another group member, and committing our view of it would silently skip
/// messages the new owner has already consumed from the last commit.
fn build_commit_list(
    consumer: &StreamConsumer,
    topic: &str,
    partition_offsets: &BTreeMap<i32, i64>,
) -> Result<TopicPartitionList, KafkaError> {
    let assignment = consumer
        .assignment()
        .map_err(|source| KafkaError::Assignment {
            topic: topic.to_owned(),
            source,
        })?;
    let mut commit_list = TopicPartitionList::new();
    let mut failure = Ok(());
    for (partition, offset) in partition_offsets {
        let still_assigned = assignment
            .elements()
            .iter()
            .any(|elem| elem.topic() == topic && elem.partition() == *partition);
        let stage = match still_assigned {
            // Committed offsets are the *next* message to consume.
            true => commit_list
                .add_partition_offset(topic, *partition, Offset::Offset(offset.saturating_add(1)))
                .map_err(|source| KafkaError::CommitList {
                    topic: topic.to_owned(),
                    source,
                }),
            false => {
                warn!(
                    topic,
                    partition = *partition,
                    "partition revoked mid-window; leaving its offsets to the new owner"
                );
                Ok(())
            }
        };
        if failure.is_ok() {
            failure = stage;
        }
    }
    failure.map(|()| commit_list)
}
