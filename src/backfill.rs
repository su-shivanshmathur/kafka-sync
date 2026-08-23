//! Orchestrates the backfill loop.
//!
//! Per window and topic: drain one time-window from Kafka into an in-memory
//! buffer → upload the batch to object storage → commit the consumed
//! offsets. Upload happens strictly *before* the commit, which is what makes
//! delivery at-least-once: if the process dies between the two steps, the
//! window's messages are simply re-consumed (and re-uploaded) later.
//!
//! Failure isolation: a failing topic logs `error!` and neither kills the
//! other topics' tasks nor the process — the loop retries it on the next
//! window.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::{debug, error, info, info_span, Instrument};

use crate::config::Config;
use crate::error::BackfillError;
use crate::kafka::{ConsumedBatch, KafkaConsumer};
use crate::storage::ObjectStore;

/// Drives window cycles for every configured topic until shutdown.
pub struct Backfill {
    config: Arc<Config>,
    store: Arc<dyn ObjectStore>,
}

impl Backfill {
    pub fn new(config: Arc<Config>, store: Arc<dyn ObjectStore>) -> Self {
        Self { config, store }
    }

    /// Runs window cycles back-to-back until the shutdown signal fires.
    ///
    /// A signal that lands mid-window cancels the in-flight tasks; nothing
    /// was committed for them, so the next run re-consumes those windows
    /// (at-least-once).
    pub async fn run(&self, shutdown: Arc<Notify>) {
        info!(
            topics = ?self.config.kafka_topics,
            window_secs = self.config.sync_duration.as_secs(),
            bucket = self.config.cloud_bucket.as_str(),
            "backfill loop started"
        );
        let mut cycle: u64 = 0;
        let mut active = true;
        while active {
            cycle += 1;
            let window_start = Utc::now();
            tokio::select! {
                biased;
                _ = shutdown.notified() => {
                    info!(
                        "shutdown signal received; aborting in-flight window tasks \
                         (nothing was committed, so those windows are re-consumed on the next run)"
                    );
                    active = false;
                }
                () = self.run_window(window_start) => {
                    debug!(cycle, "window cycle complete");
                }
            }
        }
        info!("backfill loop stopped");
    }

    /// Drains one window for every topic concurrently and joins them all.
    async fn run_window(&self, window_start: DateTime<Utc>) {
        let mut tasks = JoinSet::new();
        for topic in &self.config.kafka_topics {
            let config = Arc::clone(&self.config);
            let store = Arc::clone(&self.store);
            let topic = topic.clone();
            tasks.spawn(sync_topic(config, store, topic, window_start));
        }
        while let Some(outcome) = tasks.join_next().await {
            match outcome {
                Ok(Ok(())) => (),
                Ok(Err(failed)) => {
                    error!(error = %failed, "topic window failed; retrying next cycle")
                }
                Err(join_error) => error!(error = %join_error, "topic task did not complete"),
            }
        }
    }
}

/// One full window for a single topic, traced with its own span.
async fn sync_topic(
    config: Arc<Config>,
    store: Arc<dyn ObjectStore>,
    topic: String,
    window_start: DateTime<Utc>,
) -> Result<(), BackfillError> {
    let span = info_span!("sync_topic", topic = topic.as_str(), window_start = %window_start);
    async move {
        let consumer =
            KafkaConsumer::connect(&config.kafka_brokers, &config.kafka_consumer_group, &topic)?;
        let batch = consumer.drain_window(config.sync_duration).await;
        sync_window(store.as_ref(), &consumer, &topic, window_start, batch).await
    }
    .instrument(span)
    .await
}

/// Upload-then-commit for one already-drained window.
async fn sync_window(
    store: &dyn ObjectStore,
    consumer: &KafkaConsumer,
    topic: &str,
    window_start: DateTime<Utc>,
    batch: ConsumedBatch,
) -> Result<(), BackfillError> {
    match batch.message_count {
        0 => {
            info!(topic, "no messages in window; skipping upload and commit");
            Ok(())
        }
        _ => {
            let key = object_key(topic, &window_start, &batch.partition_offsets);
            let bytes = batch.payload.len();
            let message_count = batch.message_count;
            store.put_object(&key, Bytes::from(batch.payload)).await?;
            info!(
                topic,
                key = key.as_str(),
                bytes,
                message_count,
                partitions = partition_fragment(&batch.partition_offsets).as_str(),
                "window uploaded to object storage"
            );
            consumer.commit_offsets(&batch.partition_offsets).await?;
            debug!(topic, "offsets committed");
            Ok(())
        }
    }
}

/// Deterministic object key:
/// `yyyy/mm/dd/<topic>/<window-start-millis>-Par<p>-Off<o>_... .json`
/// with partitions sorted ascending.
fn object_key(
    topic: &str,
    window_start: &DateTime<Utc>,
    partition_offsets: &BTreeMap<i32, i64>,
) -> String {
    format!(
        "{:04}/{:02}/{:02}/{}/{}-{}.json",
        window_start.year(),
        window_start.month(),
        window_start.day(),
        topic,
        window_start.timestamp_millis(),
        partition_fragment(partition_offsets)
    )
}

/// `BTreeMap` iteration order (ascending partition) makes this fragment
/// deterministic regardless of insertion order.
fn partition_fragment(partition_offsets: &BTreeMap<i32, i64>) -> String {
    partition_offsets
        .iter()
        .map(|(partition, offset)| format!("Par{partition}-Off{offset}"))
        .collect::<Vec<_>>()
        .join("_")
}
