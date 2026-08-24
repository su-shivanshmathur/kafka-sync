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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

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
///
/// One [`KafkaConsumer`] per topic is kept for the whole lifetime: the
/// group membership survives between windows, so there is no join/rebalance
/// per window and no executor-blocking consumer drop per window either.
pub struct Backfill {
    config: Arc<Config>,
    store: Arc<dyn ObjectStore>,
    consumers: Arc<Mutex<HashMap<String, Arc<KafkaConsumer>>>>,
}

impl Backfill {
    pub fn new(config: Arc<Config>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            store,
            consumers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Runs window cycles back-to-back until the shutdown signal fires.
    ///
    /// A signal that lands mid-window cancels the in-flight tasks; nothing
    /// was committed for them, so the next run re-consumes those windows
    /// (at-least-once). Consumers stay in the shared cache, so their
    /// executor-blocking drop never happens inside an aborted task.
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
        info!("backfill loop stopped; closing group members");
        self.shutdown_members().await;
    }

    /// Drains one window for every topic concurrently and joins them all.
    async fn run_window(&self, window_start: DateTime<Utc>) {
        let mut tasks = JoinSet::new();
        for topic in &self.config.kafka_topics {
            let config = Arc::clone(&self.config);
            let store = Arc::clone(&self.store);
            let consumers = Arc::clone(&self.consumers);
            let topic = topic.clone();
            tasks.spawn(sync_topic(config, store, consumers, topic, window_start));
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

    /// Closes every cached group member on the blocking pool, so no
    /// consumer drop busy-polls the runtime during process exit.
    async fn shutdown_members(&self) {
        let members = {
            let mut guard = match self.consumers.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            std::mem::take(&mut *guard)
        };
        for (_topic, member) in members {
            match Arc::try_unwrap(member) {
                Ok(consumer) => consumer.shutdown().await,
                Err(_shared) => {
                    debug!("consumer still referenced by an aborted task; skipping group-leave")
                }
            }
        }
    }
}

/// One full window for a single topic, traced with its own span.
async fn sync_topic(
    config: Arc<Config>,
    store: Arc<dyn ObjectStore>,
    consumers: Arc<Mutex<HashMap<String, Arc<KafkaConsumer>>>>,
    topic: String,
    window_start: DateTime<Utc>,
) -> Result<(), BackfillError> {
    let span = info_span!("sync_topic", topic = topic.as_str(), window_start = %window_start);
    async move {
        let consumer = cached_consumer(&consumers, &config, &topic)?;
        let batch = consumer
            .drain_window(config.sync_duration, config.max_window_bytes)
            .await;
        sync_window(
            store.as_ref(),
            consumer.as_ref(),
            &topic,
            window_start,
            batch,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Returns the cached consumer for `topic`, connecting on first use (and
/// retrying after a previously failed attempt). Failed connects surface as
/// that window's topic error; the next window retries.
fn cached_consumer(
    consumers: &Mutex<HashMap<String, Arc<KafkaConsumer>>>,
    config: &Config,
    topic: &str,
) -> Result<Arc<KafkaConsumer>, crate::error::KafkaError> {
    let cached = {
        let guard = match consumers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(topic).cloned()
    };
    match cached {
        Some(consumer) => Ok(consumer),
        None => {
            let consumer = Arc::new(KafkaConsumer::connect(
                &config.kafka_brokers,
                &config.kafka_consumer_group,
                topic,
            )?);
            let mut guard = match consumers.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Windows are sequential and topics are unique per window, so
            // two creators can never race for the same key.
            guard.insert(topic.to_owned(), Arc::clone(&consumer));
            Ok(consumer)
        }
    }
}

/// Upload-then-commit for one already-drained window.
async fn sync_window(
    store: &dyn ObjectStore,
    consumer: &KafkaConsumer,
    topic: &str,
    window_start: DateTime<Utc>,
    batch: ConsumedBatch,
) -> Result<(), BackfillError> {
    match (batch.message_count, batch.payload.is_empty()) {
        (0, _) => {
            info!(topic, "no messages in window; skipping upload and commit");
            Ok(())
        }
        (_, true) => {
            info!(
                topic,
                message_count = batch.message_count,
                "window held only empty/tombstone messages; committing offsets without an upload"
            );
            consumer.commit_offsets(&batch.partition_offsets).await?;
            Ok(())
        }
        (_, false) => {
            let key = object_key(topic, &window_start, &batch.partition_offsets);
            let bytes = batch.payload.len();
            let message_count = batch.message_count;
            store.put_object(&key, Bytes::from(batch.payload)).await?;
            info!(
                topic,
                key = %key,
                bytes,
                message_count,
                partitions = %partition_fragment(&batch.partition_offsets),
                "window uploaded to object storage"
            );
            consumer.commit_offsets(&batch.partition_offsets).await?;
            debug!(topic, "offsets committed");
            Ok(())
        }
    }
}

/// S3 object keys are limited to 1024 bytes; stay comfortably below that
/// before the partition list is replaced by its hash.
const MAX_KEY_BYTES: usize = 960;

/// Deterministic object key: `yyyy/mm/dd/<topic>/Par<p>-Off<o>_....json`
/// (partitions sorted ascending).
///
/// The consumed offsets uniquely identify a window's data on a topic, so a
/// retry after a failed offset commit **overwrites** the same object
/// instead of duplicating it. Two caveats keep this at-least-once storage
/// rather than exactly-once:
///   * the date prefix is *processing* time — a commit failure straddling
///     midnight lands the retry under the next day's prefix;
///   * the retry window may contain *more* messages (new arrivals), so it
///     writes a superset object under a different fragment and the earlier
///     object remains.
///
/// Downstream consumers deduplicate by offset range; nothing is ever lost.
/// Topics with many partitions would exceed S3's 1024-byte key limit, so an
/// oversized key falls back to the FNV-1a hash of the partition fragment.
fn object_key(
    topic: &str,
    window_start: &DateTime<Utc>,
    partition_offsets: &BTreeMap<i32, i64>,
) -> String {
    let prefix = format!(
        "{:04}/{:02}/{:02}/{}",
        window_start.year(),
        window_start.month(),
        window_start.day(),
        topic
    );
    let fragment = partition_fragment(partition_offsets);
    let suffix = match prefix.len() + fragment.len() + "/.json".len() > MAX_KEY_BYTES {
        true => format!("Partitions-{:016x}", fnv1a_64(fragment.as_bytes())),
        false => fragment,
    };
    format!("{prefix}/{suffix}.json")
}

/// FNV-1a (64-bit): std-only, deterministic, well-distributed enough to
/// disambiguate pathologically long partition lists within a day prefix.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
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
