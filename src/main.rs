//! kafka-sync — consumes Kafka topics in time windows and archives the
//! messages to object storage for cold storage.

mod backfill;
mod config;
mod error;
mod kafka;
mod storage;
mod telemetry;

use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Notify;
use tracing::info;

use crate::backfill::Backfill;
use crate::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env().context("invalid configuration")?;
    telemetry::init().context("failed to initialize telemetry")?;
    let store = storage::from_config(&config).await;
    let shutdown = install_shutdown_handler().context("failed to install the Ctrl-C handler")?;
    info!(step = "Server Start", value = "ok", "kafka-sync started");
    Backfill::new(Arc::new(config), store).run(shutdown).await;
    info!(
        step = "Shutdown",
        value = "ok",
        "kafka-sync stopped cleanly"
    );
    Ok(())
}

/// Wires SIGINT/SIGTERM to a [`Notify`] the main loop can `await`, giving
/// the process a graceful shutdown path.
fn install_shutdown_handler() -> Result<Arc<Notify>, ctrlc::Error> {
    let shutdown = Arc::new(Notify::new());
    let notifier = Arc::clone(&shutdown);
    ctrlc::set_handler(move || notifier.notify_one())?;
    Ok(shutdown)
}
