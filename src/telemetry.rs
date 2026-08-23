//! Structured JSON telemetry built on [`tracing`].
//!
//! Replaces the hand-rolled `log_state` helper: events carry real levels and
//! arbitrary structured fields, and the subscriber honors the standard
//! `RUST_LOG` directive set (falling back to `info`).

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::error::TelemetryError;

/// Installs the process-wide JSON logger.
///
/// The event fields (`step`, `value`, `topic`, `key`, ...) are flattened into
/// the top-level JSON object, preserving the `{timestamp, level, ...}` shape
/// of the previous logger while adding real levels and span context.
pub fn init() -> Result<(), TelemetryError> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(from_env) => from_env,
        Err(_) => EnvFilter::try_new("info")?,
    };
    let json = fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false);
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(filter).with(json),
    )?;
    Ok(())
}
