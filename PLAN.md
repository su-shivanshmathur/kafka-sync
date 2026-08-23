# kafka-sync — Modularization & Refactor Plan

## Goal

Refactor the single-file `backfill` binary into a clean, modular Rust
application. Modularize the AWS/S3 and logging code, replace deprecated
dependencies, centralize configuration, and fix correctness bugs found during
review.

**Scope:** Full refactor — modularize + migrate `rusoto` → `aws-sdk-s3` +
`tracing` + centralized config + error handling + bug fixes.
**AWS seam:** Provider-agnostic `ObjectStore` trait with an S3 implementation.

---

## Current state

`src/main.rs` (197 lines) does everything inline:

- Consumes Kafka messages, writes each to a temp file.
- Uploads the file to S3 via **rusoto 0.46** (deprecated/unmaintained).
- Deletes the temp file, looping forever in `while 1 == 1`.
- Logging is a hand-rolled `log_state()` that builds a `Log` struct and
  `println!`s JSON.
- Config is scattered `env::var(...)` calls with `unwrap()`.

## Defects found during review

| # | Severity | Issue |
|---|----------|-------|
| 1 | Critical | Double-read bug in `upload_to_s3` (lines 131–137): `file_content` is read twice into the same `Vec`, so every uploaded object has a **duplicated body**. |
| 2 | High | `rusoto` 0.46 is deprecated/unmaintained → migrate to `aws-sdk-s3`. |
| 3 | High | Panics on the hot path: `poll().unwrap()`, `commit_consumed().unwrap()`, `File::create`/`write_all` `panic!`, `SYNC_DURATION.parse().unwrap()`. One bad message or bad env var is fatal. |
| 4 | High | Synchronous `kafka` crate + `thread::sleep(1ns)` run directly on the tokio runtime — blocks the executor. |
| 5 | Medium | S3 client (creds + HTTP client) rebuilt on every upload instead of once. |
| 6 | Medium | Hardcoded `Region::EuCentral1` while everything else is env-driven. |
| 7 | Medium | Nondeterministic S3 object key (built from `HashMap` iteration order). |
| 8 | Low | Dead code: unused `Config` struct + top-level `#![allow(dead_code)]`. |
| 9 | Low | `ctrlc` is a dependency but there is no graceful shutdown. |
| 10 | Low | `log_state` `level` field is always empty; `step`/`value` misused as message. |
| 11 | Low | Typos / naming: `ENVIORNMENT`, `genericFile`, camelCase locals. |

---

## Target module layout

```
src/
  main.rs        # ~30 lines: init telemetry, load config, run backfill, await Ctrl-C
  config.rs      # Config::from_env() — typed, validated, fail-fast
  telemetry.rs   # tracing_subscriber JSON + EnvFilter (RUST_LOG)
  error.rs       # thiserror errors per module; anyhow at the app boundary
  kafka.rs       # KafkaConsumer wrapper, drains one time-window via spawn_blocking
  storage/
    mod.rs       # ObjectStore trait: async put_object(key, body)
    s3.rs        # S3Store — one shared aws-sdk-s3 Client behind Arc
  backfill.rs    # orchestrate: consume window -> buffer -> upload -> commit offsets
```

### The two abstractions

**Logging (`telemetry.rs`)** — replace `log_state` / `println!` with `tracing`:

- JSON `tracing_subscriber` + `EnvFilter` (honors `RUST_LOG`).
- Preserve the structured `{timestamp, level, step, value}` shape via structured
  fields: `info!(step = "...", value = "...")`.
- Real levels: `log_state("Error...", ...)` calls collapse to `warn!`/`error!`.
- Spans per topic / per upload for traceability.

**AWS (`storage/`)** — `ObjectStore` trait + `aws-sdk-s3`:

```rust
#[async_trait]
trait ObjectStore {
    async fn put_object(&self, key: &str, body: Bytes) -> Result<(), StorageError>;
}
```

- `S3Store` holds one `aws_sdk_s3::Client`, built **once** at startup from config
  (region, bucket) and shared via `Arc`.
- Matches the existing `OBJECT_STORAGE=AWS` env var — provider stays swappable.
- Empty-file check moves to the orchestrator; fixes the double-read bug.

---

## Dependency changes (`Cargo.toml`)

**Remove:** `rusoto_core`, `rusoto_s3`, `rusoto_credential`.

**Add:** `aws-config`, `aws-sdk-s3`, `tracing`, `tracing-subscriber`
(features: `json`, `env-filter`), `anyhow`, `thiserror`, `async-trait`.

**Keep:** `tokio`, `serde`, `serde_json`, `chrono` / `time`, `dotenv`, `kafka`,
`ctrlc`.

---

## Build order

1. **`telemetry.rs` + `config.rs`** — low-risk foundation. Replaces `log_state`
   and all scattered `env::var` with one validated `Config::from_env()`
   (fail-fast, no `unwrap` panics). Makes the dead `Config` struct real.
2. **`storage/`** — `ObjectStore` trait + `S3Store` on `aws-sdk-s3`; client built
   once. Swap out rusoto in `Cargo.toml`. Fixes the double-read bug.
3. **`kafka.rs`** — consumer wrapper; run the synchronous consumer under
   `spawn_blocking`, drop `thread::sleep(1ns)`.
4. **`backfill.rs` + slim `main.rs`** — orchestration; remove hot-path panics
   (log-and-skip bad messages), wire graceful shutdown via `ctrlc` /
   `tokio::signal`.
5. **Tests** — config parsing, deterministic S3-key formatting (sorted
   partitions), and a mock `ObjectStore` to test the orchestrator without AWS.

## Also fixed along the way

- Remove `#![allow(dead_code)]` and the dead `Config` struct (made real).
- Deterministic S3 key (sorted partitions instead of `HashMap` order).
- Real log levels.
- Typos / naming (`ENVIORNMENT` → `ENVIRONMENT`, `genericFile`, camelCase locals).

---

## Testing strategy

- Unit: `Config::from_env` parsing + validation; S3-key/object-name formatting.
- Orchestrator: mock `ObjectStore` impl, assert upload-on-window-close and
  offset-commit ordering.
- Manual: run against local Kafka (`docker-compose`) + a test bucket.
