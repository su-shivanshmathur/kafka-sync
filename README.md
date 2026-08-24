# Kafka Sync

Archives Kafka topics to AWS S3 for cold storage. Messages are written in
fixed time windows; offsets are committed only *after* each window is
uploaded — so the archive is **at-least-once**: crashes duplicate, never lose.

## How it works

Per window, per topic:

1. **Consume** — a long-lived consumer (one per topic) buffers messages for
   `sync_duration_secs` (capped at `max_window_bytes`).
2. **Upload** — the buffer goes to S3 from memory. Empty windows skip both
   upload and commit; tombstone-only windows commit without uploading.
3. **Commit** — offsets advance only after a successful upload.

Topics run concurrently; a failure only retries the next cycle. Ctrl-C closes
the group members and exits.

## Object layout

```
s3://<bucket>/yyyy/mm/dd/<topic>/Par<partition>-Off<offset>[_...].json
```

Partitions sorted ascending (deterministic). Over-long keys fall back to an
FNV-1a hash of the partition fragment.

## Configuration

All settings live in **`config/<RUN_ENV>.toml`** (`RUN_ENV` defaults to
`development`, which also loads `.env`). Env vars override file values.
Reference: `config/config.example.toml`.

| Key | Env var | Required | Default | Meaning |
|---|---|---|---|---|
| `kafka.brokers` | `KAFKA_BROKERS` | **yes** | — | `host:port` list |
| `kafka.topics` | `KAFKA_TOPICS` | **yes** | — | Comma-separated |
| `kafka.consumer_group` | `KAFKA_CONSUMER_GROUP` | no | `kafka-sync` | Offset identity |
| `kafka.sync_duration_secs` | `SYNC_DURATION` | no | `10` | Window (s) |
| `kafka.max_window_bytes` | `KAFKA_MAX_WINDOW_BYTES` | no | 256 MiB | Buffer cap |
| `storage.provider` | `OBJECT_STORAGE` | no | `AWS` | Only AWS today |
| `storage.bucket` | `CLOUD_BUCKET` | **yes** | — | Target bucket |
| `storage.region` | `AWS_REGION` | **yes** | — | SDK region |

Blank = unset; invalid fails fast. Secrets in env/`.env`, not in
`config/*.toml`. AWS creds via the standard SDK chain. `RUST_LOG` is env-only.

## Running

```bash
cargo run --release                             # uses config/development.toml
docker build -t kafka-sync . && docker run --rm \
  -e KAFKA_TOPICS=a,b -e KAFKA_BROKERS=b:9092 kafka-sync
```

## Observability

JSON logs via `tracing` (`RUST_LOG` filters, e.g.
`RUST_LOG=backfill=debug`). Events carry fields like `topic`, `key`, `bytes`,
plus per-topic `"span": {...}`.

## Layout

`main` wires → `config` (validated) → `telemetry` → `storage` (cached S3
client behind `ObjectStore`) → `backfill` (consume → upload → commit), plus
`kafka`, `error`, `consts`. Lints deny `unsafe_code`, `unwrap`, `expect`,
`panic`.
