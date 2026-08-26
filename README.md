# Kafka Sync

Archives Kafka topics to a pluggable object store (local filesystem, AWS S3,
or Google Cloud Storage) for cold storage. Messages are written in fixed
time windows; offsets are committed only *after* each window is uploaded —
so the archive is **at-least-once**: crashes duplicate, never lose.

## How it works

Per window, per topic:

1. **Consume** — a long-lived consumer (one per topic) buffers messages for
   `sync_duration_secs` (capped at `max_window_bytes`).
2. **Upload** — the buffer goes to the configured storage backend from
   memory. Empty windows skip both upload and commit; tombstone-only
   windows commit without uploading.
3. **Commit** — offsets advance only after a successful upload.

Topics run concurrently; a failure only retries the next cycle. Ctrl-C closes
the group members and exits.

## Object layout

```
s3://<bucket>/<path>/yyyy/mm/dd/<topic>/Par<partition>-Off<offset>[_...].json   (AWS)
gs://<bucket>/yyyy/mm/dd/<topic>/Par<partition>-Off<offset>[_...].json          (GCS)
<root>/yyyy/mm/dd/<topic>/Par<partition>-Off<offset>[_...].json                 (FileSystem)
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
| `[storage] provider` | `OBJECT_STORAGE` | no | `FileSystem` | `FileSystem` \| `AWS` \| `GCS` |
| `[storage.filesystem] path` | `STORAGE_ROOT` | no | `./logs` | FileSystem root dir |
| `[storage.aws] bucket` | `CLOUD_BUCKET` | AWS | — | Target bucket |
| `[storage.aws] path` | `CLOUD_PATH` | AWS | — | S3 key prefix |
| `[storage.aws] region` | `AWS_REGION` | AWS | — | SDK region |
| `[storage.gcs] bucket` | `CLOUD_BUCKET` | GCS | — | Target bucket |
| `[storage.gcs] credentials_path` | `GCS_CREDENTIALS_PATH` | GCS | — | Service-account JSON |

Blank = unset; invalid fails fast. Secrets in env/`.env`, not in
`config/*.toml`. `RUST_LOG` is env-only.

### Storage backends

- **FileSystem** (default) — no auth, writes under `./logs` (git-ignored);
  ideal for local dev. Uploads are atomic (temp write + rename), so a crash
  never leaves a torn file, and object keys that would escape the root
  (`..`, absolute) are refused.
- **AWS S3** — credentials via the standard SDK chain (env → profile → SSO
  → IMDS/ECS); objects land under the required `path` prefix
  (`s3://<bucket>/<path>/…`).
- **GCS** — authenticates with a service-account JSON key file
  (`credentials_path`); the secret stays on disk, never in config or logs.

A bare `cargo run` with only `KAFKA_BROKERS` + `KAFKA_TOPICS` set archives
to `./logs` — selecting a cloud backend is always opt-in.

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

`main` wires → `config` (validated) → `telemetry` → `storage`
(provider-selected store behind `ObjectStore`) → `backfill` (consume →
upload → commit), plus `kafka`, `error`, `consts`. Lints deny `unsafe_code`,
`unwrap`, `expect`, `panic`.
