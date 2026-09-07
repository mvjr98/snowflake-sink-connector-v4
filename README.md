## Sink connector to write data to snowflake

This connector is used to write data to snowflake. It is a sink connector that reads data from Kafka topics and writes it to snowflake tables.

The package name is the version namespace, and all versions ship in the same jar:

| Version | Class | How data lands in Snowflake |
|---|---|---|
| v2 | `br.com.datastreambrasil.v2.SnowflakeSinkConnector` | PUT to stage → COPY straight into the final table |
| v3 | `br.com.datastreambrasil.v3.SnowflakeSinkConnector` | PUT to stage → COPY into `_INGEST` → MERGE/DELETE into the final table |
| v4 | `br.com.datastreambrasil.v4.SnowflakeSinkConnector` | Snowpipe Streaming straight into `_INGEST`, optional MERGE/DELETE |

---

## v4 — Snowpipe Streaming

v4 drops the PUT-to-stage and COPY steps entirely. Rows are streamed row by row into
`<table>_INGEST` through the [Snowpipe Streaming high-performance
architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview),
which is serverless and billed per uncompressed GB ingested instead of by warehouse time and
cloud services.

Two modes:

- **`ingestion_only: true`** — rows land in `_INGEST` and nothing else runs. No MERGE, no cleanup
  job, no warehouse at steady state. Deduplication is yours to do in Snowflake, typically with a
  Dynamic Table (there is a worked example in [`infra/scripts/snowflake_v4.sql`](infra/scripts/snowflake_v4.sql)).
- **`ingestion_only: false`** — the connector also applies what Snowflake already holds to the
  final table with MERGE/DELETE, like v3 did. `merge_interval` controls how often that happens:
  the default merges on every Connect commit (60s), and raising it to `PT15M` cuts the cycles by
  15x. The statement costs nearly the same for one row as for a million — 0.67s vs 0.75s measured —
  so what drives the bill is how many cycles run, not how much data moves.

### Prerequisites

- **A programmatic access token (PAT)**, supplied through `password` exactly as v2/v3 already do,
  so an existing connector definition carries over unchanged. JDBC takes the token as a plain
  password; the streaming SDK has no password concept, so the connector forwards the same value
  to it as `authorization_type=PAT`. Create the service user, network policy, role and token with
  [`infra/scripts/snowflake_v4.sql`](infra/scripts/snowflake_v4.sql). Tokens expire after 15 days
  by default and 365 days at most, so plan the rotation.
- **A glibc-based runtime image.** The SDK ships a Rust core loaded over JNI and supports
  x86\_64/ARM64 Linux with glibc ≥ 2.26, macOS ARM64 and Windows. **Alpine/musl images will not
  work.** The native libraries also add roughly 100 MB to the shaded jar.
- **Java 17+**, as before.

### Delivery semantics

Each topic-partition gets its own channel, named `<channel_name_prefix>_<topic>_<partition>`, and
the Kafka offset is used as the channel's offset token. On `open()` the connector reads the
channel's committed token and rewinds the consumer to it, so a restart resumes exactly where
Snowflake left off rather than where Kafka thinks it was. `preCommit` reports the committed token
rather than what was merely handed to `put()`, so Kafka offsets only advance once the data is
durable in Snowflake.

Unlike v3, v4 does **not** compact the batch by primary key in memory: every event is streamed.
Tables with many repeated updates to the same key will therefore ingest more bytes than v3 wrote,
which matters because billing is per ingested byte. Measure a real topic before migrating
everything.

### Configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `url` | string | — | JDBC URL; also the source of the account, database and endpoint |
| `user` | string | — | Snowflake user owning the PAT |
| `password` | password | — | The PAT, same key and same place as v2/v3 |
| `schema` | string | URL's `schema` | Target schema |
| `table` | string | — | Final table; rows are streamed into `<table>_INGEST` |
| `role` | string | user's default | Streaming session only, never sent over JDBC |
| `streaming_url` | string | `https://<host of url>:443` | Only when the streaming endpoint differs from the JDBC one |
| `ingestion_only` | boolean | `false` | Stream into `_INGEST` only: no MERGE, no cleanup job |
| `pipe` | string | `<table>_INGEST-STREAMING` | Override to use a custom pipe |
| `channel_name_prefix` | string | connector name | Must be stable across restarts |
| `max_client_lag_seconds` | int | SDK default | Higher buffers longer, writing fewer and larger files |
| `merge_interval` | duration | `PT0S` | Minimum time between MERGE cycles; merge mode only |
| `append_max_retries` | int | `5` | Retries with backoff on 408/429/500/503 |
| `fail_on_row_error` | boolean | `true` | Fail the task when the pipe rejects rows, as v3's COPY did |
| `find_columns_in_metadata` | boolean | `false` | Same meaning and default as v3 |
| `ignore_columns` | list | empty | Ingest-table columns never written to |
| `exclude_ingest_additional_fields` | list | the `IH_*` columns | Ingest columns absent from the final table |
| `timestamp_fields_convert` | list | empty | Columns whose epoch-millis value becomes a `LocalDateTime` |
| `date_fields_convert` | list | empty | Columns whose epoch-days value becomes a `LocalDate` |
| `time_fields_convert` | list | empty | Columns whose nanos-of-day value becomes a `LocalTime` |
| `job_cleanup_duration` | duration | `PT4H` | Cleanup job interval; merge mode only |
| `job_cleanup_retention_hours` | int | `4` | Hours of rows the cleanup job keeps |
| `job_cleanup_disable` | boolean | `false` | Always disabled when `ingestion_only` is true |

The v3 keys `stage`, `tmp_data_folder`, `buffer_initial_capacity` and `copy_only` do not exist in
v4, nor do the ones that were already dead code in v3 (`pk`, `always_truncate_before_bulk`,
`truncate_when_nodata_after_seconds`, `redis_*`).

Nothing was added either. The connection keeps v3's shape exactly: only `user` and `password` are
handed to the driver, and the account, database, schema and role come from the URL and the user's
defaults. The streaming SDK cannot work that way — it takes the account, endpoint, database and
schema as explicit arguments — so the connector derives them from that same JDBC URL:

| The SDK needs | v4 reads it from |
|---|---|
| account | the first host label, or an explicit `account=` parameter |
| endpoint | `https://<host>:443`, so PrivateLink and region-qualified hosts just work |
| database | the `db=` (or `database=`) parameter |
| schema | the `schema` config, falling back to the `schema=` parameter |

So a v3 URL works unchanged.

The temporal conversions keep v3's behaviour of using the JVM's default timezone, so a v3 → v4
migration does not shift existing values. Set `TZ` explicitly on the Connect pods if that matters
to you.

A ready-to-edit connector definition is in
[`infra/k8s/connectors/sink-snowflake-v4_sample.yaml`](infra/k8s/connectors/sink-snowflake-v4_sample.yaml).

### Deploying on Strimzi: raise the /tmp size limit

The SDK extracts its ~29 MiB Rust core into `java.io.tmpdir` and `dlopen()`s it from there.
Strimzi mounts `/tmp` as a **memory-backed `EmptyDir` that defaults to `5Mi`**, so the extraction
fails with `No space left on device` and the task dies with the unhelpful
`Failed to load both main and test libraries`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
spec:
  template:
    pod:
      tmpDirSizeLimit: 128Mi
```

It is pod memory rather than disk, so keep it just big enough. If you would rather not spend pod
memory, mount a disk-backed volume and point the JVM at it instead:

```yaml
spec:
  jvmOptions:
    javaSystemProperties:
      - name: java.io.tmpdir
        value: /tmp/native
  template:
    pod:
      volumes:
        - name: native-tmp
          emptyDir:
            sizeLimit: 256Mi
    connectContainer:
      volumeMounts:
        - name: native-tmp
          mountPath: /tmp/native
```

From v4.0.2 the connector detects this and says so, instead of surfacing the SDK's message. Note
that the JVM caches a failed static initializer, so the SDK's own loader error is only logged the
first time it is touched in a worker — a restarted task never sees it again.
