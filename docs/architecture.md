# Architecture

## Why Kafka?
Security telemetry arrives continuously and from many independent
sources (auth systems, firewalls, endpoints, cloud APIs). Kafka
decouples "things that produce events" from "things that consume them":
the generator (or, in production, real log shippers) never needs to know
who's downstream, how fast they can process, or whether they're even
running. That decoupling is also what makes backpressure survivable
(see docs/scalability.md) — Kafka's disk-backed log absorbs bursts that
would otherwise overwhelm a synchronous pipeline.

## Why partition by hostname?
Section 12 asks for a documented, meaningful partition key. CyberStream
partitions `security-events` by `hostname` rather than `source_ip` or
`user`:

- **Detection locality**: nearly every detector in `shared/detection.py`
  (brute force, port scan, privilege escalation, data-transfer anomaly)
  needs to see *all* events for a given host together to reason about
  windows correctly. Partitioning by host means a single Spark task
  handling that partition already has everything it needs, instead of
  requiring a cross-partition shuffle for every detection.
- **Cardinality**: hostnames are a moderate-cardinality key (hundreds to
  low thousands of hosts), which spreads load across partitions without
  the extreme skew you'd get from partitioning by a handful of
  high-volume source IPs (e.g. an external scanner would flood a single
  partition if keyed by source_ip).
- **Trade-off, stated honestly**: password-spraying and cross-host
  correlation *do* span multiple hosts, so those detectors necessarily
  look across partitions within a micro-batch. We accept that cost
  because it only affects two detectors, not all of them.

## Why Spark Structured Streaming (not raw DStreams)?
The DataFrame-based Structured Streaming API gives us, without hand-
rolled state management: `window()` + `withWatermark()` for late-event
handling (docs/streaming.md), checkpointing for exactly-once-ish sink
semantics, and a `foreachBatch` escape hatch that lets us run arbitrary
Python business logic (our detection/scoring modules) per micro-batch
instead of being boxed into DataFrame-only aggregation expressions.

## Why Parquet + Postgres, and not one or the other?
- **Parquet** for `data/processed` and `data/dlq`: columnar, splittable,
  and queryable directly by DuckDB without a database server — ideal for
  "large analytical event storage" (Section 38) that the Event Explorer
  and Threat Hunting pages page through without ever loading a full
  dataset into memory.
- **Postgres** for alerts, incidents, and benchmark runs: this is
  operational metadata that's small in volume but needs transactional
  updates (alert status transitions, Section 24) and relational queries
  (top hosts, severity counts) that are awkward over a Parquet lake.

## Why windowing? Why watermarking?
Nearly every detection is a *rate* phenomenon — "many failed logins in a
short time", not "one failed login". Windowing groups events by time
bucket so a detector can count within a bounded interval instead of the
entire history. Watermarking then bounds how long Spark waits for
late-arriving events before finalizing a window's results — without it,
a single very-late event would force Spark to keep every window's state
in memory forever. `withWatermark("event_time", "2 minutes")` says:
"assume no event that's more than 2 minutes late will show up; if it
does, drop it, don't hold state open for it indefinitely."

## Why aggregation and alert dedup?
Section 26's core observation: **raw event volume != useful alert
volume**. A single brute-force burst is one incident to a SOC analyst,
not the 40 individual alerts each failed login would otherwise generate.
`shared/correlation.py::dedupe_alerts` groups by (rule, source, host)
within a window; `correlate_incidents` goes one step further and chains
related alerts (brute force -> priv-esc -> exfil) into a single incident
— this now runs for real inside `streaming/spark_job.py::process_batch`
per micro-batch, not just as unused library code.

## Why a file-polling ingestor instead of Spark writing to Postgres directly?
Spark writes alerts/incidents as JSON (one append per micro-batch)
rather than opening a JDBC connection to Postgres from inside executors.
`backend/app/ingest.py` runs as a FastAPI background task, tailing those
JSON files and upserting rows keyed by `alert_id`/`incident_id` — safe
to re-read the same file twice. This keeps Spark's write path simple
(no JDBC driver bundling, no connection-pool management from JVM-side
Python workers) while still giving Postgres a live, queryable view.

## Why bucket detection windows in Python rather than native Spark `window()`?
Section 18 asks for four distinct window sizes (1 min / 5 min / 15 min /
1 hour), each tied to a specific detection category. `shared/windowing.py`
groups each micro-batch's events into per-rule time buckets using each
event's own event-time timestamp, then runs that rule's detector against
only its own bucket — a 3-minute-wide burst of failed logins will NOT
trigger the 1-minute brute-force rule, exactly as the spec's window
semantics require. The alternative (native Spark SQL `window()`
aggregates) doesn't compose with detectors that have per-event branching
logic like ours — see docs/spark.md's `foreachBatch` trade-off note for
the same discussion applied to Spark generally.

## A subtle bug this design fixes: burst/window alignment
Early testing surfaced a real failure mode worth documenting: the
`multi_stage` scenario's brute-force sub-burst (12 failed logins, 5s
apart, spanning 55s) is close enough to the 60-second brute-force window
that, depending purely on the wall-clock second the generator happened
to start at, the burst could straddle a window boundary and split into
two under-threshold groups — silently never triggering brute_force at
all. `generator/generator.py::_snap_to_window_start` fixes this by
anchoring every attack burst to the *start* of its detector's window
(not the nearest boundary — the next one), guaranteeing the full burst
always lands in a single window regardless of when the generator runs.
Verified by sweeping every possible second-of-minute start time and
confirming brute_force fires at all of them (see the generator's own
`WINDOW_SECONDS` import from `shared/windowing.py`, so this stays in
sync with the actual detection windows rather than a hardcoded 60).

## Why bound duplicate-detection to a recent horizon instead of all history?
`shared/dedup.py::DuplicateFilter` tracks up to 500,000 recent event_ids
and evicts the oldest once full — deliberately bounded, the same way
Spark's own `dropDuplicates` combined with a watermark only dedups
within a recent window. An event_id reappearing after it's aged out of
the tracked set is treated as new; this trades perfect historical dedup
for bounded memory use, which is the right trade-off for a
continuously-running stream.

## Why event-time processing?
Processing time (when Spark sees an event) and event time (when the
event actually happened, per its own timestamp) diverge whenever a host
is slow to ship logs, a network hiccups, or a batch of events is
replayed. Using event time for windowing means a 1-minute detection
window reflects what actually happened in that minute on the source
system, not an artifact of ingestion jitter.

## Why doesn't the dashboard query Kafka directly?
Kafka holds raw, unvalidated, unenriched, undetected events — showing
those directly would mean re-implementing normalization/enrichment/
detection in the frontend, and it would mean the "live" view could never
reflect risk scores or MITRE mappings that only exist after Spark has
processed the event. The dashboard only ever talks to the FastAPI layer,
which reads Spark's *output* (Parquet + Postgres) — see Section 6.
