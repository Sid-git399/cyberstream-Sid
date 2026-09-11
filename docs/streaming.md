# Streaming Pipeline

## Stages (Section 14)
1. **Read** — `spark.readStream.format("kafka")` subscribes to
   `security-events`.
2. **Parse JSON** — `parse_and_split()` applies the explicit
   `RAW_EVENT_SCHEMA`; rows that fail to parse go to `malformed` (-> DLQ),
   the rest continue.
3. **Validate + normalize** — `shared.normalize.normalize_event()`
   repairs recoverable issues (missing optional fields, defaultable
   values) and raises `ValidationError` for unrecoverable ones (which
   are also routed to the DLQ, with the original event + error reason,
   Section 47).
4. **Enrich** — `shared.normalize.enrich_event()` adds hour-of-day,
   day-of-week, internal/external classification, privileged-account
   flag, known/suspicious port, and asset criticality — all computed
   from local, synthetic data only (Section 16, no external IP intel).
5. **Detect** — `shared.detection.run_detectors()` runs every detector
   against the current micro-batch's events, grouped by host.
6. **Score** — `shared.scoring.score_detection()` computes a transparent
   additive risk score with an itemized breakdown.
7. **Aggregate/dedup** — `shared.correlation.dedupe_alerts()` collapses
   repeated identical detections into one alert with an occurrence count.
8. **Write** — normalized+enriched events -> Parquet
   (`partitionBy("event_type")`); deduped alerts -> a JSON sink the
   backend also loads into Postgres.

## Event time vs. processing time
Every event carries its own `timestamp` field (event time — when the
activity happened on the source system). Spark also has an implicit
notion of processing time (when the micro-batch executes). CyberStream
windows and watermarks on event time
(`withWatermark("event_time", "2 minutes")`) specifically so that a host
whose logs are shipped a little late still gets grouped into the correct
time window, rather than being smeared across whatever window happens to
be "current" when Spark finally sees it.

## Handling late events
The watermark tells Spark: results for a window are considered final
2 minutes after the window's own end, in event time. Events arriving
within that grace period are still incorporated; events later than that
are dropped from windowed aggregation (they still land in Parquet via
the non-windowed `process_batch` write path — only the *windowed
detection* respects the watermark's finality).

## Duplicate handling and data quality (Sections 61-62)
`process_batch` in `streaming/spark_job.py` runs, per micro-batch, in
this order: late-event accounting -> duplicate filtering
(`shared/dedup.py`) -> normalization/enrichment -> DLQ routing for
anything that still fails validation. Every one of those steps updates
the shared `shared.data_quality.GLOBAL_TRACKER` counters, which the
backend exposes at `GET /api/pipeline/data-quality`. See
`shared/dedup.py`'s module docstring for exactly what counts as a
duplicate (same `event_id`, not "looks similar") and why that's the
right definition for security telemetry specifically.
