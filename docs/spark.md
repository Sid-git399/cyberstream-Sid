# Spark

## Why `foreachBatch` instead of pure streaming DataFrame aggregations?
Spark's native `groupBy(window(...)).agg(...)` is excellent for simple
numeric aggregations, but CyberStream's detectors (brute force, password
spraying, port scanning, etc. — `shared/detection.py`) are Python
functions with branching logic that doesn't map cleanly onto SQL
aggregate expressions. `foreachBatch` hands us a plain Spark DataFrame
per micro-batch, which we `.collect()` and process with ordinary Python
— trading some scalability (a `.collect()` pulls the batch to the
driver) for the ability to reuse one detection/scoring codebase across
Spark, the local dev-mode runner, and the unit tests. For a "genuine
Big Data *laboratory*" whose job is to demonstrate concepts clearly, we
judged that trade-off worth it; a hardened production system would push
detection logic into Pandas UDFs or Scala to keep it fully distributed.

## Consumer configuration
See `streaming/spark_job.py::read_kafka_stream`. Key options:
- `startingOffsets=latest` — a restarted job doesn't try to reprocess
  months of history it wasn't running for.
- `maxOffsetsPerTrigger=200000` — caps how much a single micro-batch
  will read, which is the direct lever for the backpressure behaviour
  documented in docs/scalability.md.
- `failOnDataLoss=false` — tolerates the topic-recreation churn that's
  common in a local dev environment without crash-looping the job.

## Checkpointing
`CHECKPOINT_DIR=/data/checkpoints`, one directory per query (main
pipeline and DLQ sink use separate subdirectories). Checkpointing is
what makes Kafka offset tracking, dedup of already-processed
micro-batches, and stateful watermarking survive a restart.

## Monitoring
`GET /api/spark/status` calls Spark's own REST API
(`/api/v1/applications/.../streams`) rather than re-deriving metrics —
`recentProgress` already contains `inputRowsPerSecond`,
`processedRowsPerSecond`, and per-batch duration, which is exactly what
Section 34 asks the dashboard to show. If the endpoint isn't reachable,
the API returns `OFFLINE`, never a guessed number.
