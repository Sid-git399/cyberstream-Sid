# Benchmarking (Load Lab)

`POST /api/benchmarks` (backend/app/routers/loadlab.py) runs a real,
bounded load test:

1. Records the current processed-event count (from Parquet via DuckDB)
   and current Kafka lag as the "before" baseline.
2. Uses `generator.generator.generate()` to produce
   `target_eps * duration_seconds` events and publishes them to Kafka
   with a real `KafkaProducer`, timing each `send()` call.
3. After the run, re-measures the processed-event count and Kafka lag,
   and computes average/peak producer-side latency from the timed sends.
4. Persists `events_generated`, `events_processed` (the *delta*, not a
   guess), `avg_latency_ms`, `peak_latency_ms`, and `kafka_lag_max` to
   `benchmark_runs` in Postgres.

Because this takes real wall-clock time, it runs as a FastAPI
`BackgroundTask`; the frontend polls `GET /api/benchmarks/{run_id}`
until `status == "completed"`.

## Why this design, and not a pre-canned table of numbers
Section 37 requires benchmark values to come from actual runs, and
Section 68 explicitly forbids unverified scale claims. A benchmark
feature that just displays static numbers would violate both — so the
Load Lab always measures your own stack, on your own hardware, for the
scenario and rate you configure. Comparing runs at different
`target_eps` settings (e.g. 1K/10K/50K events/sec) reproduces the
low/medium/high table structure from Section 37, but with real figures
per environment rather than one fixed table baked into the docs.
