# Scalability & Backpressure

## What "designed for horizontal scaling" means here (Section 68)
CyberStream does not claim to handle billions of events on the default
Docker Compose stack (1 Kafka broker, 1 Spark executor). What it *does*
demonstrate, honestly:

- **Kafka partitions** (`KAFKA_DEFAULT_PARTITIONS`) are the real
  mechanism for parallel consumption — increasing partitions and
  running more Spark executors increases how many partitions can be
  consumed concurrently. This is architecturally real, not simulated;
  it's simply not exercised at large scale in the default local stack.
- **Spark's `maxOffsetsPerTrigger`** bounds per-batch work, which is
  also the direct lever for demonstrating backpressure (below).
- **The Load Lab** (`/api/benchmarks`, Section 36) lets you actually
  measure your own hardware's throughput rather than trusting a claimed
  number — see benchmarking.md.

## Backpressure, demonstrated for real (Section 35)
If the generator's configured rate (`GENERATOR_RATE`) exceeds what the
single-executor Spark job can process per `maxOffsetsPerTrigger` window,
Kafka's queue for that topic grows — this is real: `GET
/api/kafka/status`'s `total_lag` will climb because
`end_offset - committed_offset` genuinely increases. The Pipeline page
surfaces this as: *"Processing capacity is currently below ingestion
rate. Kafka backlog is increasing."* — derived from the same lag figure,
not a separate scripted message.

## Scaling this system further (not implemented, but architecturally supported)
- More Kafka partitions + more Spark executors (`spark-submit
  --num-executors`) increases consumption parallelism linearly, bounded
  by partition count.
- The `foreachBatch` Python detection logic (see docs/spark.md's
  trade-off note) is the current scalability ceiling — a production
  system would port `shared/detection.py`'s logic into Pandas UDFs or
  native Spark SQL to avoid the driver-side `.collect()`.
- Postgres would need read replicas or a time-series-oriented store
  (e.g. TimescaleDB) well before Parquet/DuckDB became the bottleneck,
  since alert volume is always far lower than raw event volume once
  dedup (Section 26) is applied.
