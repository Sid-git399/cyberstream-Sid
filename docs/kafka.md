# Kafka

## Topics
`security-events` is the single consolidated topic for normalized-shape
input (Section 11 explicitly permits a consolidated topic when
documented). We chose one topic over six per-event-type topics because:

- Most detections (correlation, incidents) need to reason across event
  types for the same host — a single topic keeps that data co-located
  by partition (see the hostname-partitioning rationale in
  architecture.md) instead of requiring a multi-topic join in Spark.
- Six topics would mean six times the partition/consumer bookkeeping for
  a demo system with one producer and one consumer group, for no
  detection-quality benefit.

`security-events-dlq` holds events that fail JSON parsing or schema
validation (Section 47).

## Partitions
Default: 3 partitions (`KAFKA_DEFAULT_PARTITIONS` in `.env`), keyed by
`hostname` (rationale in architecture.md). More partitions allow more
parallel Spark tasks to consume concurrently — this is the literal
mechanism behind "Kafka partitioning enables horizontal scaling"
(Section 12): each partition can only be read by one consumer within a
group at a time, so partition count is an upper bound on consumption
parallelism.

## Consumer group
The Spark job connects with `kafka.group.id=cyberstream-spark`
(`KAFKA_CONSUMER_GROUP`). Using an explicit, named group (rather than
Spark's internal default) means:

- Kafka UI and `kafka-consumer-groups.sh` can show meaningful lag
  numbers against a name an operator recognizes.
- If the Spark job restarts, it resumes from committed offsets under
  that group rather than starting a fresh, anonymous group with no
  history (fault recovery, Section 13).

## Offsets & checkpointing
Structured Streaming manages Kafka offsets itself via its checkpoint
directory (`CHECKPOINT_DIR=/data/checkpoints`), not via Kafka consumer-
group commits alone. On restart, Spark resumes each partition from the
offset recorded in its checkpoint — this is what makes the pipeline
resume-safe if the `spark` container crashes and restarts (Section 46).

## Observing lag
`GET /api/kafka/status` (backend/app/metrics.py) computes lag as
`end_offset - committed_offset` per partition via `kafka-python`'s
`KafkaConsumer`/`KafkaAdminClient`. If Kafka is unreachable, the response
is `{"status": "OFFLINE", "reason": ...}` — never a fabricated lag
number (Section 33, 67).
