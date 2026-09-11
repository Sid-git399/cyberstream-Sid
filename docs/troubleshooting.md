# Troubleshooting

**Kafka container is healthy but the backend reports Kafka OFFLINE.**
Check `KAFKA_BROKERS` — the backend/streaming containers must use the
internal listener (`kafka:29092`), while anything running on your host
machine (e.g. `python scripts/run_local_pipeline.py`, which doesn't
touch Kafka at all) is irrelevant here. If you're running the generator
from your host against the Compose stack, use `localhost:9092`.

**Spark status shows OFFLINE / "Spark UI unreachable".**
The Spark UI (port 4040) is only bound while a streaming query is
actively running. Give the `spark` container ~30-60s after startup for
`spark-submit` to resolve its `--packages` dependencies on first run.

**Event Explorer says "no processed events yet".**
This means no Parquet files exist under `data/processed/` yet — either
the generator hasn't produced enough events, or the Spark job hasn't
completed a micro-batch. Check `docker compose logs spark generator`.

**Alerts stay empty even with traffic flowing.**
Alerts require *scenario* traffic, not just `normal` — set
`GENERATOR_SCENARIO` to `brute_force`, `multi_stage`, etc. in `.env`
(Section 52's demo scenarios exist precisely for this).

**I don't have a machine that can run Kafka+Spark comfortably.**
Use `scripts/run_local_pipeline.py` — see the README's "no Docker"
quickstart. It exercises the identical `shared/` business logic without
any JVM processes.
