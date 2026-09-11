# CyberStream

A distributed Big Data cybersecurity analytics platform. Kafka ingests
synthetic security telemetry, Spark Structured Streaming normalizes,
enriches, detects and scores it, and a FastAPI + React dashboard exposes
the resulting analytics — not raw event streams — to a SOC-style UI.

This is a genuine pipeline, not a mockup: Kafka actually receives events,
Spark actually consumes and processes them, and every metric the
dashboard shows is read live from Kafka/Spark/Postgres/Parquet, or
explicitly marked "unavailable" when it can't be measured (see
`docs/security.md` and Section 67 of the original spec: no fabricated
metrics, ever).

## Quickstart (full stack, Docker)

```bash
cp .env.example .env
docker compose up --build
```

This starts, in order: Zookeeper, Kafka, Kafka UI, Postgres, the Spark
streaming job, the synthetic event generator, the FastAPI backend, and
the React dashboard.

| Service      | URL                          |
|--------------|------------------------------|
| Dashboard    | http://localhost:5173        |
| Backend API  | http://localhost:8000/docs   |
| Kafka UI     | http://localhost:8089        |
| Spark UI     | http://localhost:4040        |

Default `.env` runs **Dev Mode**: `small` preset (100K events), 200
events/sec, `normal` traffic. Edit `.env` for **Lab Mode** (higher rate,
more hosts, attack scenarios) — see comments in `.env.example`.

## Quickstart (no Docker — logic-only demo)

You can validate the entire normalize → enrich → detect → score → dedupe
→ correlate chain without Kafka or Spark at all:

```bash
pip install -r generator/requirements.txt
python3 scripts/run_local_pipeline.py --events 30000 --scenario multi_stage
# add --inject-duplicates 500 to see Section 62's dedup logic catch real replayed events
```

This is the same code the Spark job calls from `foreachBatch` — it's a
correctness demo and a low-resource dev-mode fallback (Section 58), not
a separate implementation.

## Running the tests

```bash
pip install -r requirements-dev.txt
python3 -m pytest tests/ -v
```

51 tests cover schema validation, normalization/enrichment, every
detection rule (including per-rule time windowing), risk scoring,
statistical anomaly detection, alert deduplication, incident
correlation, duplicate-event handling, data-quality tracking, the
generator, and the settings/detection-rules API endpoints — including
tests that verify the API genuinely reports Kafka/Spark as `OFFLINE`
rather than fabricating a number when those services are unreachable
(Section 67). `requirements-dev.txt` pulls in `backend/requirements.txt`
too, since the API-layer tests import backend modules directly.

## Project layout

```
shared/        normalization, enrichment, detection, windowing, dedup,
               data-quality tracking, scoring, correlation, MITRE mapping
               — the single source of business logic used by both the
               Spark job and the local fallback runner.
generator/     synthetic security-event generator + Kafka producer.
streaming/     PySpark Structured Streaming job (Kafka -> Parquet/Postgres).
backend/       FastAPI service — serves processed analytics, never raw Kafka;
               includes the background ingestor that tails Spark's JSON
               alert/incident/DLQ output into Postgres.
frontend/      React + TypeScript SOC dashboard (16 pages, see below).
infra/         Postgres schema, Kafka UI config.
scripts/       local (no-Kafka/Spark) pipeline runner + retention cleanup job.
tests/         pytest suite — 51 tests across shared/, generator/, and the API layer.
docs/          architecture rationale (Sections 65-66).
```

## Dashboard pages

Overview, Live Stream, Alerts (+ detail/investigation view), Incidents,
Event Explorer, Threat Hunting, Detection Rules, MITRE ATT&CK, Pipeline
(health + data quality + dead-letter queue), Kafka, Spark, Big Data Lab,
Load Lab, Benchmarks, Settings (+ report export), Architecture.

## Data flow

```
Event Generator -> Kafka -> Spark Structured Streaming
  -> late-event accounting -> duplicate filtering -> normalize -> enrich
  -> per-rule windowed detection -> risk scoring -> alert dedup
  -> incident correlation
  -> Parquet (events) + JSON (alerts/incidents/DLQ)
  -> backend ingestor -> Postgres
  -> FastAPI -> React Dashboard
```

The dashboard never queries Kafka directly — see `docs/architecture.md`
for why.

## What's real vs. what you configure

- **Real, measured, never hardcoded**: Kafka topic/partition/consumer-group
  status, Spark batch/throughput metrics, alert counts, event counts,
  Load Lab / Benchmark results.
- **Configurable, clearly synthetic**: the event generator's traffic
  (Section 9) — always synthetic telemetry, never a real attack
  (Section 49). See `.env.example`.

## Honesty about scale claims

This repo is designed for horizontal scaling (more Kafka partitions, more
Spark executors), but the numbers you'll see locally reflect a
single-broker, single-executor Docker Compose stack on one machine. Use
the Load Lab (Section 36) to generate your own benchmark numbers for your
hardware — don't assume the defaults represent a claim about
production-scale throughput (Section 68).
