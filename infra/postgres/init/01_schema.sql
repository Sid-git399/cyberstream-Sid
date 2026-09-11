-- CyberStream application metadata store (Section 5, 38).
-- Raw/processed EVENTS live in Parquet (data/processed) — Postgres only
-- holds operational metadata: alerts, incidents, benchmark runs, and
-- pipeline metric snapshots. This keeps Postgres small and query-fast.

CREATE TABLE IF NOT EXISTS alerts (
    alert_id            TEXT PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    title               TEXT NOT NULL,
    detection_rule      TEXT NOT NULL,
    category            TEXT,
    severity            TEXT NOT NULL,
    risk_score          INTEGER NOT NULL,
    score_breakdown     JSONB,
    source_ip           TEXT,
    destination_ip      TEXT,
    hostname            TEXT,
    username            TEXT,
    evidence            JSONB,
    mitre_technique     JSONB,
    occurrences         INTEGER DEFAULT 1,
    total_underlying_events INTEGER DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'NEW'
                         CHECK (status IN ('NEW','ACKNOWLEDGED','INVESTIGATING','RESOLVED','FALSE_POSITIVE'))
);

CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_hostname ON alerts (hostname);
CREATE INDEX IF NOT EXISTS idx_alerts_source_ip ON alerts (source_ip);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id     TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    entity          TEXT,
    stage_count     INTEGER,
    stages          JSONB,
    max_severity    TEXT
);

CREATE TABLE IF NOT EXISTS dlq_entries (
    id              SERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_reason    TEXT,
    original_event  JSONB
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                  SERIAL PRIMARY KEY,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingestion_eps       DOUBLE PRECISION,
    processing_eps      DOUBLE PRECISION,
    detection_eps       DOUBLE PRECISION,
    latency_ms          DOUBLE PRECISION,
    kafka_lag           BIGINT,
    spark_batch_duration_ms DOUBLE PRECISION,
    source              TEXT DEFAULT 'live'  -- 'live' vs 'benchmark' (Section 67)
);

CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    target_events       BIGINT,
    target_eps          INTEGER,
    partitions          INTEGER,
    duration_seconds     INTEGER,
    events_generated    BIGINT,
    events_processed    BIGINT,
    avg_latency_ms      DOUBLE PRECISION,
    peak_latency_ms     DOUBLE PRECISION,
    kafka_lag_max       BIGINT,
    detection_count     INTEGER,
    status              TEXT DEFAULT 'running'
);
