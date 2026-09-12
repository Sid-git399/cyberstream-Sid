export default function Architecture() {
  return (
    <div>
      <h1>Architecture</h1>
      <p className="page-subtitle">Matches the actual running system (Section 55) — see docs/architecture.md for the rationale behind each choice.</p>
      <div className="card mono" style={{ whiteSpace: "pre", lineHeight: 1.5, overflowX: "auto" }}>
{`EVENT GENERATOR (Python)
        |
        v
   +-----------+
   |   KAFKA   |   topic: security-events (partitioned by hostname)
   | P0 P1 P2  |
   +-----+-----+
        |
        v
 SPARK STRUCTURED STREAMING
        |
  +-----+------+------+
  v            v      v
Normalize   Enrich   Detect (windowed, watermarked)
  +-----+------+------+
        v
   Risk Scoring + Alert Dedup
        |
  +-----+------+
  v            v
Parquet      Postgres
(events)     (alerts, incidents, benchmarks)
  +-----+------+
        v
   FASTAPI (DuckDB over Parquet + psycopg over Postgres)
        |
        v
   REACT DASHBOARD  (this app)`}
      </div>
    </div>
  );
}
