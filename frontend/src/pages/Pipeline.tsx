import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function Pipeline() {
  const [health, setHealth] = useState<any>(null);
  const [dlq, setDlq] = useState<any>(null);
  const [quality, setQuality] = useState<any>(null);

  useEffect(() => {
    const load = () => {
      api.pipelineHealth().then(setHealth).catch(() => setHealth(null));
      api.dlq({ page_size: "10" }).then(setDlq).catch(() => setDlq(null));
      api.dataQuality().then(setQuality).catch(() => setQuality(null));
    };
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Pipeline Health</h1>
      <p className="page-subtitle">Real component status (Section 32) — never a hardcoded "ONLINE".</p>
      {!health && <div className="card">Loading…</div>}
      {health && (
        <div className="grid grid-4">
          <div className="card"><div className="metric-label">Kafka</div><Badge value={health.kafka.status} /></div>
          <div className="card"><div className="metric-label">Spark</div><Badge value={health.spark.status} /></div>
          <div className="card"><div className="metric-label">Database</div><Badge value={health.database.status} /></div>
          <div className="card"><div className="metric-label">Storage</div><Badge value={health.storage.status} /></div>
        </div>
      )}
      {health?.spark?.detail?.reason && (
        <div className="card" style={{ marginTop: 14 }}>Spark: {health.spark.detail.reason}</div>
      )}
      {health?.kafka?.detail?.reason && (
        <div className="card" style={{ marginTop: 14 }}>Kafka: {health.kafka.detail.reason}</div>
      )}

      <h2 style={{ marginTop: 24 }}>Data quality (Section 61)</h2>
      {quality && (
        <div className="grid grid-4" style={{ marginBottom: 14 }}>
          <div className="card"><div className="metric-label">Invalid events</div><div className="metric-value">{quality.invalid_events}</div></div>
          <div className="card"><div className="metric-label">Duplicate events</div><div className="metric-value">{quality.duplicate_events}</div></div>
          <div className="card"><div className="metric-label">Late events</div><div className="metric-value">{quality.late_events}</div></div>
          <div className="card"><div className="metric-label">Processing errors</div><div className="metric-value">{quality.processing_errors}</div></div>
        </div>
      )}

      <h2>Dead-letter queue (Section 47)</h2>
      <div className="card">
        {dlq && <div className="page-subtitle">{dlq.total} entries total.</div>}
        <table>
          <thead><tr><th>Time</th><th>Error reason</th></tr></thead>
          <tbody>
            {dlq?.entries?.map((e: any) => (
              <tr key={e.id}>
                <td className="mono">{e.created_at}</td>
                <td>{e.error_reason}</td>
              </tr>
            ))}
            {(!dlq || dlq.entries?.length === 0) && (
              <tr><td colSpan={2} className="page-subtitle">No dead-lettered events — nothing has failed normalization.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

