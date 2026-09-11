import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { StatCard } from "../components/StatCard";
import { Badge } from "../components/Badge";

export default function Overview() {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = () => api.overview().then(setData).catch((e) => setError(String(e)));
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Overview</h1>
      <p className="page-subtitle">
        What's happening, is the pipeline healthy, and what needs attention right now.
      </p>

      {error && <div className="card">Backend unreachable: {error}</div>}
      {!data && !error && <div className="card">Loading…</div>}

      {data && (
        <>
          <h2>Throughput</h2>
          <div className="grid grid-4" style={{ marginBottom: 20 }}>
            <StatCard label="Ingestion" value={fmtEps(data.throughput?.ingestion_eps)} />
            <StatCard label="Processing" value={fmtEps(data.throughput?.processing_eps)} />
            <StatCard label="Latency" value={fmtMs(data.throughput?.latency_ms)} />
            <StatCard label="Kafka Lag" value={data.throughput?.kafka_lag} />
          </div>

          <h2>Severity distribution</h2>
          <div className="grid grid-4" style={{ marginBottom: 20 }}>
            {["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"].map((s) => (
              <div className="card" key={s}>
                <div className="metric-label"><Badge value={s} /></div>
                <div className="metric-value">{data.severity_distribution?.[s] ?? 0}</div>
              </div>
            ))}
          </div>

          <div className="grid grid-2">
            <div className="card">
              <h2>Top source IPs</h2>
              <Table rows={data.top_source_ips} cols={["source_ip", "n"]} />
            </div>
            <div className="card">
              <h2>Top targeted hosts</h2>
              <Table rows={data.top_targeted_hosts} cols={["hostname", "n"]} />
            </div>
          </div>

          <div className="card" style={{ marginTop: 14 }}>
            <h2>Highest-severity alerts</h2>
            <table>
              <thead><tr><th>Title</th><th>Severity</th><th>Score</th><th>Host</th><th>Source</th></tr></thead>
              <tbody>
                {(data.top_severity_alerts || []).map((a: any) => (
                  <tr key={a.alert_id}>
                    <td>{a.title}</td>
                    <td><Badge value={a.severity} /></td>
                    <td>{a.risk_score}</td>
                    <td className="mono">{a.hostname || "—"}</td>
                    <td className="mono">{a.source_ip || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

function fmtEps(v: any) { return v === "unavailable" || v == null ? "unavailable" : `${Number(v).toFixed(0)}/sec`; }
function fmtMs(v: any) { return v === "unavailable" || v == null ? "unavailable" : `${Number(v).toFixed(0)} ms`; }

function Table({ rows, cols }: { rows: any[]; cols: string[] }) {
  if (!rows || rows.length === 0) return <div className="page-subtitle">No data yet.</div>;
  return (
    <table>
      <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>{cols.map((c) => <td key={c} className="mono">{r[c]}</td>)}</tr>
        ))}
      </tbody>
    </table>
  );
}
