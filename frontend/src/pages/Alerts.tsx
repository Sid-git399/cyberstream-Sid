import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function Alerts() {
  const [filters, setFilters] = useState({ severity: "", status: "", hostname: "" });
  const [data, setData] = useState<any>({ alerts: [], total: 0 });
  const [page, setPage] = useState(1);

  useEffect(() => {
    const params: Record<string, string> = { page: String(page), page_size: "25" };
    Object.entries(filters).forEach(([k, v]) => { if (v) params[k] = v; });
    api.alerts(params).then(setData).catch(() => setData({ alerts: [], total: 0 }));
  }, [filters, page]);

  return (
    <div>
      <h1>Alerts</h1>
      <p className="page-subtitle">
        Deduplicated detections (Section 26-27) — not raw event counts. {data.total} total.
      </p>

      <div className="filters">
        <select value={filters.severity} onChange={(e) => setFilters({ ...filters, severity: e.target.value })}>
          <option value="">All severities</option>
          {["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"].map((s) => <option key={s}>{s}</option>)}
        </select>
        <select value={filters.status} onChange={(e) => setFilters({ ...filters, status: e.target.value })}>
          <option value="">All statuses</option>
          {["NEW", "ACKNOWLEDGED", "INVESTIGATING", "RESOLVED", "FALSE_POSITIVE"].map((s) => <option key={s}>{s}</option>)}
        </select>
        <input placeholder="hostname" value={filters.hostname}
          onChange={(e) => setFilters({ ...filters, hostname: e.target.value })} />
      </div>

      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Title</th><th>Rule</th><th>Severity</th><th>Score</th>
              <th>Occurrences</th><th>Host</th><th>Source</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {data.alerts?.map((a: any) => (
              <tr key={a.alert_id}>
                <td><Link to={`/alerts/${a.alert_id}`}>{a.title}</Link></td>
                <td className="mono">{a.detection_rule}</td>
                <td><Badge value={a.severity} /></td>
                <td>{a.risk_score}</td>
                <td>{a.occurrences}</td>
                <td className="mono">{a.hostname || "—"}</td>
                <td className="mono">{a.source_ip || "—"}</td>
                <td>{a.status}</td>
              </tr>
            ))}
            {(!data.alerts || data.alerts.length === 0) && (
              <tr><td colSpan={8} className="page-subtitle">No alerts yet — generate traffic and let the pipeline run.</td></tr>
            )}
          </tbody>
        </table>
        <div style={{ marginTop: 10, display: "flex", gap: 8 }}>
          <button className="primary" disabled={page <= 1} onClick={() => setPage(page - 1)}>Prev</button>
          <button className="primary" onClick={() => setPage(page + 1)}>Next</button>
        </div>
      </div>
    </div>
  );
}
