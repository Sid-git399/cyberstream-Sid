import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function AlertDetail() {
  const { id } = useParams();
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    api.alert(id).then(setData).catch((e) => setError(String(e)));
  }, [id]);

  if (error) return <div className="card">Could not load alert: {error}</div>;
  if (!data) return <div className="card">Loading…</div>;

  const a = data.alert;

  return (
    <div>
      <Link to="/alerts" className="page-subtitle">&larr; Back to Alerts</Link>
      <h1 style={{ marginTop: 8 }}>{a.title}</h1>
      <p className="page-subtitle">Alert ID: <span className="mono">{a.alert_id}</span></p>

      <div className="grid grid-4" style={{ marginBottom: 16 }}>
        <div className="card"><div className="metric-label">Severity</div><Badge value={a.severity} /></div>
        <div className="card"><div className="metric-label">Risk Score</div><div className="metric-value">{a.risk_score}</div></div>
        <div className="card"><div className="metric-label">Occurrences</div><div className="metric-value">{a.occurrences}</div></div>
        <div className="card"><div className="metric-label">Status</div><div className="metric-value">{a.status}</div></div>
      </div>

      <div className="grid grid-2">
        <div className="card">
          <h2>Detection details</h2>
          <table>
            <tbody>
              <tr><td>Detection rule</td><td className="mono">{a.detection_rule}</td></tr>
              <tr><td>Host</td><td className="mono">{a.hostname || "—"}</td></tr>
              <tr><td>Source IP</td><td className="mono">{a.source_ip || "—"}</td></tr>
              <tr><td>Destination IP</td><td className="mono">{a.destination_ip || "—"}</td></tr>
              <tr><td>Username</td><td className="mono">{a.username || "—"}</td></tr>
              <tr><td>Underlying events</td><td>{a.total_underlying_events}</td></tr>
            </tbody>
          </table>
        </div>

        <div className="card">
          <h2>MITRE ATT&CK</h2>
          {a.mitre_technique ? (
            <table>
              <tbody>
                <tr><td>Technique</td><td className="mono">{a.mitre_technique.id}</td></tr>
                <tr><td>Name</td><td>{a.mitre_technique.name}</td></tr>
                <tr><td>Tactic</td><td>{a.mitre_technique.tactic}</td></tr>
              </tbody>
            </table>
          ) : <div className="page-subtitle">No technique mapped.</div>}
        </div>
      </div>

      <div className="card" style={{ marginTop: 14 }}>
        <h2>Score breakdown (Section 21 — every alert explains its score)</h2>
        <table>
          <thead><tr><th>Reason</th><th>Points</th></tr></thead>
          <tbody>
            {(a.score_breakdown || []).map((b: any, i: number) => (
              <tr key={i}><td>{b.reason}</td><td>+{b.points}</td></tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="card" style={{ marginTop: 14 }}>
        <h2>Evidence</h2>
        <pre className="mono" style={{ whiteSpace: "pre-wrap", margin: 0 }}>
          {JSON.stringify(a.evidence, null, 2)}
        </pre>
      </div>

      <div className="card" style={{ marginTop: 14 }}>
        <h2>Recommended investigation steps</h2>
        <ul>
          {data.recommended_steps?.map((s: string, i: number) => <li key={i}>{s}</li>)}
        </ul>
      </div>

      {data.related_alerts?.length > 0 && (
        <div className="card" style={{ marginTop: 14 }}>
          <h2>Related alerts (same host/source)</h2>
          <table>
            <thead><tr><th>Title</th><th>Severity</th><th>Created</th></tr></thead>
            <tbody>
              {data.related_alerts.map((r: any) => (
                <tr key={r.alert_id}>
                  <td><Link to={`/alerts/${r.alert_id}`}>{r.title}</Link></td>
                  <td><Badge value={r.severity} /></td>
                  <td className="mono">{r.created_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
