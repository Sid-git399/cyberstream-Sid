import { useState } from "react";
import { api } from "../lib/api";

export default function ThreatHunting() {
  const [query, setQuery] = useState({ source_ip: "", username: "", event_type: "" });
  const [results, setResults] = useState<any>(null);

  const run = () => {
    const q: Record<string, string> = {};
    Object.entries(query).forEach(([k, v]) => { if (v) q[k] = v; });
    api.hunt(q).then(setResults);
  };

  return (
    <div>
      <h1>Threat Hunting</h1>
      <p className="page-subtitle">Structured hunting over normalized events (Section 29).</p>
      <div className="filters">
        <input placeholder="source_ip" value={query.source_ip}
          onChange={(e) => setQuery({ ...query, source_ip: e.target.value })} />
        <input placeholder="username" value={query.username}
          onChange={(e) => setQuery({ ...query, username: e.target.value })} />
        <input placeholder="event_type" value={query.event_type}
          onChange={(e) => setQuery({ ...query, event_type: e.target.value })} />
        <button className="primary" onClick={run}>Hunt</button>
      </div>
      {results && (
        <div className="card">
          <div className="page-subtitle">{results.total} matches (max 500 shown)</div>
          {results.note && <div className="page-subtitle">{results.note}</div>}
          <table>
            <thead><tr><th>Time</th><th>Type</th><th>Host</th><th>User</th><th>Source</th></tr></thead>
            <tbody>
              {results.events?.map((e: any, i: number) => (
                <tr key={i}>
                  <td className="mono">{e.timestamp}</td>
                  <td>{e.event_type}</td>
                  <td className="mono">{e.hostname}</td>
                  <td className="mono">{e.username}</td>
                  <td className="mono">{e.source_ip}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
