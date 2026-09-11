import { useEffect, useState } from "react";
import { api } from "../lib/api";

export default function EventExplorer() {
  const [filters, setFilters] = useState({ event_type: "", hostname: "", username: "", source_ip: "" });
  const [data, setData] = useState<any>({ events: [], total: 0 });
  const [page, setPage] = useState(1);

  useEffect(() => {
    const params: Record<string, string> = { page: String(page), page_size: "50" };
    Object.entries(filters).forEach(([k, v]) => { if (v) params[k] = v; });
    api.events(params).then(setData).catch(() => setData({ events: [], total: 0 }));
  }, [filters, page]);

  return (
    <div>
      <h1>Event Explorer</h1>
      <p className="page-subtitle">
        Server-side paginated over Parquet (DuckDB) — never a full dataset load into the browser. {data.total} matching events.
      </p>

      <div className="filters">
        <input placeholder="event_type" value={filters.event_type}
          onChange={(e) => setFilters({ ...filters, event_type: e.target.value })} />
        <input placeholder="hostname" value={filters.hostname}
          onChange={(e) => setFilters({ ...filters, hostname: e.target.value })} />
        <input placeholder="username" value={filters.username}
          onChange={(e) => setFilters({ ...filters, username: e.target.value })} />
        <input placeholder="source_ip" value={filters.source_ip}
          onChange={(e) => setFilters({ ...filters, source_ip: e.target.value })} />
      </div>

      <div className="card">
        {data.note && <div className="page-subtitle">{data.note}</div>}
        <table>
          <thead>
            <tr><th>Time</th><th>Type</th><th>Action</th><th>Host</th><th>User</th><th>Source</th><th>Dest</th><th>Status</th></tr>
          </thead>
          <tbody>
            {data.events?.map((e: any, i: number) => (
              <tr key={i}>
                <td className="mono">{e.timestamp}</td>
                <td>{e.event_type}</td>
                <td>{e.action}</td>
                <td className="mono">{e.hostname}</td>
                <td className="mono">{e.username}</td>
                <td className="mono">{e.source_ip}</td>
                <td className="mono">{e.destination_ip}</td>
                <td>{e.status}</td>
              </tr>
            ))}
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
