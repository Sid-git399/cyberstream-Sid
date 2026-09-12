import { useEffect, useRef, useState } from "react";
import { api } from "../lib/api";

const MAX_ROWS = 200;

export default function LiveStream() {
  const [rows, setRows] = useState<any[]>([]);
  const [paused, setPaused] = useState(false);
  const [severityFilter, setSeverityFilter] = useState("");
  const seenIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    const poll = async () => {
      if (paused) return;
      try {
        const res = await api.events({ page: "1", page_size: "50" });
        const fresh = (res.events || []).filter((e: any) => {
          const id = e.event_id || `${e.timestamp}-${e.hostname}-${e.action}`;
          if (seenIds.current.has(id)) return false;
          seenIds.current.add(id);
          return true;
        });
        if (fresh.length) {
          setRows((prev) => [...fresh, ...prev].slice(0, MAX_ROWS));
        }
      } catch {
        /* backend not reachable yet — leave the last known rows displayed */
      }
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => clearInterval(id);
  }, [paused]);

  const filtered = severityFilter
    ? rows.filter((r) => (r.event_type || "").toLowerCase().includes(severityFilter.toLowerCase()))
    : rows;

  return (
    <div>
      <h1>Live Stream</h1>
      <p className="page-subtitle">
        A bounded window (max {MAX_ROWS} rows) of recently processed events — never an
        unbounded list (Section 41, 44).
      </p>

      <div className="filters">
        <button className="primary" onClick={() => setPaused((p) => !p)}>
          {paused ? "Resume" : "Pause"}
        </button>
        <input placeholder="filter by event type" value={severityFilter}
          onChange={(e) => setSeverityFilter(e.target.value)} />
        <button className="primary" onClick={() => { setRows([]); seenIds.current.clear(); }}>
          Clear
        </button>
        <span className="page-subtitle">{filtered.length} shown, updates every 3s while running</span>
      </div>

      <div className="card mono" style={{ maxHeight: 560, overflowY: "auto" }}>
        <table>
          <thead>
            <tr><th>Time</th><th>Type</th><th>Action</th><th>Host</th><th>User</th><th>Source</th></tr>
          </thead>
          <tbody>
            {filtered.map((e, i) => (
              <tr key={i}>
                <td>{e.timestamp}</td>
                <td>{e.event_type}</td>
                <td>{e.action}</td>
                <td>{e.hostname}</td>
                <td>{e.username}</td>
                <td>{e.source_ip}</td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr><td colSpan={6} className="page-subtitle">
                Waiting for processed events — start the generator + streaming job.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
