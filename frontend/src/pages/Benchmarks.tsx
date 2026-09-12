import { useEffect, useState } from "react";
import { api } from "../lib/api";

export default function Benchmarks() {
  const [runs, setRuns] = useState<any[]>([]);

  useEffect(() => {
    const load = () => api.listBenchmarks().then(setRuns).catch(() => setRuns([]));
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Benchmarks</h1>
      <p className="page-subtitle">
        History of Load Lab runs — every row is a real measured run, never a
        pre-canned table (Section 37, 68).
      </p>

      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Run</th><th>Target EPS</th><th>Duration</th><th>Generated</th>
              <th>Processed</th><th>Avg latency</th><th>Peak latency</th><th>Kafka lag</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.run_id}>
                <td className="mono">{r.run_id}</td>
                <td>{r.target_eps}/sec</td>
                <td>{r.duration_seconds}s</td>
                <td>{r.events_generated ?? "—"}</td>
                <td>{r.events_processed ?? "—"}</td>
                <td>{r.avg_latency_ms ? `${Number(r.avg_latency_ms).toFixed(1)} ms` : "—"}</td>
                <td>{r.peak_latency_ms ? `${Number(r.peak_latency_ms).toFixed(1)} ms` : "—"}</td>
                <td>{r.kafka_lag_max ?? "—"}</td>
                <td>{r.status}</td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr><td colSpan={9} className="page-subtitle">
                No benchmark runs yet — start one from the Load Lab page.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
