import { useState } from "react";
import { api } from "../lib/api";

export default function LoadLab() {
  const [config, setConfig] = useState({ target_eps: 1000, partitions: 3, duration_seconds: 30 });
  const [run, setRun] = useState<any>(null);
  const [polling, setPolling] = useState(false);

  const start = async () => {
    const res = await api.startBenchmark(config);
    setRun({ run_id: res.run_id, status: "running" });
    setPolling(true);
    poll(res.run_id);
  };

  const poll = (id: string) => {
    const iv = setInterval(async () => {
      const r = await api.getBenchmark(id);
      setRun(r);
      if (r.status === "completed") { clearInterval(iv); setPolling(false); }
    }, 2000);
  };

  return (
    <div>
      <h1>Load Lab</h1>
      <p className="page-subtitle">
        Runs a real, bounded generator burst against Kafka and measures actual latency/lag (Section 36) — not simulated numbers.
      </p>
      <div className="filters">
        <input type="number" value={config.target_eps}
          onChange={(e) => setConfig({ ...config, target_eps: Number(e.target.value) })} />
        <span className="page-subtitle">events/sec</span>
        <input type="number" value={config.duration_seconds}
          onChange={(e) => setConfig({ ...config, duration_seconds: Number(e.target.value) })} />
        <span className="page-subtitle">seconds</span>
        <button className="primary" onClick={start} disabled={polling}>
          {polling ? "Running…" : "Start Test"}
        </button>
      </div>
      {run && (
        <div className="card">
          <table>
            <tbody>
              <tr><td>Run ID</td><td className="mono">{run.run_id}</td></tr>
              <tr><td>Status</td><td>{run.status}</td></tr>
              <tr><td>Events generated</td><td>{run.events_generated ?? "pending"}</td></tr>
              <tr><td>Events processed (delta)</td><td>{run.events_processed ?? "pending"}</td></tr>
              <tr><td>Avg latency</td><td>{run.avg_latency_ms ? `${run.avg_latency_ms.toFixed(1)} ms` : "pending"}</td></tr>
              <tr><td>Peak latency</td><td>{run.peak_latency_ms ? `${run.peak_latency_ms.toFixed(1)} ms` : "pending"}</td></tr>
              <tr><td>Kafka lag (max observed)</td><td>{run.kafka_lag_max ?? "pending"}</td></tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
