import { useEffect, useState } from "react";
import { api } from "../lib/api";

export default function Settings() {
  const [data, setData] = useState<any>(null);
  useEffect(() => { api.settings().then(setData); }, []);

  return (
    <div>
      <h1>Settings</h1>
      <p className="page-subtitle">
        Read-only view of the environment configuration actually in effect for this
        running stack (Section 56). Change these in `.env` and restart the affected
        containers — this page does not push changes.
      </p>

      {data && (
        <div className="grid grid-2">
          <div className="card">
            <h2>Kafka</h2>
            <table>
              <tbody>
                <tr><td>Brokers</td><td className="mono">{data.kafka.brokers}</td></tr>
                <tr><td>Topic</td><td className="mono">{data.kafka.topic}</td></tr>
                <tr><td>Consumer group</td><td className="mono">{data.kafka.consumer_group}</td></tr>
              </tbody>
            </table>
          </div>

          <div className="card">
            <h2>Spark</h2>
            <table>
              <tbody>
                <tr><td>Shuffle partitions</td><td className="mono">{data.spark.shuffle_partitions}</td></tr>
              </tbody>
            </table>
          </div>

          <div className="card">
            <h2>Retention (Section 39)</h2>
            <table>
              <tbody>
                <tr><td>Raw events</td><td>{data.retention_days.raw} days</td></tr>
                <tr><td>Processed events</td><td>{data.retention_days.processed} days</td></tr>
                <tr><td>Alerts / incidents</td><td>{data.retention_days.alerts} days</td></tr>
              </tbody>
            </table>
          </div>

          <div className="card">
            <h2>Storage paths</h2>
            <table>
              <tbody>
                {Object.entries(data.storage_paths).map(([k, v]: any) => (
                  <tr key={k}><td>{k}</td><td className="mono">{v}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="card" style={{ marginTop: 14 }}>
        <h2>Export report</h2>
        <p className="page-subtitle">A full pipeline report (Section 64), built from live data only.</p>
        <div style={{ display: "flex", gap: 8 }}>
          <a href={api.reportText()} target="_blank" rel="noreferrer"><button className="primary">Download text report</button></a>
          <a href={api.reportJson()} target="_blank" rel="noreferrer"><button className="primary">View JSON report</button></a>
        </div>
      </div>
    </div>
  );
}
