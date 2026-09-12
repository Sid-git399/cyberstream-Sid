import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function KafkaPage() {
  const [status, setStatus] = useState<any>(null);

  useEffect(() => {
    const load = () => api.kafkaStatus().then(setStatus).catch(() => setStatus(null));
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Kafka</h1>
      <p className="page-subtitle">Topics, partitions, consumer group and lag (Section 33) — real broker metadata only.</p>
      {status && (
        <>
          <div className="card" style={{ marginBottom: 14 }}>
            <div className="metric-label">Status</div>
            <Badge value={status.status} />
            {status.reason && <div className="page-subtitle">{status.reason}</div>}
          </div>
          {status.status === "ONLINE" && (
            <div className="card">
              <table>
                <tbody>
                  <tr><td>Topic</td><td className="mono">{status.topic}</td></tr>
                  <tr><td>Partitions</td><td className="mono">{JSON.stringify(status.partitions)}</td></tr>
                  <tr><td>Consumer group</td><td className="mono">{status.consumer_group}</td></tr>
                  <tr><td>Total lag</td><td className="mono">{status.total_lag ?? "unavailable"}</td></tr>
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
