import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function SparkPage() {
  const [status, setStatus] = useState<any>(null);

  useEffect(() => {
    const load = () => api.sparkStatus().then(setStatus).catch(() => setStatus(null));
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Spark</h1>
      <p className="page-subtitle">Structured Streaming query status pulled from Spark's own REST API (Section 34).</p>
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
                  <tr><td>Query</td><td className="mono">{status.query_name}</td></tr>
                  <tr><td>Batches processed</td><td className="mono">{status.batches_processed}</td></tr>
                  <tr><td>Input rows/sec</td><td className="mono">{status.input_rows_per_second ?? "unavailable"}</td></tr>
                  <tr><td>Processed rows/sec</td><td className="mono">{status.processed_rows_per_second ?? "unavailable"}</td></tr>
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
