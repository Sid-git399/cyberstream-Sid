import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";

export default function BigDataLab() {
  const [data, setData] = useState<any>(null);
  useEffect(() => { api.bigDataConcepts().then(setData); }, []);

  return (
    <div>
      <h1>Big Data Lab</h1>
      <p className="page-subtitle">
        The difference between traditional small-scale analysis and distributed
        processing, explained against this system's own architecture.
      </p>

      {data && (
        <>
          <div className="grid grid-3" style={{ marginBottom: 16 }}>
            {Object.entries(data.volume_velocity_variety).map(([k, v]: any) => (
              <div className="card" key={k}>
                <div className="metric-label">{k}</div>
                <div style={{ marginTop: 8, lineHeight: 1.5 }}>{v}</div>
              </div>
            ))}
          </div>

          <div className="card">
            <h2>Core concepts</h2>
            <table>
              <tbody>
                {Object.entries(data.concepts).map(([k, v]: any) => (
                  <tr key={k}>
                    <td className="mono" style={{ width: 180, verticalAlign: "top" }}>{k.replace(/_/g, " ")}</td>
                    <td>{v}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      <div className="card" style={{ marginTop: 14 }}>
        <h2>Run a controlled workload</h2>
        <p className="page-subtitle">
          Push real events through Kafka at a configured rate and watch throughput,
          latency, and Kafka lag respond — measured, not simulated.
        </p>
        <Link to="/load-lab"><button className="primary">Go to Load Lab</button></Link>
      </div>
    </div>
  );
}
