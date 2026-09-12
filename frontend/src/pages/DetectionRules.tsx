import { useEffect, useState } from "react";
import { api } from "../lib/api";

export default function DetectionRules() {
  const [data, setData] = useState<any>(null);
  useEffect(() => { api.detectionRules().then(setData); }, []);

  return (
    <div>
      <h1>Detection Rules</h1>
      <p className="page-subtitle">
        The actual thresholds and window sizes in effect — read live from
        shared/detection.py and shared/windowing.py, not a separate description
        that could drift from the real logic.
      </p>

      <div className="card" style={{ marginBottom: 14 }}>
        <table>
          <thead>
            <tr><th>Rule</th><th>Window</th><th>Threshold</th><th>Risk weight</th></tr>
          </thead>
          <tbody>
            {data?.rules.map((r: any) => (
              <tr key={r.rule}>
                <td className="mono">{r.rule}</td>
                <td>{r.window_seconds ? `${r.window_seconds / 60} min` : "instantaneous"}</td>
                <td>{r.threshold ?? "—"}</td>
                <td>+{r.risk_weight}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data && (
        <div className="card">
          <h2>Severity bands</h2>
          <table>
            <thead><tr><th>Score range</th><th>Severity</th></tr></thead>
            <tbody>
              {data.severity_bands.map((b: any) => (
                <tr key={b.label}><td>{b.min}–{b.max}</td><td>{b.label}</td></tr>
              ))}
            </tbody>
          </table>
          <div className="page-subtitle" style={{ marginTop: 10 }}>
            Alert correlation window: {data.correlation_window_seconds / 60} minutes.
          </div>
        </div>
      )}
    </div>
  );
}
