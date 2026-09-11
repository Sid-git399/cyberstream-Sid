import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import { Badge } from "../components/Badge";

export default function Incidents() {
  const [data, setData] = useState<any>({ incidents: [], total: 0 });

  useEffect(() => {
    const load = () => api.incidents().then(setData).catch(() => setData({ incidents: [], total: 0 }));
    load();
    const id = setInterval(load, 8000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>Incidents</h1>
      <p className="page-subtitle">
        Related alerts on the same host/user/source, chained into a single incident
        rather than treated as unrelated tickets (Sections 20, 43). {data.total} total.
      </p>

      <div className="card">
        <table>
          <thead>
            <tr><th>Incident</th><th>Entity</th><th>Stages</th><th>Chain</th><th>Max severity</th></tr>
          </thead>
          <tbody>
            {data.incidents?.map((inc: any) => (
              <tr key={inc.incident_id}>
                <td className="mono">{inc.incident_id}</td>
                <td className="mono">{inc.entity}</td>
                <td>{inc.stage_count}</td>
                <td className="mono">
                  {(inc.stages || []).map((s: any, i: number) => (
                    <span key={i}>
                      {i > 0 && " → "}
                      {s.alert_id ? <Link to={`/alerts/${s.alert_id}`}>{s.detection_rule}</Link> : s.detection_rule}
                    </span>
                  ))}
                </td>
                <td>{inc.max_severity && <Badge value={inc.max_severity} />}</td>
              </tr>
            ))}
            {(!data.incidents || data.incidents.length === 0) && (
              <tr><td colSpan={5} className="page-subtitle">
                No correlated incidents yet — these appear once at least two related
                alerts fire against the same host within the correlation window.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
