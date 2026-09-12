import { useEffect, useState } from "react";
import { api } from "../lib/api";

export default function MitrePage() {
  const [data, setData] = useState<any>(null);
  useEffect(() => { api.mitre().then(setData); }, []);

  return (
    <div>
      <h1>MITRE ATT&CK Mapping</h1>
      <p className="page-subtitle">Local technique dataset (Section 25, 51) — no internet dependency.</p>
      <div className="card">
        <table>
          <thead><tr><th>Detection rule</th><th>Technique ID</th><th>Name</th><th>Tactic</th></tr></thead>
          <tbody>
            {data && Object.entries(data.techniques).map(([rule, t]: any) => (
              <tr key={rule}>
                <td className="mono">{rule}</td>
                <td className="mono">{t.id}</td>
                <td>{t.name}</td>
                <td>{t.tactic}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
