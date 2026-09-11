import { Routes, Route, NavLink } from "react-router-dom";
import Overview from "./pages/Overview";
import Alerts from "./pages/Alerts";
import AlertDetail from "./pages/AlertDetail";
import Incidents from "./pages/Incidents";
import EventExplorer from "./pages/EventExplorer";
import ThreatHunting from "./pages/ThreatHunting";
import LiveStream from "./pages/LiveStream";
import DetectionRules from "./pages/DetectionRules";
import Pipeline from "./pages/Pipeline";
import KafkaPage from "./pages/Kafka";
import SparkPage from "./pages/Spark";
import BigDataLab from "./pages/BigDataLab";
import LoadLab from "./pages/LoadLab";
import Benchmarks from "./pages/Benchmarks";
import MitrePage from "./pages/Mitre";
import Settings from "./pages/Settings";
import Architecture from "./pages/Architecture";

const NAV = [
  { to: "/", label: "Overview" },
  { to: "/live-stream", label: "Live Stream" },
  { to: "/alerts", label: "Alerts" },
  { to: "/incidents", label: "Incidents" },
  { to: "/events", label: "Event Explorer" },
  { to: "/hunting", label: "Threat Hunting" },
  { to: "/detection-rules", label: "Detection Rules" },
  { to: "/mitre", label: "MITRE ATT&CK" },
  { to: "/pipeline", label: "Pipeline" },
  { to: "/kafka", label: "Kafka" },
  { to: "/spark", label: "Spark" },
  { to: "/bigdata-lab", label: "Big Data Lab" },
  { to: "/load-lab", label: "Load Lab" },
  { to: "/benchmarks", label: "Benchmarks" },
  { to: "/settings", label: "Settings" },
  { to: "/architecture", label: "Architecture" },
];

export default function App() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          CYBERSTREAM
          <small>Security Analytics Lab</small>
        </div>
        <nav>
          {NAV.map((n) => (
            <NavLink key={n.to} to={n.to} end={n.to === "/"}
              className={({ isActive }) => (isActive ? "active" : "")}>
              {n.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="main">
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/live-stream" element={<LiveStream />} />
          <Route path="/alerts" element={<Alerts />} />
          <Route path="/alerts/:id" element={<AlertDetail />} />
          <Route path="/incidents" element={<Incidents />} />
          <Route path="/events" element={<EventExplorer />} />
          <Route path="/hunting" element={<ThreatHunting />} />
          <Route path="/detection-rules" element={<DetectionRules />} />
          <Route path="/mitre" element={<MitrePage />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="/kafka" element={<KafkaPage />} />
          <Route path="/spark" element={<SparkPage />} />
          <Route path="/bigdata-lab" element={<BigDataLab />} />
          <Route path="/load-lab" element={<LoadLab />} />
          <Route path="/benchmarks" element={<Benchmarks />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="/architecture" element={<Architecture />} />
        </Routes>
      </main>
    </div>
  );
}
