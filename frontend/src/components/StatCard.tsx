export function StatCard({ label, value }: { label: string; value: React.ReactNode }) {
  const unavailable = value === "unavailable" || value === null || value === undefined;
  return (
    <div className="card">
      <div className="metric-label">{label}</div>
      <div className={`metric-value ${unavailable ? "unavailable" : ""}`}>
        {unavailable ? "Metrics unavailable" : value}
      </div>
    </div>
  );
}
