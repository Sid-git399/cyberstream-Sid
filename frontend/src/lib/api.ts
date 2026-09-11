const API_URL = (import.meta as any).env?.VITE_API_URL || "http://localhost:8000";

async function req(path: string, opts: RequestInit = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!res.ok) throw new Error(`API ${path} -> ${res.status}`);
  return res.json();
}

export const api = {
  overview: () => req("/api/overview"),
  pipelineHealth: () => req("/api/pipeline/health"),
  kafkaStatus: () => req("/api/kafka/status"),
  sparkStatus: () => req("/api/spark/status"),
  throughput: () => req("/api/pipeline/throughput"),
  dataQuality: () => req("/api/pipeline/data-quality"),
  dlq: (params: Record<string, string> = {}) => req(`/api/pipeline/dlq?${new URLSearchParams(params)}`),
  mitre: () => req("/api/mitre"),
  alerts: (params: Record<string, string> = {}) =>
    req(`/api/alerts?${new URLSearchParams(params)}`),
  alert: (id: string) => req(`/api/alerts/${id}`),
  updateAlertStatus: (id: string, status: string) =>
    req(`/api/alerts/${id}/status?status=${status}`, { method: "PATCH" }),
  incidents: (params: Record<string, string> = {}) =>
    req(`/api/incidents?${new URLSearchParams(params)}`),
  incident: (id: string) => req(`/api/incidents/${id}`),
  events: (params: Record<string, string> = {}) =>
    req(`/api/events?${new URLSearchParams(params)}`),
  hunt: (query: Record<string, string>) =>
    req("/api/events/hunt", { method: "POST", body: JSON.stringify(query) }),
  startBenchmark: (body: any) =>
    req("/api/benchmarks", { method: "POST", body: JSON.stringify(body) }),
  getBenchmark: (id: string) => req(`/api/benchmarks/${id}`),
  listBenchmarks: () => req("/api/benchmarks"),
  detectionRules: () => req("/api/detection-rules"),
  settings: () => req("/api/settings"),
  bigDataConcepts: () => req("/api/bigdata-lab/concepts"),
  reportText: () => `${API_URL}/api/reports/text`,
  reportJson: () => `${API_URL}/api/reports`,
};

