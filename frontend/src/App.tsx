import { useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, CircleDashed, Cpu, Gauge, HardDrive, MemoryStick, Wifi } from "lucide-react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type NodeStatus = {
  node_id: string;
  status: string;
  cpu_percent: number;
  memory_mb: number;
  latency_ms: number;
  last_seen: string;
  health_score: number;
};

type AlertItem = {
  node_id: string;
  metric: string;
  severity: string;
  value: number;
  z_score: number;
  timestamp: string;
};

const apiBase = "http://localhost:8000";

const formatTime = (iso: string) => {
  const date = new Date(iso);
  return isNaN(date.getTime()) ? "--:--:--" : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
};

const getSeverityClasses = (severity: string) => {
  if (severity === "CRITICAL") return "bg-red-500/15 text-red-300 border-red-500/30";
  if (severity === "WARNING") return "bg-amber-500/15 text-amber-200 border-amber-500/30";
  return "bg-slate-500/10 text-slate-200 border-slate-500/20";
};

function App() {
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [selectedNode, setSelectedNode] = useState<string>("all");
  const [chartData, setChartData] = useState<Array<Record<string, any>>>([]);

  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000/ws/stream");

    ws.onopen = () => {
      console.info("Connected to telemetry stream");
    };

    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.event === "snapshot" || payload.event === "telemetry" || payload.event === "fault-injection") {
        if (payload.nodes) {
          setNodes(payload.nodes);
        }
        if (payload.anomalies && payload.anomalies.length) {
          setAlerts((current) => [
            ...payload.anomalies.map((a: AlertItem) => ({ ...a, timestamp: a.timestamp ?? new Date().toISOString() })),
            ...current,
          ].slice(0, 25));
        }
      }

      if (payload.nodes) {
        const now = new Date().toISOString();
        const nextPoint = {
          time: formatTime(now),
          ...Object.fromEntries(payload.nodes.map((node: NodeStatus) => [node.node_id, node.cpu_percent])),
        };
        setChartData((current) => [...current, nextPoint].slice(-24));
      }
    };

    ws.onerror = () => {
      console.error("WebSocket error");
    };

    return () => ws.close();
  }, []);

  useEffect(() => {
    const fetchNodes = async () => {
      try {
        const res = await fetch(`${apiBase}/api/v1/nodes`);
        const data = await res.json();
        setNodes(data.nodes || []);
      } catch (_error) {
        console.warn("could not fetch nodes");
      }
    };

    const fetchAlerts = async () => {
      try {
        const res = await fetch(`${apiBase}/api/v1/anomalies?page=1&page_size=20`);
        const data = await res.json();
        setAlerts(data.items || []);
      } catch (_error) {
        console.warn("could not fetch alerts");
      }
    };

    fetchNodes();
    fetchAlerts();
  }, []);

  const filteredNodes = useMemo(
    () => (selectedNode === "all" ? nodes : nodes.filter((node) => node.node_id === selectedNode)),
    [nodes, selectedNode],
  );

  const totals = useMemo(() => {
    const avgLatency = nodes.length
      ? nodes.reduce((sum, node) => sum + node.latency_ms, 0) / nodes.length
      : 0;
    const avgHealth = nodes.length
      ? nodes.reduce((sum, node) => sum + node.health_score, 0) / nodes.length
      : 100;
    const criticalCount = alerts.filter((alert) => alert.severity === "CRITICAL").length;
    return {
      activeNodes: nodes.length,
      latency: avgLatency,
      healthScore: Math.round(avgHealth),
      criticalAlerts: criticalCount,
    };
  }, [nodes, alerts]);

  return (
    <div className="min-h-screen bg-bg p-6 text-zinc-100">
      <div className="mx-auto max-w-7xl">
        <header className="mb-6 flex items-center justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.28em] text-violet-300">Citadel</p>
            <h1 className="mt-2 text-3xl font-semibold">System Telemetry Command Center</h1>
          </div>
          <div className="rounded-full border border-violet-500/40 bg-violet-500/10 px-4 py-2 text-sm text-violet-200">
            <span className="inline-flex items-center gap-2"><CircleDashed className="h-4 w-4" /> Live data stream</span>
          </div>
        </header>

        <section className="mb-6 grid gap-4 md:grid-cols-4">
          <MetricCard icon={<Activity className="h-5 w-5" />} label="Active Nodes" value={String(totals.activeNodes)} hint="across 4 clusters" />
          <MetricCard icon={<Gauge className="h-5 w-5" />} label="Global Avg Latency" value={`${totals.latency.toFixed(0)} ms`} hint="rolling 30s" />
          <MetricCard icon={<Cpu className="h-5 w-5" />} label="System Health Score" value={`${totals.healthScore}%`} hint="anomaly weighted" />
          <MetricCard icon={<AlertTriangle className="h-5 w-5" />} label="Active Critical Alerts" value={String(totals.criticalAlerts)} hint="root-cause view" />
        </section>

        <section className="mb-6 rounded-2xl border border-border bg-panel p-4 shadow-glow">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <p className="text-sm uppercase tracking-[0.2em] text-zinc-400">Live multi-node telemetry</p>
              <h2 className="mt-2 text-xl font-semibold">Resource intensity and error patterns</h2>
            </div>
            <div className="flex gap-2">
              <button className={`rounded-lg border px-3 py-2 text-sm ${selectedNode === "all" ? "border-violet-500/50 bg-violet-500/15 text-violet-100" : "border-border bg-panelAlt text-zinc-300"}`} onClick={() => setSelectedNode("all")}>All Nodes</button>
              {nodes.map((node) => (
                <button key={node.node_id} className={`rounded-lg border px-3 py-2 text-sm ${selectedNode === node.node_id ? "border-violet-500/50 bg-violet-500/15 text-violet-100" : "border-border bg-panelAlt text-zinc-300"}`} onClick={() => setSelectedNode(node.node_id)}>{node.node_id}</button>
              ))}
            </div>
          </div>

          <div className="h-72 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                <CartesianGrid stroke="#27272a" strokeDasharray="6 6" />
                <XAxis dataKey="time" stroke="#71717a" />
                <YAxis stroke="#71717a" domain={[0, 100]} />
                <Tooltip contentStyle={{ background: "#18181b", border: "1px solid #27272a", borderRadius: 12 }} />
                {filteredNodes.length ? filteredNodes.map((node) => (
                  <Line key={node.node_id} type="monotone" dataKey={node.node_id} stroke={node.node_id.includes("db") ? "#f59e0b" : node.node_id.includes("eu") ? "#38bdf8" : node.node_id.includes("ap") ? "#22c55e" : "#a78bfa"} strokeWidth={2.5} dot={false} />
                )) : <Line dataKey="system" stroke="#a78bfa" dot={false} />}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.6fr_1fr]">
          <div className="rounded-2xl border border-border bg-panel p-4">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-xl font-semibold">Node status grid</h3>
              <span className="text-xs uppercase tracking-[0.2em] text-zinc-400">Live</span>
            </div>
            <div className="grid gap-4 md:grid-cols-2">
              {nodes.length ? nodes.map((node) => (
                <NodeCard key={node.node_id} node={node} />
              )) : <div className="rounded-xl border border-dashed border-border p-8 text-center text-zinc-500">Waiting for telemetry...</div>}
            </div>
          </div>

          <div className="rounded-2xl border border-border bg-panel p-4">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-xl font-semibold">Anomaly alert feed</h3>
              <span className="rounded-full border border-red-500/30 bg-red-500/10 px-2 py-1 text-xs uppercase tracking-[0.2em] text-red-200">{alerts.filter((a) => a.severity === "CRITICAL").length} critical</span>
            </div>
            <div className="space-y-3">
              {alerts.length ? alerts.slice(0, 10).map((alert, idx) => (
                <div key={`${alert.node_id}-${alert.metric}-${idx}`} className={`rounded-xl border p-3 ${getSeverityClasses(alert.severity)}`}>
                  <div className="mb-1 flex items-center justify-between gap-2">
                    <p className="font-medium">{alert.node_id}</p>
                    <span className="text-xs uppercase tracking-[0.18em]">{alert.severity}</span>
                  </div>
                  <p className="text-sm text-zinc-200">{alert.metric} breached z-score threshold</p>
                  <div className="mt-2 flex items-center justify-between text-xs text-zinc-300">
                    <span>z={alert.z_score.toFixed(2)}</span>
                    <span>{formatTime(alert.timestamp)}</span>
                  </div>
                </div>
              )) : <div className="rounded-xl border border-dashed border-border p-8 text-center text-zinc-500">No active anomalies</div>}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

function MetricCard({ icon, label, value, hint }: { icon: React.ReactNode; label: string; value: string; hint: string }) {
  return (
    <div className="rounded-2xl border border-border bg-panel p-4">
      <div className="mb-3 flex items-center justify-between text-violet-300">{icon}</div>
      <div className="text-sm text-zinc-400">{label}</div>
      <div className="mt-1 font-mono text-3xl font-semibold">{value}</div>
      <div className="mt-2 text-xs text-zinc-500">{hint}</div>
    </div>
  );
}

function NodeCard({ node }: { node: NodeStatus }) {
  const sparkline = [
    { value: Math.max(0, node.cpu_percent - 10) },
    { value: node.cpu_percent },
    { value: Math.min(100, node.cpu_percent + 8) },
    { value: Math.max(0, node.cpu_percent - 16) },
    { value: node.cpu_percent },
  ];

  return (
    <div className="rounded-xl border border-border bg-panelAlt p-4">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <p className="font-medium text-zinc-200">{node.node_id}</p>
          <p className="text-xs uppercase tracking-[0.2em] text-zinc-500">{node.status}</p>
        </div>
        <span className={`rounded-full px-2 py-1 text-xs ${node.status === "critical" ? "bg-red-500/15 text-red-300" : node.status === "warning" ? "bg-amber-500/15 text-amber-200" : "bg-emerald-500/15 text-emerald-300"}`}>
          {node.health_score}%
        </span>
      </div>

      <div className="mb-4 h-12 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={sparkline}>
            <defs>
              <linearGradient id={`grad-${node.node_id}`} x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.9} />
                <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0.1} />
              </linearGradient>
            </defs>
            <Area type="monotone" dataKey="value" stroke="#8b5cf6" fill={`url(#grad-${node.node_id})`} strokeWidth={2} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <div className="grid grid-cols-3 gap-2 text-sm">
        <div className="rounded-lg border border-border bg-bg/60 p-2">
          <div className="flex items-center gap-2 text-zinc-400"><Cpu className="h-3.5 w-3.5" /> CPU</div>
          <div className="mt-2 font-mono text-lg">{node.cpu_percent.toFixed(1)}%</div>
        </div>
        <div className="rounded-lg border border-border bg-bg/60 p-2">
          <div className="flex items-center gap-2 text-zinc-400"><MemoryStick className="h-3.5 w-3.5" /> RAM</div>
          <div className="mt-2 font-mono text-lg">{node.memory_mb.toFixed(0)} MB</div>
        </div>
        <div className="rounded-lg border border-border bg-bg/60 p-2">
          <div className="flex items-center gap-2 text-zinc-400"><Wifi className="h-3.5 w-3.5" /> Latency</div>
          <div className="mt-2 font-mono text-lg">{node.latency_ms.toFixed(0)} ms</div>
        </div>
      </div>
    </div>
  );
}

export default App;
