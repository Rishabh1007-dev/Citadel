import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, Cpu, Gauge, MemoryStick, Wifi, X, Download, Play, Pause, ShieldCheck } from "lucide-react";
import { Area, AreaChart, CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

type NodeStatus = { node_id: string; status: string; cpu_percent: number; memory_mb: number; latency_ms: number; last_seen: string; health_score: number; healing?: boolean };
type AlertItem = { id: number; node_id: string; metric: string; severity: string; value: number; z_score: number; timestamp: string };
type Frame = { node_id: string; timestamp: string; cpu_percent: number; memory_mb: number; latency_ms: number; z_scores?: Record<string, number> };
type Report = { anomaly: AlertItem; root_cause: string; correlated_anomalies: AlertItem[]; pre_incident_snapshot: Frame[]; blast_radius: string[]; remediation_timeline: Array<{command: string; status: string; started_at: string; completed_at?: string | null; details: string}> };
const apiBase = import.meta.env.VITE_BACKEND_URL ?? "http://localhost:8000";
const wsBase = apiBase.replace("https://", "wss://").replace("http://", "ws://");
const colors = ["#a78bfa", "#38bdf8", "#34d399", "#f59e0b"];
const formatTime = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const severityClass = (s: string) => s === "CRITICAL" ? "border-red-500/40 bg-red-500/10 text-red-200" : "border-amber-500/40 bg-amber-500/10 text-amber-100";

function App() {
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [chartData, setChartData] = useState<Array<Record<string, any>>>([]);
  const [selectedNode, setSelectedNode] = useState("all");
  const [autoHeal, setAutoHeal] = useState(true);
  const [report, setReport] = useState<Report | null>(null);
  const [replayMode, setReplayMode] = useState(false);
  const [frames, setFrames] = useState<Frame[]>([]);
  const [replayIndex, setReplayIndex] = useState(0);
  const [loadingReport, setLoadingReport] = useState(false);

  const addFrame = useCallback((incoming: NodeStatus[], timestamp = new Date().toISOString()) => {
    setChartData(old => [...old, { time: formatTime(timestamp), ...Object.fromEntries(incoming.map(n => [n.node_id, n.cpu_percent])) }].slice(-180));
  }, []);

  useEffect(() => {
    let ws: WebSocket;
    let reconnect: number;
    let disposed = false;
    const connect = () => {
      ws = new WebSocket(`${wsBase}/ws/stream`);
      ws.onmessage = event => {
        const message = JSON.parse(event.data);
        if (message.auto_heal_enabled !== undefined) setAutoHeal(message.auto_heal_enabled);
        if (message.nodes) {
          setNodes(message.nodes);
          if (!replayMode) addFrame(message.nodes, message.timestamp);
        }
        if (message.anomalies?.length) setAlerts(old => [...message.anomalies, ...old].filter((a: AlertItem, i: number, all: AlertItem[]) => all.findIndex(x => x.id === a.id) === i).slice(0, 40));
      };
      ws.onclose = () => { if (!disposed) reconnect = window.setTimeout(connect, 1500); };
    };
    connect();
    return () => { disposed = true; window.clearTimeout(reconnect); ws?.close(); };
  }, [addFrame, replayMode]);

  useEffect(() => {
    fetch(`${apiBase}/api/v1/anomalies?page=1&page_size=30`).then(r => r.json()).then(data => setAlerts(data.items ?? [])).catch(() => undefined);
  }, []);

  const toggleAutoHeal = async () => {
    try {
      const response = await fetch(`${apiBase}/api/v1/auto-heal?enabled=${!autoHeal}`, { method: "POST" });
      if (!response.ok) throw new Error("Unable to update auto-heal setting");
      setAutoHeal(!autoHeal);
    } catch (error) { console.error(error); }
  };

  const loadReplay = async () => {
    try {
      const response = await fetch(`${apiBase}/api/v1/replay?minutes=5`);
      const data = await response.json();
      setFrames(data.frames ?? []);
      setReplayIndex(Math.max(0, (data.frames ?? []).length - 1));
      setReplayMode(true);
    } catch (error) { console.error(error); }
  };

  const replayNodes = useMemo(() => {
    if (!replayMode || !frames.length) return nodes;
    const timestamp = frames[replayIndex]?.timestamp;
    const pointFrames = frames.filter(frame => frame.timestamp === timestamp);
    return pointFrames.map(frame => ({ ...frame, status: "replay", last_seen: frame.timestamp, health_score: 100 } as NodeStatus));
  }, [frames, nodes, replayIndex, replayMode]);
  const displayedNodes = replayMode ? replayNodes : nodes;
  const selected = selectedNode === "all" ? displayedNodes : displayedNodes.filter(n => n.node_id === selectedNode);
  const averages = displayedNodes.length ? displayedNodes.reduce((sum, n) => sum + n.latency_ms, 0) / displayedNodes.length : 0;
  const health = displayedNodes.length ? Math.round(displayedNodes.reduce((sum, n) => sum + n.health_score, 0) / displayedNodes.length) : 100;
  const critical = alerts.filter(a => a.severity === "CRITICAL").length;

  const openReport = async (alert: AlertItem) => {
    if (!alert.id) return;
    setLoadingReport(true);
    try { const response = await fetch(`${apiBase}/api/v1/anomalies/${alert.id}/postmortem`); if (!response.ok) throw new Error("Report unavailable"); setReport(await response.json()); }
    catch (error) { console.error(error); }
    finally { setLoadingReport(false); }
  };

  const downloadReport = () => {
    if (!report) return;
    const body = `# Incident Post-Mortem — ${report.anomaly.node_id}\n\n- **Incident:** ${report.anomaly.metric} (${report.anomaly.severity})\n- **Timestamp:** ${report.anomaly.timestamp}\n- **Value / Z-score:** ${report.anomaly.value} / ${report.anomaly.z_score}\n\n## Root cause analysis\n${report.root_cause}\n\n## Blast radius\n${report.blast_radius.map(node => `- ${node}`).join("\n")}\n\n## Pre-incident snapshot\n${report.pre_incident_snapshot.map(row => `- ${row.node_id}: CPU ${row.cpu_percent}%, memory ${row.memory_mb} MB, latency ${row.latency_ms} ms at ${row.timestamp}`).join("\n")}\n\n## Remediation timeline\n${report.remediation_timeline.length ? report.remediation_timeline.map(item => `- ${item.started_at}: ${item.command} — ${item.status}${item.completed_at ? ` (completed ${item.completed_at})` : ""}`).join("\n") : "- No remediation recorded."}\n`;
    const url = URL.createObjectURL(new Blob([body], { type: "text/markdown" }));
    const link = document.createElement("a"); link.href = url; link.download = `citadel-incident-${report.anomaly.id}.md`; link.click(); URL.revokeObjectURL(url);
  };

  return <main className="min-h-screen bg-bg p-5 text-zinc-100 md:p-8">
    <div className="mx-auto max-w-[1500px]">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div><p className="text-xs uppercase tracking-[.28em] text-violet-300">Citadel / observability</p><h1 className="mt-2 text-3xl font-semibold">System Command Center</h1><p className="mt-1 text-sm text-zinc-500">Real-time telemetry · autonomous incident response</p></div>
        <button onClick={toggleAutoHeal} className={`inline-flex items-center gap-2 rounded-xl border px-4 py-3 text-sm font-medium ${autoHeal ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200" : "border-zinc-700 bg-zinc-900 text-zinc-300"}`}><ShieldCheck size={17}/> Auto-Heal: {autoHeal ? "ON" : "OFF"}</button>
      </header>
      <section className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi icon={<Activity/>} label="Active nodes" value={`${displayedNodes.length}`} /><Kpi icon={<Gauge/>} label="Global avg latency" value={`${averages.toFixed(0)} ms`} /><Kpi icon={<Cpu/>} label="System health" value={`${health}%`} /><Kpi icon={<AlertTriangle/>} label="Critical alerts" value={`${critical}`} />
      </section>
      <Topology nodes={displayedNodes}/>
      <section className="my-5 rounded-2xl border border-border bg-panel p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3"><div><div className="text-xs uppercase tracking-[.2em] text-zinc-500">Live telemetry</div><h2 className="mt-1 text-lg font-semibold">CPU utilization by node</h2></div><div className="flex flex-wrap gap-2"><button onClick={() => setSelectedNode("all")} className={tabClass(selectedNode === "all")}>All nodes</button>{nodes.map(n => <button key={n.node_id} onClick={() => setSelectedNode(n.node_id)} className={tabClass(selectedNode === n.node_id)}>{n.node_id}</button>)}</div></div>
        <div className="h-72"><ResponsiveContainer width="100%" height="100%"><LineChart data={chartData}><CartesianGrid stroke="#27272a" strokeDasharray="5 5"/><XAxis dataKey="time" stroke="#71717a"/><YAxis domain={[0,100]} stroke="#71717a"/><Tooltip contentStyle={{background:"#18181b",border:"1px solid #27272a",borderRadius:10}}/><ReferenceLine y={92} stroke="#ef4444" strokeDasharray="4 4" label="Critical"/>{(selected.length ? selected : nodes).map(n => <Line key={n.node_id} dataKey={n.node_id} stroke={colors[Math.max(0,nodes.findIndex(x => x.node_id === n.node_id)) % colors.length]} dot={false} type="monotone" strokeWidth={2}/>)}</LineChart></ResponsiveContainer></div>
      </section>
      <section className="grid gap-5 xl:grid-cols-[1.4fr_.8fr]"><div className="rounded-2xl border border-border bg-panel p-4"><h2 className="mb-4 text-lg font-semibold">Node health matrix</h2><div className="grid gap-3 md:grid-cols-2">{displayedNodes.map(n => <NodeCard key={n.node_id} node={n}/>)}</div>{!displayedNodes.length && <p className="p-8 text-center text-zinc-500">Waiting for telemetry…</p>}</div>
        <div className="rounded-2xl border border-border bg-panel p-4"><div className="mb-4 flex items-center justify-between"><h2 className="text-lg font-semibold">Live anomaly feed</h2><span className="text-xs text-zinc-500">Select an incident for analysis</span></div><div className="max-h-[560px] space-y-2 overflow-auto">{alerts.map((a, i) => <button key={a.id ?? `${a.node_id}-${i}`} onClick={() => openReport(a)} className={`block w-full rounded-xl border p-3 text-left transition hover:brightness-125 ${severityClass(a.severity)}`}><div className="flex justify-between gap-2"><b>{a.node_id}</b><span className="text-xs tracking-wider">{a.severity}</span></div><div className="mt-1 text-sm">{a.metric} · {a.value} · z={Number(a.z_score).toFixed(2)}</div><div className="mt-1 text-xs opacity-70">{formatTime(a.timestamp)} · Incident #{a.id ?? "pending"}</div></button>)}{!alerts.length && <p className="p-8 text-center text-zinc-500">No anomalies detected.</p>}</div></div></section>
      <div className="fixed inset-x-0 bottom-0 z-30 border-t border-border bg-[#111113]/95 px-5 py-3 backdrop-blur"><div className="mx-auto flex max-w-[1500px] flex-wrap items-center gap-4"><button onClick={() => replayMode ? setReplayMode(false) : void loadReplay()} className="inline-flex items-center gap-2 rounded-lg border border-border bg-panel px-3 py-2 text-sm">{replayMode ? <><Play size={15}/> Go Live</> : <><Pause size={15}/> Replay</>}</button><span className={`text-xs font-semibold uppercase tracking-widest ${replayMode ? "text-amber-300" : "text-emerald-300"}`}>{replayMode ? "Replay mode" : "Live mode"}</span>{replayMode && <><input aria-label="Replay timestamp" className="min-w-40 flex-1 accent-violet-500" type="range" min={0} max={Math.max(0,frames.length-1)} value={replayIndex} onChange={e => setReplayIndex(Number(e.target.value))}/><span className="min-w-40 font-mono text-xs text-zinc-400">{frames[replayIndex] ? formatTime(frames[replayIndex].timestamp) : "No frames"} · {frames.length} frames</span></>}</div></div>
      {(report || loadingReport) && <div className="fixed inset-0 z-40 flex justify-end bg-black/60" onClick={() => setReport(null)}><aside className="h-full w-full max-w-xl overflow-y-auto border-l border-border bg-[#111113] p-6 shadow-2xl" onClick={e => e.stopPropagation()}><div className="mb-6 flex items-start justify-between"><div><p className="text-xs uppercase tracking-widest text-violet-300">Incident analysis</p><h2 className="mt-1 text-xl font-semibold">Post-mortem report</h2></div><button aria-label="Close report" onClick={() => setReport(null)} className="rounded-lg border border-border p-2"><X size={18}/></button></div>{loadingReport ? <p className="text-zinc-400">Analyzing incident…</p> : report && <><div className={`rounded-xl border p-4 ${severityClass(report.anomaly.severity)}`}><div className="font-semibold">{report.anomaly.node_id} · {report.anomaly.metric}</div><div className="mt-2 font-mono text-sm">{report.anomaly.value} · Z {report.anomaly.z_score} · {report.anomaly.timestamp}</div></div><Section title="Deterministic root-cause analysis"><p>{report.root_cause}</p>{report.correlated_anomalies.map((a,i)=><p key={i} className="mt-2 text-xs text-zinc-400">{a.timestamp} — {a.node_id}: {a.metric} ({a.severity})</p>)}</Section><Section title="Blast radius"><div className="flex flex-wrap gap-2">{report.blast_radius.map(n=><span key={n} className="rounded-full border border-cyan-500/30 bg-cyan-500/10 px-3 py-1 text-xs text-cyan-100">{n}</span>)}</div></Section><Section title="Pre-incident metric snapshot">{report.pre_incident_snapshot.length ? report.pre_incident_snapshot.map(f=><div key={f.node_id} className="mb-2 rounded-lg bg-zinc-900 p-3 text-xs"><b>{f.node_id}</b><div className="mt-1 font-mono text-zinc-300">CPU {f.cpu_percent}% · RAM {f.memory_mb} MB · latency {f.latency_ms} ms</div><div className="mt-1 text-zinc-500">{f.timestamp}</div></div>) : <p>No telemetry frames in the incident window.</p>}</Section><Section title="Automated remediation timeline">{report.remediation_timeline.length ? report.remediation_timeline.map(r=><div key={`${r.started_at}-${r.command}`} className="mb-2 border-l-2 border-cyan-500 pl-3"><b className="text-cyan-200">{r.command}</b><p className="text-xs text-zinc-400">{r.started_at} · {r.status}{r.completed_at ? ` · completed ${r.completed_at}` : ""}</p><p className="text-xs text-zinc-500">{r.details}</p></div>) : <p>No remediation action recorded for this incident.</p>}</Section><button onClick={downloadReport} className="mt-4 inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-3 text-sm font-semibold hover:bg-violet-500"><Download size={16}/> Download Incident Report (.md)</button></>}</aside></div>}
    </div>
  </main>;
}
function tabClass(active: boolean) { return `rounded-lg border px-3 py-2 text-xs ${active ? "border-violet-500/50 bg-violet-500/15 text-violet-100" : "border-border bg-panelAlt text-zinc-300"}`; }
function Kpi({icon,label,value}:{icon:React.ReactNode;label:string;value:string}) { return <div className="rounded-2xl border border-border bg-panel p-4"><div className="mb-3 text-violet-300">{icon}</div><div className="text-sm text-zinc-400">{label}</div><div className="mt-1 font-mono text-3xl font-semibold">{value}</div></div>; }
function NodeCard({node}:{node:NodeStatus}) { const color = node.healing ? "#06b6d4" : node.status === "critical" ? "#ef4444" : node.status === "warning" ? "#f59e0b" : "#10b981"; return <div className="rounded-xl border border-border bg-panelAlt p-4"><div className="mb-3 flex justify-between"><div><b>{node.node_id}</b><p className="text-xs uppercase text-zinc-500">{node.healing ? "self-healing" : node.status}</p></div><span className="font-mono" style={{color}}>{node.health_score}%</span></div><div className="mb-3 h-12"><ResponsiveContainer width="100%" height="100%"><AreaChart data={[node.cpu_percent-8,node.cpu_percent-3,node.cpu_percent,node.cpu_percent+2,node.cpu_percent].map(value=>({value}))}><Area dataKey="value" stroke={color} fill={color} fillOpacity={.16}/></AreaChart></ResponsiveContainer></div><div className="grid grid-cols-3 gap-2 text-xs"><span><Cpu size={13} className="inline"/> {node.cpu_percent.toFixed(1)}%</span><span><MemoryStick size={13} className="inline"/> {node.memory_mb.toFixed(0)} MB</span><span><Wifi size={13} className="inline"/> {node.latency_ms.toFixed(0)} ms</span></div></div>; }
function Section({title,children}:{title:string;children:React.ReactNode}) { return <section className="mt-6"><h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-300">{title}</h3><div className="text-sm leading-6 text-zinc-400">{children}</div></section>; }
function Topology({nodes}:{nodes:NodeStatus[]}) { const colorFor = (id:string) => {const node=nodes.find(n=>n.node_id===id); return node?.healing ? "#06b6d4" : node?.status === "critical" ? "#ef4444" : node?.status === "warning" ? "#f59e0b" : "#10b981";}; const regional=["us-east-node-1","eu-central-node-2","ap-south-node-3"]; return <section className="rounded-2xl border border-border bg-panel p-4"><div className="mb-2 flex items-center justify-between"><div><p className="text-xs uppercase tracking-[.2em] text-zinc-500">Live traffic map</p><h2 className="text-lg font-semibold">Service topology</h2></div><div className="flex gap-3 text-[10px] text-zinc-400"><span className="text-emerald-400">● Healthy</span><span className="text-amber-400">● Warning</span><span className="text-red-400">● Critical</span><span className="text-cyan-400">● Healing</span></div></div><div className="overflow-x-auto"><svg viewBox="0 0 1000 220" className="min-w-[700px] w-full" role="img" aria-label="Traffic flow from API gateway through regional nodes to primary database"><defs>{regional.map(id=><marker key={id} id={`arrow-${id}`} markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" fill={colorFor(id)}/></marker>)}</defs><rect x="30" y="82" width="155" height="56" rx="13" fill="#202024" stroke="#8b5cf6"/><text x="107" y="116" fill="#f4f4f5" textAnchor="middle" fontSize="14">API Gateway</text>{regional.map((id,i)=>{const y=25+i*70;const color=colorFor(id);return <g key={id}><path d={`M185 110 C245 110 235 ${y+27} 300 ${y+27}`} fill="none" stroke={color} strokeWidth="2" markerEnd={`url(#arrow-${id})`} opacity=".8"/><circle r="4" fill={color}><animateMotion dur={`${1.5+i*.25}s`} repeatCount="indefinite" path={`M185 110 C245 110 235 ${y+27} 300 ${y+27}`}/></circle><rect x="305" y={y} width="220" height="54" rx="12" fill="#202024" stroke={color} strokeWidth="2" className={color==="#ef4444"?"critical-node":""}/><text x="415" y={y+23} fill="#f4f4f5" textAnchor="middle" fontSize="12">{id}</text><text x="415" y={y+40} fill={color} textAnchor="middle" fontSize="10">{nodes.find(n=>n.node_id===id)?.status ?? "awaiting telemetry"}</text><path d={`M525 ${y+27} C620 ${y+27} 610 110 715 110`} fill="none" stroke={colorFor("db-primary-01")} strokeWidth="2" markerEnd="url(#arrow-${id})" opacity=".8"/><circle r="4" fill={colorFor("db-primary-01")}><animateMotion dur={`${1.8+i*.2}s`} repeatCount="indefinite" path={`M525 ${y+27} C620 ${y+27} 610 110 715 110`}/></circle></g>;})}<rect x="720" y="82" width="235" height="56" rx="13" fill="#202024" stroke={colorFor("db-primary-01")} strokeWidth="2"/><text x="837" y="106" fill="#f4f4f5" textAnchor="middle" fontSize="13">Primary Database</text><text x="837" y="124" fill={colorFor("db-primary-01")} textAnchor="middle" fontSize="10">db-primary-01</text></svg></div></section>; }
export default App;
