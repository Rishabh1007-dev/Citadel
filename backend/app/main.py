from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from statistics import fmean, pstdev
from typing import Any
from uuid import uuid4

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from .remediation import runbook_for

app = FastAPI(title="Citadel Telemetry Engine", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5173").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_PATH = os.getenv("DATABASE_PATH", "./telemetry.db")
WINDOW_SIZE = 30
CRITICAL_TICKS = 3


class TelemetryPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    node_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime
    cpu_percent: float = Field(ge=0, le=100)
    memory_mb: float = Field(ge=0)
    latency_ms: float = Field(ge=0)
    status: str = "healthy"


node_histories: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
node_latest: dict[str, dict[str, Any]] = {}
alert_history: deque[dict[str, Any]] = deque(maxlen=500)
stream_clients: set[WebSocket] = set()
agent_clients: dict[str, WebSocket] = {}
critical_streaks: dict[tuple[str, str], int] = defaultdict(int)
active_remediations: dict[str, dict[str, Any]] = {}
auto_heal_enabled = os.getenv("AUTO_HEAL_ENABLED", "true").lower() == "true"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def initialize_db() -> None:
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT NOT NULL, timestamp TEXT NOT NULL,
            cpu_percent REAL, memory_mb REAL, latency_ms REAL, status TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT NOT NULL, metric TEXT NOT NULL,
            severity TEXT NOT NULL, value REAL, z_score REAL, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS remediations (
            id TEXT PRIMARY KEY, node_id TEXT NOT NULL, anomaly_id INTEGER, command TEXT NOT NULL,
            status TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT, details TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_time_node ON metrics(timestamp, node_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(timestamp)")


@app.on_event("startup")
async def startup_event() -> None:
    initialize_db()


async def publish_event(channel: str, payload: dict[str, Any]) -> None:
    client = None
    try:
        client = redis.from_url(REDIS_URL, decode_responses=True)
        await client.publish(channel, json.dumps(payload, default=str))
    except Exception:
        # Redis is an optional fan-out layer; local WebSocket delivery remains available.
        pass
    finally:
        if client is not None:
            await client.aclose()


async def broadcast(message: dict[str, Any]) -> None:
    stale: list[WebSocket] = []
    for ws in tuple(stream_clients):
        try:
            await ws.send_json(message)
        except Exception:
            stale.append(ws)
    for ws in stale:
        stream_clients.discard(ws)
    await publish_event("citadel.stream", message)


def metric_severity(metric: str, current: float, z_score: float) -> str | None:
    hard_critical = (metric == "cpu_percent" and current >= 92) or (metric == "latency_ms" and current >= 350)
    if hard_critical or z_score >= 3.5:
        return "CRITICAL"
    if z_score >= 2.0:
        return "WARNING"
    return None


def compute_anomalies(node_id: str, payload: TelemetryPayload) -> list[dict[str, Any]]:
    history = node_histories[node_id]
    current_row = payload.model_dump(mode="json")
    # Compare against preceding frames, not the current point, to avoid diluting a spike.
    prior = list(history)
    history.append(current_row)
    if len(prior) < 8:
        return []
    output = []
    for metric in ("cpu_percent", "memory_mb", "latency_ms"):
        values = [float(item[metric]) for item in prior[-WINDOW_SIZE:]]
        current = float(getattr(payload, metric))
        mean = fmean(values)
        std = pstdev(values)
        z_score = abs(current - mean) / std if std > 1e-9 else (0.0 if abs(current - mean) < 1e-9 else 99.0)
        severity = metric_severity(metric, current, z_score)
        key = (node_id, metric)
        critical_streaks[key] = critical_streaks[key] + 1 if severity == "CRITICAL" else 0
        if severity:
            output.append({
                "id": None, "node_id": node_id, "metric": metric, "severity": severity,
                "value": current, "z_score": round(z_score, 3),
                "timestamp": payload.timestamp.isoformat(),
            })
    return output


def persist_metric(payload: TelemetryPayload) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO metrics(node_id,timestamp,cpu_percent,memory_mb,latency_ms,status) VALUES(?,?,?,?,?,?)",
            (payload.node_id, payload.timestamp.isoformat(), payload.cpu_percent, payload.memory_mb,
             payload.latency_ms, payload.status),
        )


def persist_anomaly(record: dict[str, Any]) -> int:
    with db() as conn:
        cursor = conn.execute(
            "INSERT INTO anomalies(node_id,metric,severity,value,z_score,timestamp) VALUES(?,?,?,?,?,?)",
            (record["node_id"], record["metric"], record["severity"], record["value"],
             record["z_score"], record["timestamp"]),
        )
        return int(cursor.lastrowid)


async def dispatch_remediation(record: dict[str, Any]) -> dict[str, Any] | None:
    if not auto_heal_enabled or critical_streaks[(record["node_id"], record["metric"])] < CRITICAL_TICKS:
        return None
    if record["node_id"] in active_remediations:
        return None
    runbook = runbook_for(record["metric"])
    remediation_id = str(uuid4())
    timestamp = now_iso()
    entry = {
        "id": remediation_id, "node_id": record["node_id"], "anomaly_id": record["id"],
        "command": runbook.command, "status": "RUNNING", "started_at": timestamp,
        "completed_at": None, "details": runbook.description,
    }
    active_remediations[record["node_id"]] = entry
    with db() as conn:
        conn.execute("INSERT INTO remediations VALUES(?,?,?,?,?,?,?,?)",
                     (remediation_id, entry["node_id"], entry["anomaly_id"], entry["command"],
                      entry["status"], timestamp, None, entry["details"]))
    ws = agent_clients.get(record["node_id"])
    if ws is None:
        entry["status"] = "AGENT_OFFLINE"
        entry["details"] = "Runbook selected, but no agent command channel is connected."
        with db() as conn:
            conn.execute("UPDATE remediations SET status=?, details=? WHERE id=?",
                         (entry["status"], entry["details"], remediation_id))
    else:
        await ws.send_json({"event": "REMEDIATION_COMMAND", **entry})
    await broadcast({"event": "remediation", "remediation": entry, "nodes": list(node_latest.values())})
    return entry


async def process_telemetry(payload: TelemetryPayload) -> dict[str, Any]:
    timestamp = payload.timestamp.isoformat()
    node_latest[payload.node_id] = {
        "node_id": payload.node_id, "cpu_percent": payload.cpu_percent,
        "memory_mb": payload.memory_mb, "latency_ms": payload.latency_ms,
        "status": payload.status, "last_seen": timestamp,
        "health_score": max(0, min(100, 100 - int(max(0, payload.cpu_percent - 70) * 0.8)
                                         - int(max(0, payload.latency_ms - 100) * 0.08))),
        "healing": payload.node_id in active_remediations,
    }
    records = compute_anomalies(payload.node_id, payload)
    persist_metric(payload)
    for record in records:
        record["id"] = persist_anomaly(record)
        alert_history.append(record)
        if record["severity"] == "CRITICAL":
            await dispatch_remediation(record)
    event = {"event": "telemetry", "nodes": list(node_latest.values()), "anomalies": records,
             "timestamp": timestamp, "auto_heal_enabled": auto_heal_enabled}
    await broadcast(event)
    return event


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/ingest")
async def ingest_telemetry(payload: list[TelemetryPayload] | TelemetryPayload) -> dict[str, Any]:
    items = payload if isinstance(payload, list) else [payload]
    return {"status": "accepted", "items": [await process_telemetry(item) for item in items]}


@app.websocket("/ws/agent/{node_id}")
async def ws_agent(websocket: WebSocket, node_id: str) -> None:
    await websocket.accept()
    agent_clients[node_id] = websocket
    await websocket.send_json({"event": "connected", "node_id": node_id})
    try:
        while True:
            message = await websocket.receive_json()
            if message.get("event") == "RECOVERY_CONFIRMED":
                remediation_id = str(message.get("remediation_id", ""))
                entry = active_remediations.get(node_id)
                if entry and entry["id"] == remediation_id:
                    entry.update(status="COMPLETED", completed_at=now_iso(), details=message.get("details", entry["details"]))
                    with db() as conn:
                        conn.execute("UPDATE remediations SET status=?,completed_at=?,details=? WHERE id=?",
                                     (entry["status"], entry["completed_at"], entry["details"], remediation_id))
                    active_remediations.pop(node_id, None)
                    if node_id in node_latest:
                        node_latest[node_id]["healing"] = False
                    await broadcast({"event": "recovery", "node_id": node_id, "remediation": entry,
                                     "nodes": list(node_latest.values()), "timestamp": now_iso()})
    except WebSocketDisconnect:
        pass
    finally:
        if agent_clients.get(node_id) is websocket:
            agent_clients.pop(node_id, None)


@app.websocket("/ws/ingest")
async def ws_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                await process_telemetry(TelemetryPayload.model_validate(item))
            await websocket.send_json({"status": "ok"})
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass


@app.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream_clients.add(websocket)
    try:
        await websocket.send_json({"event": "snapshot", "nodes": list(node_latest.values()),
                                   "anomalies": list(alert_history)[-25:],
                                   "auto_heal_enabled": auto_heal_enabled, "timestamp": now_iso()})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        stream_clients.discard(websocket)


@app.get("/api/v1/nodes")
async def get_nodes() -> dict[str, Any]:
    return {"nodes": sorted(node_latest.values(), key=lambda node: node["node_id"])}


@app.get("/api/v1/anomalies")
async def get_anomalies(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
        rows = conn.execute("SELECT * FROM anomalies ORDER BY id DESC LIMIT ? OFFSET ?",
                            (page_size, (page - 1) * page_size)).fetchall()
    return {"page": page, "page_size": page_size, "total": total, "items": [dict(row) for row in rows]}


@app.post("/api/v1/auto-heal")
async def set_auto_heal(enabled: bool = Query(...)) -> dict[str, bool]:
    global auto_heal_enabled
    auto_heal_enabled = enabled
    result = {"event": "auto-heal", "auto_heal_enabled": enabled, "timestamp": now_iso()}
    await broadcast(result)
    return {"auto_heal_enabled": enabled}


@app.post("/api/v1/nodes/{node_id}/inject-fault")
async def inject_fault(node_id: str, fault: str = Query("memory-leak")) -> dict[str, str]:
    if node_id not in node_latest:
        raise HTTPException(status_code=404, detail="Node not found")
    await broadcast({"event": "fault-injection", "node_id": node_id, "fault": fault, "timestamp": now_iso()})
    return {"node_id": node_id, "fault": fault, "status": "injection requested"}


@app.get("/api/v1/replay")
async def replay(minutes: int = Query(5, ge=1, le=5), limit: int = Query(3000, ge=1, le=10000)) -> dict[str, Any]:
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    with db() as conn:
        rows = conn.execute("SELECT * FROM metrics WHERE timestamp >= ? ORDER BY timestamp ASC LIMIT ?",
                            (cutoff, limit)).fetchall()
    frames = [dict(row) for row in rows]
    for frame in frames:
        history = list(node_histories.get(frame["node_id"], []))
        frame["z_scores"] = {}
        for metric in ("cpu_percent", "memory_mb", "latency_ms"):
            vals = [float(row[metric]) for row in history[:-1]]
            if len(vals) >= 2:
                std = pstdev(vals)
                frame["z_scores"][metric] = round(abs((frame[metric] - fmean(vals)) / std), 3) if std else 0.0
    return {"minutes": minutes, "frames": frames}


@app.get("/api/v1/anomalies/{anomaly_id}/postmortem")
async def postmortem(anomaly_id: int) -> dict[str, Any]:
    with db() as conn:
        anomaly_row = conn.execute("SELECT * FROM anomalies WHERE id=?", (anomaly_id,)).fetchone()
        if anomaly_row is None:
            raise HTTPException(status_code=404, detail="Anomaly not found")
        anomaly = dict(anomaly_row)
        at = datetime.fromisoformat(anomaly["timestamp"])
        start = (at - timedelta(seconds=15)).isoformat()
        samples = conn.execute("SELECT * FROM metrics WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp ASC",
                               (start, anomaly["timestamp"])).fetchall()
        remediations = conn.execute("SELECT * FROM remediations WHERE anomaly_id=? ORDER BY started_at ASC",
                                    (anomaly_id,)).fetchall()
        preceding = conn.execute("SELECT * FROM anomalies WHERE timestamp BETWEEN ? AND ? AND id<>? ORDER BY timestamp ASC",
                                 (start, anomaly["timestamp"], anomaly_id)).fetchall()
    sample_list = [dict(row) for row in samples]
    latest_by_node: dict[str, dict[str, Any]] = {}
    for row in sample_list:
        latest_by_node[row["node_id"]] = row
    dependencies = {
        "API Gateway": ["us-east-node-1", "eu-central-node-2", "ap-south-node-3"],
        "us-east-node-1": ["db-primary-01"], "eu-central-node-2": ["db-primary-01"],
        "ap-south-node-3": ["db-primary-01"], "db-primary-01": [],
    }
    affected = [anomaly["node_id"]]
    if anomaly["node_id"] == "db-primary-01":
        affected.extend(["us-east-node-1", "eu-central-node-2", "ap-south-node-3"])
    elif anomaly["node_id"] in dependencies:
        affected.extend(dependencies[anomaly["node_id"]])
    causal = [dict(row) for row in preceding]
    if causal:
        first = causal[0]
        cause = (f"{first['metric']} anomaly on {first['node_id']} at {first['timestamp']} preceded "
                 f"{anomaly['metric']} on {anomaly['node_id']}.")
    else:
        cause = f"{anomaly['metric']} on {anomaly['node_id']} is the earliest recorded anomaly in the 15-second window."
    return {
        "anomaly": anomaly,
        "root_cause": cause,
        "correlated_anomalies": causal,
        "pre_incident_snapshot": list(latest_by_node.values()),
        "blast_radius": sorted(set(affected)),
        "remediation_timeline": [dict(row) for row in remediations],
        "window_start": start,
        "window_end": anomaly["timestamp"],
    }
