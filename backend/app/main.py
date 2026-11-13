from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timezone
from statistics import fmean, pstdev
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict, field_validator

app = FastAPI(title="Citadel Telemetry Engine", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = "redis://redis:6379/0"
WINDOW_SIZE = 30


class TelemetryPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    node_id: str
    timestamp: str
    cpu_percent: float = Field(ge=0, le=100)
    memory_mb: float = Field(ge=0)
    latency_ms: float = Field(ge=0)
    status: str = "healthy"


class AnomalyModel(BaseModel):
    node_id: str
    metric: str
    severity: str
    value: float
    z_score: float
    timestamp: str


class NodeState(BaseModel):
    node_id: str
    status: str
    last_seen: str
    cpu_percent: float
    memory_mb: float
    latency_ms: float
    health_score: int


class AlertMessage(BaseModel):
    kind: str
    payload: dict[str, Any]


node_histories: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
node_latest: dict[str, dict[str, Any]] = {}
alert_history: deque[dict[str, Any]] = deque(maxlen=500)
stream_clients: list[WebSocket] = []


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect("./telemetry.db")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            cpu_percent REAL,
            memory_mb REAL,
            latency_ms REAL,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            severity TEXT NOT NULL,
            value REAL,
            z_score REAL,
            timestamp TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


async def publish_event(channel: str, payload: dict[str, Any]) -> None:
    try:
        client = redis.from_url(REDIS_URL, decode_responses=True)
        await client.publish(channel, json.dumps(payload))
        await client.aclose()
    except Exception:
        pass


async def broadcast(message: dict[str, Any]) -> None:
    for ws in list(stream_clients):
        try:
            await ws.send_json(message)
        except Exception:
            stream_clients.remove(ws)
    await publish_event("citadel.stream", message)


def compute_anomalies(node_id: str, payload: TelemetryPayload) -> list[dict[str, Any]]:
    history = node_histories[node_id]
    history.append(payload.model_dump())

    if len(history) < 10:
        return []

    detected: list[dict[str, Any]] = []
    for metric in ("cpu_percent", "memory_mb", "latency_ms"):
        values = [float(item[metric]) for item in history]
        current = values[-1]
        mean = fmean(values)
        std = pstdev(values) if len(values) > 1 else 1.0
        z_score = abs((current - mean) / std) if std else 0.0

        severity = None
        if z_score >= 3.5:
            severity = "CRITICAL"
        elif z_score >= 2.0:
            severity = "WARNING"

        if metric == "cpu_percent" and current >= 92:
            severity = "CRITICAL"
            z_score = max(z_score, 3.5)

        if severity:
            detected.append(
                {
                    "node_id": node_id,
                    "metric": metric,
                    "severity": severity,
                    "value": current,
                    "z_score": round(z_score, 3),
                    "timestamp": payload.timestamp,
                }
            )

    return detected


async def persist_telemetry(payload: TelemetryPayload) -> None:
    conn = connect_db()
    conn.execute(
        "INSERT INTO metrics (node_id, timestamp, cpu_percent, memory_mb, latency_ms, status) VALUES (?, ?, ?, ?, ?, ?)",
        (
            payload.node_id,
            payload.timestamp,
            payload.cpu_percent,
            payload.memory_mb,
            payload.latency_ms,
            payload.status,
        ),
    )
    conn.commit()
    conn.close()


async def persist_anomaly(record: dict[str, Any]) -> None:
    conn = connect_db()
    conn.execute(
        "INSERT INTO anomalies (node_id, metric, severity, value, z_score, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (
            record["node_id"],
            record["metric"],
            record["severity"],
            record["value"],
            record["z_score"],
            record["timestamp"],
        ),
    )
    conn.commit()
    conn.close()


async def process_telemetry(payload: TelemetryPayload) -> dict[str, Any]:
    payload_dict = payload.model_dump()
    node_latest[payload.node_id] = {
        "node_id": payload.node_id,
        "cpu_percent": payload.cpu_percent,
        "memory_mb": payload.memory_mb,
        "latency_ms": payload.latency_ms,
        "status": payload.status,
        "last_seen": payload.timestamp,
    }

    anomalies = compute_anomalies(payload.node_id, payload)
    for record in anomalies:
        alert_history.append(record)
        await persist_anomaly(record)

    await persist_telemetry(payload)

    state = {
        "nodes": list(node_latest.values()),
        "anomalies": list(anomalies),
        "event": "telemetry",
        "timestamp": payload.timestamp,
    }
    await broadcast(state)
    return state


@app.on_event("startup")
async def startup_event() -> None:
    connect_db()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/ingest")
async def ingest_telemetry(payload: list[TelemetryPayload] | TelemetryPayload) -> dict[str, Any]:
    items = payload if isinstance(payload, list) else [payload]
    results = []
    for item in items:
        results.append(await process_telemetry(item))
    return {"status": "accepted", "items": results}


@app.websocket("/ws/ingest")
async def ws_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            raw = await websocket.receive_text()
            payload = json.loads(raw)
            if isinstance(payload, list):
                for item in payload:
                    await process_telemetry(TelemetryPayload(**item))
            else:
                await process_telemetry(TelemetryPayload(**payload))
            await websocket.send_json({"status": "ok"})
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream_clients.append(websocket)
    try:
        snapshot = {
            "event": "snapshot",
            "nodes": list(node_latest.values()),
            "anomalies": list(alert_history)[-25:],
            "timestamp": now_iso(),
        }
        await websocket.send_json(snapshot)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        stream_clients.remove(websocket)


@app.get("/api/v1/nodes")
async def get_nodes() -> dict[str, Any]:
    nodes = []
    for node_id in sorted(node_latest):
        detail = node_latest[node_id]
        nodes.append(
            {
                "node_id": node_id,
                "status": detail.get("status", "healthy"),
                "last_seen": detail.get("last_seen", now_iso()),
                "cpu_percent": detail.get("cpu_percent", 0.0),
                "memory_mb": detail.get("memory_mb", 0.0),
                "latency_ms": detail.get("latency_ms", 0.0),
                "health_score": 100 if detail.get("status") == "healthy" else 62,
            }
        )
    return {"nodes": nodes}


@app.get("/api/v1/anomalies")
async def get_anomalies(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    items = list(alert_history)
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": page_items,
    }


@app.post("/api/v1/nodes/{node_id}/inject-fault")
async def inject_fault(node_id: str, fault: str = "memory-leak") -> dict[str, str]:
    if node_id not in node_latest:
        raise HTTPException(status_code=404, detail="Node not found")

    node_latest[node_id]["status"] = "warning"
    alert_history.append(
        {
            "node_id": node_id,
            "metric": fault,
            "severity": "WARNING",
            "value": 1,
            "z_score": 2.5,
            "timestamp": now_iso(),
        }
    )
    await broadcast({"event": "fault-injection", "node_id": node_id, "fault": fault, "timestamp": now_iso()})
    return {"node_id": node_id, "fault": fault, "status": "injected"}
