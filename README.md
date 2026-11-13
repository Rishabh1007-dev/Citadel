# Citadel

Citadel is a real-time telemetry, anomaly detection, and autonomous remediation demo built as a three-service monorepo.

## Architecture

- `agent/`: simulates four distributed nodes and maintains a per-node WebSocket command channel.
- `backend/`: FastAPI ingestion, 30-frame rolling detector, SQLite persistence, anomaly analysis, and command orchestration.
- `frontend/`: React + TypeScript dashboard with live charts, topology, incident post-mortems, and replay.
- Redis is used as an optional stream fan-out layer; local WebSocket clients remain functional if Redis is unavailable.

## Run locally

```bash
docker compose up --build
```

Open <http://localhost:5173> for the dashboard and <http://localhost:8000/docs> for the API.

## Autonomous self-healing

Auto-heal is enabled by default and can be toggled in the dashboard or through `POST /api/v1/auto-heal?enabled=false`. A critical metric must persist for three consecutive telemetry ticks before dispatch. Runbooks map memory pressure to `FLUSH_CACHE_AND_RESTART_WORKER`, CPU pressure to `THROTTLE_INGRESS_AND_REBALANCE`, and latency to `REROUTE_TRAFFIC_TO_BACKUP`. The agent receives node-targeted commands at `/ws/agent/{node_id}`, simulates a four-second linear recovery to baseline, and reports `RECOVERY_CONFIRMED`. This simulator does not restart host processes or alter real production traffic.

Enable simulated degradations with `docker compose run --rm agent --chaos-mode` or `CHAOS_MODE=true`.

## Topology and incident analysis

The dashboard visualizes API Gateway → US/EU/AP regional nodes → primary database traffic. Node/edge colors indicate current health, and animated packets show direction. Click an anomaly for a deterministic 15-second cross-node correlation report, pre-incident metric snapshot, modeled blast radius, and remediation timeline. Download the report as Markdown.

## Time travel

The bottom scrubber loads up to five minutes of persisted telemetry from `GET /api/v1/replay?minutes=5`; select a frame to inspect historical node metrics, or return to Live mode. Frames are capped for API response size.

## API summary

- `POST /api/v1/ingest` — ingest one telemetry point or a batch.
- `GET /api/v1/nodes` — current node state.
- `GET /api/v1/anomalies?page=1&page_size=20` — persisted anomaly history.
- `GET /api/v1/anomalies/{id}/postmortem` — incident correlation and remediation report.
- `GET /api/v1/replay?minutes=5` — historical telemetry frames.
- `POST /api/v1/nodes/{node_id}/inject-fault?fault=memory-leak` — publish a fault-injection request.
- `POST /api/v1/auto-heal?enabled=true|false` — change the global remediation switch.
- `/ws/stream` — dashboard event stream; `/ws/agent/{node_id}` — authenticated-by-network simulator command channel.

For deployments beyond local development, configure restrictive `CORS_ORIGINS`, secure the API/agent channels with authentication and TLS, and use managed PostgreSQL/Redis rather than local SQLite.
