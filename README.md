# Citadel

A production-grade real-time system telemetry and anomaly detection engine built as a three-service monorepo.

## Architecture

- `agent/`: simulates distributed node telemetry and sends it to the backend
- `backend/`: FastAPI ingestion and anomaly detection engine with WebSocket streaming
- `frontend/`: React + TypeScript dashboard for monitoring live telemetry

## Running locally

```bash
docker compose up --build
```

Then visit:

- Frontend: http://localhost:5173
- Backend API: http://localhost:8000/docs
- Redis: localhost:6379

## Features

- Real-time metrics generation from 4 virtual nodes
- Rolling z-score + EMA-based anomaly detection
- Redis Pub/Sub + WebSocket streaming to dashboard clients
- SQLite-backed telemetry persistence
- Fault injection controls from the UI or API
- Dark-mode telemetry monitoring dashboard

## Agent chaos mode

```bash
docker compose run --rm agent --chaos-mode
```

or locally:

```bash
cd agent
python generator.py --chaos-mode
```
