from __future__ import annotations

import argparse
import asyncio
import os
import random
from datetime import datetime, timezone
from typing import Any

import httpx
import websockets

NODE_IDS = ["us-east-node-1", "eu-central-node-2", "ap-south-node-3", "db-primary-01"]


class TelemetryGenerator:
    def __init__(self, backend_url: str, chaos_mode: bool = False, interval: float = 0.5):
        self.backend_url = backend_url.rstrip("/")
        self.ws_base = self.backend_url.replace("https://", "wss://").replace("http://", "ws://")
        self.chaos_mode = chaos_mode
        self.interval = interval
        self.tick = 0
        self.nodes = {node: {"cpu": 0.0, "memory": 0.0, "latency": 0.0,
                             "recovery": None, "recovery_started": 0.0}
                      for node in NODE_IDS}

    def baseline(self, node_id: str, tick: int) -> dict[str, float]:
        offset = NODE_IDS.index(node_id)
        return {"cpu_percent": 28 + offset * 5 + (tick % 12) * 0.7,
                "memory_mb": 850 + offset * 100 + (tick % 20) * 2.0,
                "latency_ms": 30 + offset * 8 + (tick % 9) * 1.5}

    def build_payload(self, node_id: str, tick: int) -> dict[str, Any]:
        state = self.nodes[node_id]
        values = self.baseline(node_id, tick)
        if self.chaos_mode:
            phase = tick % 120
            if node_id == "db-primary-01" and 30 <= phase < 100:
                values["memory_mb"] += (phase - 29) * 18
            if tick % 40 in range(1, 7):
                values["cpu_percent"] += 38 + random.random() * 12
            if tick % 29 in range(1, 5):
                values["latency_ms"] += 330 + random.random() * 120
        recovery = state["recovery"]
        if recovery:
            elapsed = asyncio.get_running_loop().time() - state["recovery_started"]
            progress = min(1.0, elapsed / 4.0)
            for key, base in self.baseline(node_id, tick).items():
                values[key] = recovery[key] + (base - recovery[key]) * progress
            if progress >= 1.0:
                state["recovery"] = None
        state.update(cpu=values["cpu_percent"], memory=values["memory_mb"], latency=values["latency_ms"])
        status = "critical" if values["cpu_percent"] >= 92 or values["latency_ms"] >= 350 else "healthy"
        return {"node_id": node_id, "timestamp": datetime.now(timezone.utc).isoformat(),
                **{key: round(value, 2) for key, value in values.items()}, "status": status}

    async def perform_recovery(self, node_id: str, ws: Any, command: dict[str, Any]) -> None:
        state = self.nodes[node_id]
        state["recovery"] = {"cpu_percent": state["cpu"], "memory_mb": state["memory"], "latency_ms": state["latency"]}
        state["recovery_started"] = asyncio.get_running_loop().time()
        action = command.get("command", "UNKNOWN")
        print(f"[{node_id}] executing remediation {action} ({command.get('id')})", flush=True)
        await asyncio.sleep(4.1)
        await ws.send(__import__("json").dumps({
            "event": "RECOVERY_CONFIRMED", "remediation_id": command.get("id"),
            "details": f"{action} completed; metrics returned to baseline over 4 seconds.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))

    async def command_listener(self, node_id: str) -> None:
        url = f"{self.ws_base}/ws/agent/{node_id}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        import json
                        message = json.loads(raw)
                        if message.get("event") == "REMEDIATION_COMMAND":
                            asyncio.create_task(self.perform_recovery(node_id, ws, message))
            except (OSError, websockets.WebSocketException) as exc:
                print(f"[{node_id}] command channel reconnecting: {exc}", flush=True)
                await asyncio.sleep(2)

    async def telemetry_loop(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                batch = [self.build_payload(node, self.tick) for node in NODE_IDS]
                try:
                    response = await client.post(f"{self.backend_url}/api/v1/ingest", json=batch)
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    print(f"Telemetry send failed: {exc}", flush=True)
                self.tick += 1
                await asyncio.sleep(self.interval)

    async def run(self) -> None:
        await asyncio.gather(self.telemetry_loop(), *(self.command_listener(node) for node in NODE_IDS))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate distributed node telemetry")
    parser.add_argument("--chaos-mode", action="store_true", help="Enable simulated anomalies")
    parser.add_argument("--backend-url", default=os.getenv("BACKEND_URL", "http://localhost:8000"))
    parser.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL_SECONDS", "0.5")))
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    chaos = args.chaos_mode or os.getenv("CHAOS_MODE", "false").lower() == "true"
    await TelemetryGenerator(args.backend_url, chaos_mode=chaos, interval=max(0.1, args.interval)).run()


if __name__ == "__main__":
    asyncio.run(main())
