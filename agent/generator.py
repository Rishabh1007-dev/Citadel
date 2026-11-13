from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
from datetime import datetime, timezone
from typing import Any

import httpx

NODE_IDS = [
    "us-east-node-1",
    "eu-central-node-2",
    "ap-south-node-3",
    "db-primary-01",
]


class TelemetryGenerator:
    def __init__(self, backend_url: str, chaos_mode: bool = False):
        self.backend_url = backend_url.rstrip("/")
        self.chaos_mode = chaos_mode
        self.node_states = {
            node_id: {
                "cpu_bias": random.uniform(0.8, 1.3),
                "memory_bias": random.uniform(0.9, 1.2),
                "latency_bias": random.uniform(0.7, 1.5),
                "status": "healthy",
                "leak_phase": random.uniform(0, 30),
            }
            for node_id in NODE_IDS
        }

    def build_payload(self, node_id: str, tick: int) -> dict[str, Any]:
        state = self.node_states[node_id]
        base_cpu = 36 + (tick % 18) * 1.4
        base_memory = 960 + ((tick * 7) % 200)
        base_latency = 24 + ((tick * 3) % 35)

        if self.chaos_mode:
            # Gradual memory leak over ~30s
            leak_factor = (tick % 60) / 60
            if tick % 60 >= 15:
                base_memory += state["memory_bias"] * (tick % 60) * 3.6

            # Sudden latency spike every ~12 seconds
            if tick % 12 == 0:
                base_latency += random.uniform(180, 500)

            # Random CPU thrash burst
            if random.random() < 0.08:
                base_cpu += random.uniform(24, 52)

            # DDoS-like load burst
            if random.random() < 0.04:
                base_latency += random.uniform(120, 350)
                base_cpu += random.uniform(18, 36)

        cpu = min(100, max(12, base_cpu * state["cpu_bias"]))
        memory = max(320, base_memory * state["memory_bias"]) + random.uniform(0, 40)
        latency = max(10, base_latency * state["latency_bias"]) + random.uniform(0, 12)

        status = "healthy"
        if cpu > 88 or latency > 220:
            status = "warning"
        if cpu > 95 or latency > 350:
            status = "critical"

        return {
            "node_id": node_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": round(cpu, 2),
            "memory_mb": round(memory, 2),
            "latency_ms": round(latency, 2),
            "status": status,
        }

    async def send_batch(self, batch: list[dict[str, Any]]) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(f"{self.backend_url}/api/v1/ingest", json=batch)
            response.raise_for_status()

    async def run(self) -> None:
        tick = 0
        while True:
            batch = [self.build_payload(node_id, tick) for node_id in NODE_IDS]
            try:
                await self.send_batch(batch)
            except Exception as exc:  # pragma: no cover
                print(f"Telemetry send failed: {exc}")

            tick += 1
            await asyncio.sleep(0.5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate distributed node telemetry")
    parser.add_argument("--chaos-mode", action="store_true", help="Enable simulated anomalies")
    parser.add_argument("--backend-url", default=os.getenv("BACKEND_URL", "http://localhost:8000"))
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    generator = TelemetryGenerator(args.backend_url, chaos_mode=args.chaos_mode or os.getenv("CHAOS_MODE", "false").lower() == "true")
    await generator.run()


if __name__ == "__main__":
    asyncio.run(main())
