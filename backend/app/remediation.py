from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Runbook:
    fault: str
    command: str
    description: str


RUNBOOKS = {
    "memory_mb": Runbook(
        fault="memory_leak",
        command="FLUSH_CACHE_AND_RESTART_WORKER",
        description="Flush the node cache and restart its worker process.",
    ),
    "cpu_percent": Runbook(
        fault="cpu_thrashing",
        command="THROTTLE_INGRESS_AND_REBALANCE",
        description="Throttle ingress briefly and rebalance work across the cluster.",
    ),
    "latency_ms": Runbook(
        fault="high_latency",
        command="REROUTE_TRAFFIC_TO_BACKUP",
        description="Reroute traffic to the healthy backup route.",
    ),
}


def runbook_for(metric: str) -> Runbook:
    """Return the deterministic remediation runbook for a telemetry metric."""
    return RUNBOOKS.get(metric, RUNBOOKS["latency_ms"])
