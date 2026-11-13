from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.main import TelemetryPayload, compute_anomalies, metric_severity, node_histories
from app.remediation import runbook_for


def test_metric_severity_respects_z_score_and_hard_thresholds() -> None:
    assert metric_severity("cpu_percent", 93, 0) == "CRITICAL"
    assert metric_severity("latency_ms", 351, 0) == "CRITICAL"
    assert metric_severity("memory_mb", 100, 2.1) == "WARNING"
    assert metric_severity("memory_mb", 100, 3.6) == "CRITICAL"
    assert metric_severity("cpu_percent", 40, 0.2) is None


def test_runbooks_are_deterministic() -> None:
    assert runbook_for("memory_mb").command == "FLUSH_CACHE_AND_RESTART_WORKER"
    assert runbook_for("cpu_percent").command == "THROTTLE_INGRESS_AND_REBALANCE"
    assert runbook_for("latency_ms").command == "REROUTE_TRAFFIC_TO_BACKUP"


def test_detector_flags_outlier_against_prior_window() -> None:
    node = "pytest-node"
    history = node_histories[node]
    history.clear()
    stamp = datetime.now(timezone.utc)
    for index in range(10):
        history.append({"node_id": node, "timestamp": (stamp - timedelta(seconds=10-index)).isoformat(),
                        "cpu_percent": 30.0, "memory_mb": 1000.0, "latency_ms": 25.0, "status": "healthy"})
    payload = TelemetryPayload(node_id=node, timestamp=stamp, cpu_percent=30, memory_mb=1000, latency_ms=500)
    detected = compute_anomalies(node, payload)
    assert any(item["metric"] == "latency_ms" and item["severity"] == "CRITICAL" for item in detected)
