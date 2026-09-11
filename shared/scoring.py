"""
Risk Scoring (Section 21) and Anomaly Detection (Section 22-23).

Risk scoring is a transparent, additive rubric — every point is explained,
nothing is a black box (Section 21: "Every alert must explain its score").

Anomaly detection uses explainable statistics (mean/stddev/z-score) against
a rolling baseline, exactly as specified in Section 22 — no ML claimed.
"""

from __future__ import annotations
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional

RISK_WEIGHTS = {
    "brute_force": 25,
    "password_spraying": 25,
    "port_scan": 15,
    "privileged_account": 15,
    "suspicious_process": 20,
    "privilege_escalation": 20,
    "data_transfer_anomaly": 15,
    "cross_host_activity": 5,
    "dns_anomaly": 10,
    "suspicious_authentication": 15,
}

SEVERITY_BANDS = [
    (0, 24, "INFO"),
    (25, 49, "LOW"),
    (50, 74, "MEDIUM"),
    (75, 89, "HIGH"),
    (90, 100, "CRITICAL"),
]


def severity_for(score: int) -> str:
    for lo, hi, label in SEVERITY_BANDS:
        if lo <= score <= hi:
            return label
    return "CRITICAL" if score > 100 else "INFO"


def score_detection(detection: Dict[str, Any], is_privileged: bool = False,
                     cross_host: bool = False) -> Dict[str, Any]:
    """
    Compute a transparent, additive risk score for a single detection.
    Returns the score, severity, and an itemized breakdown so the alert
    can literally show its math (Section 21 requirement).
    """
    rule = detection["detection_rule"]
    breakdown = []

    base = RISK_WEIGHTS.get(rule, 10)
    breakdown.append({"reason": f"base weight: {rule}", "points": base})

    total = base
    if is_privileged:
        total += RISK_WEIGHTS["privileged_account"]
        breakdown.append({"reason": "privileged account involved",
                           "points": RISK_WEIGHTS["privileged_account"]})
    if cross_host:
        total += RISK_WEIGHTS["cross_host_activity"]
        breakdown.append({"reason": "activity spans multiple hosts",
                           "points": RISK_WEIGHTS["cross_host_activity"]})

    total = min(total, 100)
    return {
        "risk_score": total,
        "severity": severity_for(total),
        "score_breakdown": breakdown,
    }


# ---------------------------------------------------------------------------
# Anomaly detection — z-score against a rolling baseline (Section 22-23)
# ---------------------------------------------------------------------------

@dataclass
class Baseline:
    """Rolling per-entity baseline (host/user/source_ip/event_type)."""
    key: str
    samples: List[float]
    max_samples: int = 200

    def add(self, value: float) -> None:
        self.samples.append(value)
        if len(self.samples) > self.max_samples:
            self.samples.pop(0)

    def stats(self) -> Optional[Dict[str, float]]:
        if len(self.samples) < 5:
            return None  # not enough history to say anything meaningful
        mu = mean(self.samples)
        sigma = pstdev(self.samples) or 1e-6
        return {"mean": mu, "stddev": sigma}


class BaselineStore:
    """In-memory baseline store keyed by entity. Section 23."""

    def __init__(self):
        self._baselines: Dict[str, Baseline] = {}

    def get_or_create(self, key: str) -> Baseline:
        if key not in self._baselines:
            self._baselines[key] = Baseline(key=key, samples=[])
        return self._baselines[key]

    def evaluate(self, key: str, observed: float, z_threshold: float = 3.0
                 ) -> Optional[Dict[str, Any]]:
        """
        Compare `observed` to the entity's baseline. Records the sample
        regardless, and returns an anomaly record IFF the z-score exceeds
        the threshold. Uses percent deviation as the human-readable figure
        (Section 22 example output).
        """
        b = self.get_or_create(key)
        stats = b.stats()
        result = None
        if stats:
            z = (observed - stats["mean"]) / stats["stddev"]
            if abs(z) >= z_threshold and observed > stats["mean"]:
                pct = ((observed - stats["mean"]) / stats["mean"]) * 100 if stats["mean"] else 0
                result = {
                    "key": key,
                    "observed": observed,
                    "baseline_mean": round(stats["mean"], 2),
                    "baseline_stddev": round(stats["stddev"], 2),
                    "z_score": round(z, 2),
                    "deviation_pct": round(pct, 1),
                }
        b.add(observed)
        return result
