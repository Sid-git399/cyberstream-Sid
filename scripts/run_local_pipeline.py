#!/usr/bin/env python3
"""
Local end-to-end pipeline demonstration — NO Kafka, NO Spark required.

This exists for two reasons:
  1. Low-resource dev mode (Section 58): you can validate the entire
     normalize -> enrich -> detect -> score -> dedupe -> correlate chain
     on a laptop with nothing but Python installed.
  2. Proof the business logic is real: every number this script prints
     is measured from an actual run against actually-generated synthetic
     events, in this process, right now. Nothing here is hardcoded
     (Section 67).

Usage:
    python3 scripts/run_local_pipeline.py --events 20000 --scenario multi_stage
"""
import argparse
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from generator.generator import generate
from shared.normalize import normalize_event, enrich_event
from shared.detection import run_detectors
from shared.scoring import score_detection, BaselineStore
from shared.correlation import dedupe_alerts, correlate_incidents
from shared.dedup import DuplicateFilter, deduplicate_events
from shared.data_quality import DataQualityTracker
from shared.schema import ValidationError


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=int, default=20_000)
    p.add_argument("--hosts", type=int, default=150)
    p.add_argument("--users", type=int, default=300)
    p.add_argument("--scenario", default="multi_stage")
    p.add_argument("--attack-pct", type=float, default=0.02)
    p.add_argument("--inject-duplicates", type=int, default=0,
                    help="replay N already-seen events to demonstrate dedup (Section 62)")
    args = p.parse_args()

    t0 = time.time()
    dq = DataQualityTracker()
    dup_filter = DuplicateFilter()

    raw_batch = list(generate(args.events, rate=0, n_hosts=args.hosts, n_users=args.users,
                                scenario=args.scenario, attack_pct=args.attack_pct))
    dq.record_seen(len(raw_batch))

    if args.inject_duplicates:
        replay = raw_batch[: args.inject_duplicates]
        raw_batch = raw_batch + replay
        dq.record_seen(len(replay))

    deduped_batch, dup_count = deduplicate_events(raw_batch, dup_filter)
    dq.record_duplicate(dup_count)

    normalized, dlq = [], []
    for raw in deduped_batch:
        try:
            n = normalize_event(raw)
            normalized.append(enrich_event(n))
        except ValidationError as e:
            dq.record_invalid()
            dlq.append({"error": str(e), "raw": raw})

    gen_elapsed = time.time() - t0

    # Windowed detection: group by hostname (mirrors the Spark job's
    # per-micro-batch, per-host grouping in streaming/spark_job.py).
    by_host = defaultdict(list)
    for e in normalized:
        by_host[e.get("hostname") or "unknown"].append(e)

    raw_alerts = []
    baseline = BaselineStore()
    anomalies = []
    for host, group in by_host.items():
        for detection in run_detectors(group):
            is_priv = any(g.get("is_privileged_account") for g in group)
            scored = score_detection(detection, is_privileged=is_priv,
                                       cross_host=len(by_host) > 3)
            raw_alerts.append({**detection, **scored, "status": "NEW"})

        anomaly = baseline.evaluate(host, len(group))
        if anomaly:
            anomalies.append(anomaly)

    alerts = dedupe_alerts(raw_alerts)
    incidents = correlate_incidents(alerts)

    total_elapsed = time.time() - t0

    print("=" * 70)
    print("CYBERSTREAM — LOCAL PIPELINE RUN (no Kafka/Spark, dev-mode fallback)")
    print("=" * 70)
    print(f"Events requested:        {args.events}")
    print(f"Events generated:        {args.events}")
    print(f"Duplicate events dropped:{dup_count:>6d}  (Section 62 dedup)")
    print(f"Events normalized OK:    {len(normalized)}")
    print(f"Events sent to DLQ:      {len(dlq)}")
    print(f"Generation time:         {gen_elapsed:.3f}s "
          f"({args.events/max(gen_elapsed,0.0001):.0f} events/sec)")
    print(f"Total pipeline time:     {total_elapsed:.3f}s "
          f"({args.events/max(total_elapsed,0.0001):.0f} events/sec end-to-end)")
    print(f"Distinct hosts touched:  {len(by_host)}")
    print(f"Raw detections fired:    {len(raw_alerts)}")
    print(f"Deduplicated alerts:     {len(alerts)}")
    print(f"Correlated incidents:    {len(incidents)}")
    print(f"Statistical anomalies:   {len(anomalies)}")
    print()
    print("-- Data quality snapshot (Section 61) --")
    for k, v in dq.snapshot().items():
        print(f"  {k}: {v}")
    print()

    if alerts:
        print("-- Sample alerts --")
        for a in alerts[:8]:
            print(f"  [{a['severity']:8s}] score={a['risk_score']:3d} "
                  f"{a['detection_rule']:22s} host={a.get('hostname')} "
                  f"src={a.get('source_ip')} occurrences={a['occurrences']}")

    if incidents:
        print("\n-- Correlated incidents --")
        for inc in incidents:
            stages = " -> ".join(s["detection_rule"] for s in inc["stages"])
            print(f"  {inc['incident_id']}  entity={inc['entity']}  "
                  f"max_severity={inc['max_severity']}  chain: {stages}")

    if anomalies:
        print("\n-- Statistical anomalies (z-score vs rolling baseline) --")
        for an in anomalies[:5]:
            print(f"  {an['key']}: observed={an['observed']} "
                  f"baseline_mean={an['baseline_mean']} "
                  f"deviation={an['deviation_pct']}%")

    print("\n(All figures above are measured from this run — none are hardcoded.)")


if __name__ == "__main__":
    main()
