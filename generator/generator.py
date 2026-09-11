"""
CyberStream Synthetic Event Generator
======================================
Generates SAFE, SYNTHETIC security telemetry only. It never executes
attacks, never touches real hosts, and never collects real credentials —
it produces JSON events that *represent* the kind of activity a SOC would
see, for a Kafka producer to publish (Section 9, 49).

Usage:
    python generator.py --preset small --scenario normal
    python generator.py --events 100000 --rate 2000 --scenario brute_force --sink kafka
    python generator.py --events 5000 --scenario port_scan --sink stdout
"""

from __future__ import annotations
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.schema import SecurityEvent, EVENT_TYPES  # noqa: E402
from shared.windowing import WINDOW_SECONDS  # noqa: E402

PRESETS = {
    "small": 100_000,
    "medium": 1_000_000,
    "large": 10_000_000,
    "stress": 50_000_000,
}

SCENARIOS = [
    "normal", "brute_force", "password_spraying", "port_scan",
    "privilege_escalation", "multi_stage", "traffic_spike", "alert_storm",
]


def _snap_to_window_start(t: datetime, window_seconds: int) -> datetime:
    """
    Round `t` UP to the start of the next window boundary of the given
    size. Used to anchor attack bursts (brute force, password spraying,
    etc.) so the whole burst deterministically lands inside a single
    detection window, regardless of the wall-clock time the generator
    happens to run at.

    Without this, a burst whose span is close to its detector's window
    size (e.g. 12 failed logins over ~55s against a 60s brute-force
    window) can straddle a window boundary purely by bad luck — splitting
    into two under-threshold groups and silently never triggering the
    detector it was designed to demonstrate. Snapping to the *next*
    boundary (not the nearest) guarantees the maximum possible margin:
    the burst starts right as a fresh window opens.
    """
    epoch = t.timestamp()
    next_boundary = (int(epoch) // window_seconds + 1) * window_seconds
    return datetime.fromtimestamp(next_boundary, tz=t.tzinfo)


class HostPool:
    def __init__(self, n_hosts: int, n_users: int, seed: int = 42):
        rnd = random.Random(seed)
        self.hosts = [f"WS-{i:04d}" if i % 20 else f"SRV-{i:04d}" for i in range(n_hosts)]
        self.users = [f"user{i:04d}" for i in range(n_users)]
        self.privileged_users = ["administrator", "root", "svc-backup"]
        self.internal_ips = [f"10.20.{rnd.randint(0,255)}.{rnd.randint(1,254)}"
                              for _ in range(n_hosts)]
        self.external_ips = [f"{rnd.randint(20,220)}.{rnd.randint(0,255)}."
                              f"{rnd.randint(0,255)}.{rnd.randint(1,254)}"
                              for _ in range(200)]
        self.countries = ["DZ", "FR", "US", "DE", "CN", "RU", "GB", "MA", "BR"]
        self.rnd = rnd


def _base_event(pool: HostPool, ts: datetime) -> dict:
    rnd = pool.rnd
    event_type = rnd.choices(
        list(EVENT_TYPES.keys()), weights=[30, 30, 15, 5, 10, 10]
    )[0]
    action = rnd.choice(EVENT_TYPES[event_type])
    status = "success" if "fail" not in action and "denied" not in action else "failure"

    username = rnd.choice(pool.users)
    src = rnd.choice(pool.internal_ips)
    if event_type == "authentication":
        # Occasionally produce genuinely suspicious-but-not-attack authentication
        # traffic (privileged off-hours login, external-source login) so the
        # suspicious_authentication detector has real signal in normal traffic,
        # not only in attack scenarios.
        if rnd.random() < 0.03:
            username = rnd.choice(pool.privileged_users)
        if rnd.random() < 0.02:
            src = rnd.choice(pool.external_ips)

    return {
        "event_id": SecurityEvent.new_id(),
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "event_type": event_type,
        "action": action,
        "status": "success" if rnd.random() > 0.05 else "failure",
        "source_ip": src,
        "destination_ip": rnd.choice(pool.internal_ips + pool.external_ips),
        "source_port": rnd.randint(1024, 65535),
        "destination_port": rnd.choice([22, 80, 443, 3389, 445, 53, 8080, rnd.randint(1, 65535)]),
        "protocol": rnd.choice(["TCP", "UDP"]),
        "username": username,
        "hostname": rnd.choice(pool.hosts),
        "bytes": rnd.randint(0, 50_000),
        "country": rnd.choice(pool.countries),
        "metadata": {},
        "is_synthetic_attack": False,
        "scenario": "normal",
    }


def _scenario_events(scenario: str, pool: HostPool, ts: datetime) -> list[dict]:
    """Produce a burst of events representing one scenario (Section 52)."""
    rnd = pool.rnd
    events = []

    if scenario == "brute_force":
        ts = _snap_to_window_start(ts, WINDOW_SECONDS["brute_force"])
        src = rnd.choice(pool.external_ips)
        user = rnd.choice(pool.privileged_users + pool.users[:5])
        host = rnd.choice(pool.hosts)
        for i in range(rnd.randint(10, 40)):
            events.append({**_base_event(pool, ts + timedelta(seconds=i)),
                           "event_type": "authentication", "action": "login_failed",
                           "status": "failure", "source_ip": src, "username": user,
                           "hostname": host, "is_synthetic_attack": True,
                           "scenario": scenario})

    elif scenario == "password_spraying":
        ts = _snap_to_window_start(ts, WINDOW_SECONDS["password_spraying"])
        src = rnd.choice(pool.external_ips)
        targets = rnd.sample(pool.users, k=min(15, len(pool.users)))
        for i, user in enumerate(targets):
            events.append({**_base_event(pool, ts + timedelta(seconds=i * 2)),
                           "event_type": "authentication", "action": "login_failed",
                           "status": "failure", "source_ip": src, "username": user,
                           "is_synthetic_attack": True, "scenario": scenario})

    elif scenario == "port_scan":
        ts = _snap_to_window_start(ts, WINDOW_SECONDS["port_scan"])
        src = rnd.choice(pool.external_ips)
        dst = rnd.choice(pool.internal_ips)
        ports = rnd.sample(range(1, 65535), k=25)
        for i, port in enumerate(ports):
            events.append({**_base_event(pool, ts + timedelta(milliseconds=i * 100)),
                           "event_type": "network", "action": "connection",
                           "status": "failure", "source_ip": src, "destination_ip": dst,
                           "destination_port": port, "is_synthetic_attack": True,
                           "scenario": scenario})

    elif scenario == "privilege_escalation":
        host = rnd.choice(pool.hosts)
        user = rnd.choice(pool.users)
        events.append({**_base_event(pool, ts), "event_type": "privilege",
                       "action": "privilege_escalation", "status": "success",
                       "username": user, "hostname": host,
                       "is_synthetic_attack": True, "scenario": scenario})

    elif scenario == "multi_stage":
        # Snap to a brute-force window boundary so the 12-attempt failed-
        # login burst below can never straddle a 60s window edge (see
        # _snap_to_window_start's docstring for why this matters).
        ts = _snap_to_window_start(ts, WINDOW_SECONDS["brute_force"])
        src = rnd.choice(pool.external_ips)
        user = rnd.choice(pool.users)
        host = rnd.choice(pool.hosts)
        t = ts
        for i in range(12):
            events.append({**_base_event(pool, t), "event_type": "authentication",
                           "action": "login_failed", "status": "failure",
                           "source_ip": src, "username": user, "hostname": host,
                           "is_synthetic_attack": True, "scenario": scenario})
            t += timedelta(seconds=5)
        t += timedelta(seconds=5)
        events.append({**_base_event(pool, t), "event_type": "authentication",
                       "action": "login_success", "status": "success",
                       "source_ip": src, "username": user, "hostname": host,
                       "is_synthetic_attack": True, "scenario": scenario})
        t += timedelta(seconds=10)
        events.append({**_base_event(pool, t), "event_type": "privilege",
                       "action": "privilege_escalation", "status": "success",
                       "username": user, "hostname": host,
                       "is_synthetic_attack": True, "scenario": scenario})
        t += timedelta(seconds=10)
        events.append({**_base_event(pool, t), "event_type": "endpoint",
                       "action": "process_created", "status": "success",
                       "username": user, "hostname": host,
                       "metadata": {"suspicious": True, "process_name": "synthsvc.exe"},
                       "is_synthetic_attack": True, "scenario": scenario})
        t += timedelta(seconds=15)
        events.append({**_base_event(pool, t), "event_type": "network",
                       "action": "connection", "status": "success",
                       "username": user, "hostname": host,
                       "source_ip": rnd.choice(pool.internal_ips),
                       "destination_ip": rnd.choice(pool.external_ips),
                       "destination_internal": False,
                       "bytes": rnd.randint(60_000_000, 90_000_000),
                       "is_synthetic_attack": True, "scenario": scenario})

    elif scenario == "traffic_spike":
        host = rnd.choice(pool.hosts)
        for i in range(rnd.randint(200, 400)):
            events.append({**_base_event(pool, ts + timedelta(milliseconds=i * 10)),
                           "hostname": host, "is_synthetic_attack": True,
                           "scenario": scenario})

    elif scenario == "alert_storm":
        src = rnd.choice(pool.external_ips)
        for i in range(rnd.randint(500, 1000)):
            user = rnd.choice(pool.users)
            events.append({**_base_event(pool, ts + timedelta(milliseconds=i * 5)),
                           "event_type": "authentication", "action": "login_failed",
                           "status": "failure", "source_ip": src, "username": user,
                           "is_synthetic_attack": True, "scenario": scenario})

    return events


def generate(events: int, rate: int, n_hosts: int, n_users: int,
             scenario: str, attack_pct: float, seed: int = 42):
    """Yields normalized-ish raw event dicts at the requested logical rate."""
    pool = HostPool(n_hosts, n_users, seed=seed)
    produced = 0
    t = datetime.now(timezone.utc)
    interval = 1.0 / rate if rate > 0 else 0

    while produced < events:
        if scenario != "normal" and pool.rnd.random() < attack_pct:
            batch = _scenario_events(scenario, pool, t)
        else:
            batch = [_base_event(pool, t)]

        for e in batch:
            if produced >= events:
                break
            yield e
            produced += 1

        t += timedelta(seconds=max(interval, 0.0001))
        if rate > 0:
            time.sleep(interval * len(batch))


def main():
    p = argparse.ArgumentParser(description="CyberStream synthetic event generator")
    p.add_argument("--preset", choices=PRESETS.keys(), default=None)
    p.add_argument("--events", type=int, default=10_000)
    p.add_argument("--rate", type=int, default=1000, help="events/sec (0 = as fast as possible)")
    p.add_argument("--hosts", type=int, default=200)
    p.add_argument("--users", type=int, default=500)
    p.add_argument("--scenario", choices=SCENARIOS, default="normal")
    p.add_argument("--attack-pct", type=float, default=0.005)
    p.add_argument("--sink", choices=["stdout", "kafka", "jsonl"], default="stdout")
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "security-events"))
    p.add_argument("--brokers", default=os.environ.get("KAFKA_BROKERS", "localhost:9092"))
    p.add_argument("--out", default="/data/raw/events.jsonl")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    n_events = PRESETS[args.preset] if args.preset else args.events

    if args.sink == "kafka":
        from kafka import KafkaProducer  # imported lazily; not needed for stdout/jsonl
        producer = KafkaProducer(
            bootstrap_servers=args.brokers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            linger_ms=20, batch_size=64_000, compression_type="lz4",
        )
    elif args.sink == "jsonl":
        out_f = open(args.out, "w")

    count = 0
    start = time.time()
    for evt in generate(n_events, args.rate, args.hosts, args.users,
                         args.scenario, args.attack_pct, args.seed):
        # Partition key choice documented in docs/kafka.md — host balances
        # cardinality vs. detection locality better than source_ip.
        key = evt.get("hostname") or "unknown"

        if args.sink == "kafka":
            producer.send(args.topic, key=key, value=evt)
        elif args.sink == "jsonl":
            out_f.write(json.dumps(evt) + "\n")
        else:
            print(json.dumps(evt))

        count += 1
        if count % 10_000 == 0:
            elapsed = time.time() - start
            sys.stderr.write(
                f"[generator] {count}/{n_events} events "
                f"({count/elapsed:.1f} events/sec actual)\n"
            )

    if args.sink == "kafka":
        producer.flush()
    elif args.sink == "jsonl":
        out_f.close()

    elapsed = time.time() - start
    sys.stderr.write(
        f"[generator] DONE: {count} events in {elapsed:.2f}s "
        f"({count/max(elapsed,0.001):.1f} events/sec)\n"
    )


if __name__ == "__main__":
    main()
