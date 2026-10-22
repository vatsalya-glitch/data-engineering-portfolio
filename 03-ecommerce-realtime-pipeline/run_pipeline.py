"""
E-COMMERCE REAL-TIME PIPELINE - ONE-COMMAND DEMO

Runs every service as a thread in one process, streams traffic for a while,
shuts down gracefully in dependency order, and prints a report.

  python run_pipeline.py                     # 60s on the local broker
  python run_pipeline.py --duration 120 --rate 50
  BROKER=pubsub python run_pipeline.py       # against the Pub/Sub emulator

Want to see each service on its own? Run them in separate terminals instead:
  python -m pipeline --help
"""

import argparse
import shutil
import sys
import threading
import time

from pipeline.broker import broker_type, get_broker, setup_topology
from pipeline.config import PROJECT_ROOT, load_pipeline_config
from pipeline.report import print_report
from pipeline.services import build_service
from pipeline.services.base import configure_logging, run_service, stop_on_signals
from pipeline.services.producer import run_producer


def reset_local_state(cfg) -> None:
    """Start from a clean slate so the demo is reproducible."""
    for rel in (cfg["broker"]["local"]["path"], cfg["sink"]["database"]):
        path = PROJECT_ROOT / rel
        for candidate in path.parent.glob(path.name + "*"):   # includes -wal/-shm files
            candidate.unlink()
    shutil.rmtree(PROJECT_ROOT / "state", ignore_errors=True)


def wait_until_drained(broker, service, quiet_seconds=2.0, timeout=60.0) -> None:
    """Wait until a service has nothing left to do.

    'Drained' = its subscriptions have no backlog (when the broker can tell us)
    and it has not made progress for quiet_seconds.
    """
    def progress():
        return service.processed + service.failed + getattr(service, "buffered", 0)

    deadline = time.monotonic() + timeout
    last, last_change = progress(), time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(0.25)
        if progress() != last:
            last, last_change = progress(), time.monotonic()
            continue
        backlog = 0
        if hasattr(broker, "stats"):
            backlog = sum(r["backlog"] for r in broker.stats()
                          if r["subscription"] in service.subscriptions)
        if backlog == 0 and time.monotonic() - last_change >= quiet_seconds:
            return
    service.log.warning("did not drain within %.0fs - stopping anyway", timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duration", type=float, default=60, help="seconds of traffic (default 60)")
    parser.add_argument("--rate", type=float, help="events per second (default from config)")
    parser.add_argument("--broker", choices=["local", "pubsub"], help="override config/BROKER")
    parser.add_argument("--keep-state", action="store_true", help="don't wipe broker + database first")
    args = parser.parse_args()

    configure_logging()
    cfg = load_pipeline_config()
    kind = broker_type(args.broker)

    print("=" * 70)
    print("E-COMMERCE REAL-TIME PIPELINE")
    print("=" * 70)
    print(f"broker: {kind}   traffic: {args.duration:.0f}s   database: {cfg['sink']['database']}\n")

    if kind == "local" and not args.keep_state:
        reset_local_state(cfg)

    broker = get_broker(args.broker)
    setup_topology(broker)

    # Start consumers BEFORE the producer: subscribers only receive messages
    # published after they exist, and we want them warmed up.
    services, stops, threads = {}, {}, {}
    for name in ("sink", "aggregator", "alerts", "validator"):
        services[name] = build_service(name, broker)
        stops[name] = threading.Event()
        threads[name] = threading.Thread(target=run_service, args=(services[name], stops[name]),
                                         name=name, daemon=True)
        threads[name].start()

    producer_cfg = dict(cfg["producer"])
    if args.rate:
        producer_cfg["events_per_second"] = args.rate
    interrupted = stop_on_signals()
    stats = run_producer(broker, interrupted, duration_seconds=args.duration, **producer_cfg)

    # Graceful shutdown in topological order: upstream first, so every
    # downstream service still runs while its inputs drain.
    print("\nTraffic stopped - draining the pipeline (upstream -> downstream) ...")
    for stage in (["validator"], ["aggregator", "alerts"], ["sink"]):
        for name in stage:
            wait_until_drained(broker, services[name])
        for name in stage:
            stops[name].set()
            threads[name].join()

    broker.close()

    print("\n" + "=" * 70)
    print("PRODUCER")
    print("=" * 70)
    for key, value in stats.items():
        print(f"  {key:<14} {value}")
    print_report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
