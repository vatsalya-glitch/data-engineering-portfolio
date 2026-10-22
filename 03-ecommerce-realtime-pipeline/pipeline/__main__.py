"""
Run ONE piece of the pipeline per terminal - the way it would run in production
(each service its own process / container).

  python -m pipeline setup                  # create topics + subscriptions
  python -m pipeline producer [--rate 25]   # start shopping traffic
  python -m pipeline validator
  python -m pipeline aggregator
  python -m pipeline alerts
  python -m pipeline sink
  python -m pipeline stats                  # backlog per subscription (local broker)
  python -m pipeline report                 # what landed in DuckDB

Stop any service with Ctrl+C - it finishes its current batch before exiting.
Prefer a single command? Use run_pipeline.py instead.
"""

import argparse
import sys

from pipeline.broker import broker_type, get_broker, setup_topology
from pipeline.config import load_pipeline_config
from pipeline.report import print_report
from pipeline.services import CONSUMERS, build_service
from pipeline.services.base import configure_logging, run_service, stop_on_signals
from pipeline.services.producer import run_producer


def print_stats(broker) -> None:
    if not hasattr(broker, "stats"):
        print("stats are only available for the local broker; "
              "use the Cloud Console / gcloud for Pub/Sub metrics")
        return
    print(f"{'subscription':<18} {'topic':<15} {'backlog':>8} {'redelivered':>12} {'oldest_age_s':>13}")
    for row in broker.stats():
        print(f"{row['subscription']:<18} {row['topic']:<15} {row['backlog']:>8} "
              f"{row['redelivered']:>12} {row['oldest_unacked_age_s']:>13}")
    print("\nmessages published per topic:", broker.topic_counts())


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["setup", "producer", "stats", "report", *CONSUMERS])
    parser.add_argument("--broker", choices=["local", "pubsub"], help="override config/BROKER")
    parser.add_argument("--rate", type=float, help="producer: events per second")
    parser.add_argument("--max-events", type=int, help="producer: stop after N events")
    args = parser.parse_args(argv)

    configure_logging()
    if args.command == "report":
        print_report()
        return 0

    broker = get_broker(args.broker)
    try:
        if args.command == "setup":
            setup_topology(broker)
            print(f"topology created on the {broker_type(args.broker)} broker")
        elif args.command == "stats":
            print_stats(broker)
        elif args.command == "producer":
            producer_cfg = dict(load_pipeline_config()["producer"])
            if args.rate:
                producer_cfg["events_per_second"] = args.rate
            run_producer(broker, stop_on_signals(), max_events=args.max_events, **producer_cfg)
        else:
            setup_topology(broker)   # idempotent - safe to call from every service
            run_service(build_service(args.command, broker), stop_on_signals())
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
