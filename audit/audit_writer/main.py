"""Kafka -> PostgreSQL audit writer. At-least-once delivery, idempotent writes."""

import argparse
import json
import logging
import os
import pathlib
import signal
import sys
import time

import psycopg
from confluent_kafka import Consumer, KafkaError

from audit_writer.projection import INSERT_DEAD, apply_event

log = logging.getLogger("audit_writer")

DEFAULT_DSN = "postgresql://rfq:rfq_dev@localhost:5432/rfq_audit"  # local compose stack only
SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "schema.sql"


def process_message(conn: psycopg.Connection, partition_key: str, offset: int, raw: bytes) -> str:
    """Returns 'applied', 'duplicate' or 'dead'. Connection failures propagate."""
    text = raw.decode("utf-8", errors="replace")
    try:
        event = json.loads(text)
        with conn.transaction(), conn.cursor() as cur:
            return "applied" if apply_event(cur, partition_key, offset, event) else "duplicate"
    except psycopg.OperationalError:
        raise
    except (psycopg.Error, ValueError, KeyError, TypeError) as exc:
        log.warning("dead-lettering %s@%s: %s", partition_key, offset, exc)
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(INSERT_DEAD, (partition_key, offset, text, f"{type(exc).__name__}: {exc}"))
        return "dead"


def run(brokers: str, topic: str, group: str, dsn: str, init_schema: bool, max_messages: int,
        idle_exit: float = 0.0) -> dict:
    """Consumes until stopped, `max_messages` is reached, or the topic has been quiet for `idle_exit` s."""
    stop = False

    def request_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    counts = {"applied": 0, "duplicate": 0, "dead": 0}
    with psycopg.connect(dsn, autocommit=True) as conn:
        if init_schema:
            conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

        log.info("consuming %s from %s", topic, brokers)
        started = last_data = time.monotonic()
        while not stop:
            if max_messages and sum(counts.values()) >= max_messages:
                break
            if idle_exit:
                now = time.monotonic()
                total = sum(counts.values())
                if total and now - last_data > idle_exit:
                    break
                if not total and now - started > max(30.0, idle_exit * 6):
                    break

            messages = consumer.consume(num_messages=500, timeout=1.0)
            for msg in messages:
                if msg.error():
                    if msg.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        log.error("kafka error: %s", msg.error())
                    continue
                key = f"{msg.topic()}:{msg.partition()}"
                counts[process_message(conn, key, msg.offset(), msg.value())] += 1
                last_data = time.monotonic()

            # Offsets are committed only after the database writes above are durable.
            if messages:
                consumer.commit(asynchronous=False)

    consumer.close()
    log.info("stopped: %s", counts)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default=os.environ.get("RFQ_KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default="rfq.events")
    parser.add_argument("--group", default="rfq-audit-writer")
    parser.add_argument("--dsn", default=os.environ.get("RFQ_PG_DSN", DEFAULT_DSN))
    parser.add_argument("--no-init-schema", dest="init_schema", action="store_false")
    parser.add_argument("--max-messages", type=int, default=0, help="exit after N messages (0 = run forever)")
    parser.add_argument("--idle-exit", type=float, default=0.0,
                        help="exit once the topic has been quiet for this many seconds (0 = never)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    counts = run(args.brokers, args.topic, args.group, args.dsn, args.init_schema, args.max_messages,
                 args.idle_exit)
    return 1 if counts["dead"] else 0


if __name__ == "__main__":
    sys.exit(main())
