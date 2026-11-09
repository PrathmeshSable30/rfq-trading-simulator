"""Engine -> Kafka -> audit writer -> Postgres. Needs the compose stack and the built rfq_demo."""

import os
import subprocess
import uuid

import psycopg
import pytest

from audit_writer.main import run

DEMO = os.environ.get("RFQ_DEMO_EXE")
BROKERS = os.environ.get("RFQ_KAFKA_BROKERS")
DSN = os.environ.get("RFQ_PG_DSN")
pytestmark = pytest.mark.skipif(not (DEMO and BROKERS and DSN),
                                reason="RFQ_DEMO_EXE, RFQ_KAFKA_BROKERS and RFQ_PG_DSN must be set")

RFQS = 200


def test_published_events_reach_postgres_exactly_once():
    topic = f"rfq.test.{uuid.uuid4().hex[:8]}"  # isolates the run from earlier traffic
    subprocess.run([DEMO, BROKERS, "none", str(RFQS), topic], check=True, timeout=60)

    counts = run(BROKERS, topic, f"pipeline-test-{topic}", DSN, init_schema=True, max_messages=0,
                 idle_exit=5.0)

    # rfq_demo per RFQ: 1 created + 3 quotes + 1 terminal event, plus a withdrawal for every 3rd RFQ.
    expected = RFQS * 5 + RFQS // 3
    assert counts == {"applied": expected, "duplicate": 0, "dead": 0}

    with psycopg.connect(DSN) as conn:
        def events(kind: str) -> int:
            return conn.execute(
                "SELECT count(*) FROM rfq_events WHERE partition_key LIKE %s AND event_type = %s",
                (f"{topic}:%", kind)).fetchone()[0]

        # rfq_demo cancels every 10th RFQ, leaves every other 5th to expire, and executes the rest.
        assert events("RFQ_CREATED") == RFQS
        assert events("RFQ_EXECUTED") == RFQS - RFQS // 5
        assert events("RFQ_CANCELLED") == RFQS // 10
        assert events("RFQ_EXPIRED") == RFQS // 5 - RFQS // 10

        executions = conn.execute(
            "SELECT count(*) FROM executions e WHERE e.rfq_id IN "
            "(SELECT rfq_id FROM rfq_events WHERE partition_key LIKE %s AND event_type = 'RFQ_EXECUTED')",
            (f"{topic}:%",)).fetchone()[0]
        assert executions == RFQS - RFQS // 5
