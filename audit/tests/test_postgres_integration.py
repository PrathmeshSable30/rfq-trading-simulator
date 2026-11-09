"""Runs against the compose Postgres; skipped unless RFQ_PG_DSN is set."""

import json
import os
import random

import psycopg
import pytest

from audit_writer.main import SCHEMA_PATH, process_message

DSN = os.environ.get("RFQ_PG_DSN")
pytestmark = pytest.mark.skipif(DSN is None, reason="RFQ_PG_DSN not set")


@pytest.fixture()
def conn():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
        yield c


def msg(event) -> bytes:
    return json.dumps(event).encode()


def test_lifecycle_is_projected_and_redelivery_is_idempotent(conn):
    rfq = random.randint(10**9, 10**10)
    quote = rfq
    key = f"test-{rfq}:0"
    events = [
        {"type": "RFQ_CREATED", "ts": 1, "rfq_id": rfq, "client_id": "C", "instrument": "BOND",
         "side": "BUY", "qty": 1000, "expires_at": 99},
        {"type": "QUOTE_RECEIVED", "ts": 2, "rfq_id": rfq, "quote_id": quote, "dealer_id": "D1",
         "price": 100, "qty": 1000, "valid_until": 50},
        {"type": "RFQ_EXECUTED", "ts": 3, "rfq_id": rfq, "quote_id": quote, "dealer_id": "D1",
         "price": 100, "qty": 1000},
    ]

    for offset, e in enumerate(events):
        assert process_message(conn, key, offset, msg(e)) == "applied"
    assert process_message(conn, key, 2, msg(events[2])) == "duplicate"

    assert conn.execute("SELECT state FROM rfqs WHERE rfq_id = %s", (rfq,)).fetchone()[0] == "EXECUTED"
    assert conn.execute("SELECT status FROM quotes WHERE quote_id = %s", (quote,)).fetchone()[0] == "EXECUTED"
    assert conn.execute("SELECT count(*) FROM executions WHERE rfq_id = %s", (rfq,)).fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM rfq_events WHERE rfq_id = %s", (rfq,)).fetchone()[0] == 3


def test_poison_messages_go_to_dead_letter_table(conn):
    key = f"test-dead-{random.randint(10**9, 10**10)}:0"

    assert process_message(conn, key, 0, b"not json") == "dead"
    assert process_message(conn, key, 1, msg({"type": "NOPE", "ts": 1, "rfq_id": 1})) == "dead"
    # Foreign key violation: quote for an RFQ that was never created.
    orphan = {"type": "QUOTE_RECEIVED", "ts": 1, "rfq_id": 10**12 + 1, "quote_id": 10**12 + 1,
              "dealer_id": "D", "price": 1, "qty": 1, "valid_until": 2}
    assert process_message(conn, key, 2, msg(orphan)) == "dead"

    assert conn.execute("SELECT count(*) FROM rfq_events_dead WHERE partition_key = %s", (key,)).fetchone()[0] == 3
    assert conn.execute("SELECT count(*) FROM rfq_events WHERE partition_key = %s", (key,)).fetchone()[0] == 0
