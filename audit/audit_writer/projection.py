"""Maps RFQ events to SQL. Pure functions here; execution lives in apply_event."""

import json
from typing import Any

Statement = tuple[str, tuple[Any, ...]]

INSERT_EVENT = """
INSERT INTO rfq_events (partition_key, event_id, rfq_id, event_type, event_ts_ns, payload)
VALUES (%s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (partition_key, event_id) DO NOTHING
"""

INSERT_DEAD = """
INSERT INTO rfq_events_dead (partition_key, event_id, raw, error)
VALUES (%s, %s, %s, %s)
ON CONFLICT (partition_key, event_id) DO NOTHING
"""


def statements_for(event: dict) -> list[Statement]:
    """Projection updates for one event. Raises KeyError/ValueError on bad input."""
    kind = event["type"]
    ts = event["ts"]
    rfq_id = event["rfq_id"]

    if kind == "RFQ_CREATED":
        return [(
            "INSERT INTO rfqs (rfq_id, client_id, instrument, side, qty, state, created_ns, expires_ns) "
            "VALUES (%s, %s, %s, %s, %s, 'OPEN', %s, %s) ON CONFLICT (rfq_id) DO NOTHING",
            (rfq_id, event["client_id"], event["instrument"], event["side"], event["qty"],
             ts, event["expires_at"]),
        )]

    if kind == "QUOTE_RECEIVED":
        return [(
            "INSERT INTO quotes (quote_id, rfq_id, dealer_id, price, qty, status, received_ns, valid_until_ns) "
            "VALUES (%s, %s, %s, %s, %s, 'ACTIVE', %s, %s) ON CONFLICT (quote_id) DO NOTHING",
            (event["quote_id"], rfq_id, event["dealer_id"], event["price"], event["qty"],
             ts, event["valid_until"]),
        )]

    if kind == "QUOTE_WITHDRAWN":
        return [("UPDATE quotes SET status = 'WITHDRAWN' WHERE quote_id = %s", (event["quote_id"],))]

    if kind == "RFQ_EXECUTED":
        return [
            ("UPDATE quotes SET status = 'EXECUTED' WHERE quote_id = %s", (event["quote_id"],)),
            (
                "INSERT INTO executions (rfq_id, quote_id, dealer_id, price, qty, executed_ns) "
                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (rfq_id) DO NOTHING",
                (rfq_id, event["quote_id"], event["dealer_id"], event["price"], event["qty"], ts),
            ),
            ("UPDATE rfqs SET state = 'EXECUTED' WHERE rfq_id = %s", (rfq_id,)),
        ]

    if kind == "RFQ_EXPIRED":
        return [("UPDATE rfqs SET state = 'EXPIRED' WHERE rfq_id = %s", (rfq_id,))]

    if kind == "RFQ_CANCELLED":
        return [("UPDATE rfqs SET state = 'CANCELLED' WHERE rfq_id = %s", (rfq_id,))]

    raise ValueError(f"unknown event type {kind!r}")


def apply_event(cur, partition_key: str, offset: int, event: dict) -> bool:
    """Stores the event and updates projections. Returns False for a redelivered event."""
    statements = statements_for(event)  # validate before touching the database

    cur.execute(INSERT_EVENT, (partition_key, offset, event["rfq_id"], event["type"],
                               event["ts"], json.dumps(event)))
    if cur.rowcount == 0:
        return False

    for sql, params in statements:
        cur.execute(sql, params)
    return True
