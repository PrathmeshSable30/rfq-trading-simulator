import pytest

from audit_writer.projection import statements_for


def sql_of(event):
    return [s for s, _ in statements_for(event)]


def test_rfq_created_inserts_open_rfq():
    (sql, params), = statements_for({
        "type": "RFQ_CREATED", "ts": 10, "rfq_id": 1, "client_id": "C", "instrument": "BOND",
        "side": "BUY", "qty": 1000, "expires_at": 99,
    })
    assert sql.startswith("INSERT INTO rfqs")
    assert params == (1, "C", "BOND", "BUY", 1000, 10, 99)


def test_quote_received_inserts_active_quote():
    (sql, params), = statements_for({
        "type": "QUOTE_RECEIVED", "ts": 11, "rfq_id": 1, "quote_id": 5, "dealer_id": "D1",
        "price": 100, "qty": 1000, "valid_until": 50,
    })
    assert sql.startswith("INSERT INTO quotes")
    assert params == (5, 1, "D1", 100, 1000, 11, 50)


def test_execution_updates_quote_execution_and_rfq():
    sqls = sql_of({
        "type": "RFQ_EXECUTED", "ts": 12, "rfq_id": 1, "quote_id": 5, "dealer_id": "D1",
        "price": 100, "qty": 1000,
    })
    assert [s.split()[0] for s in sqls] == ["UPDATE", "INSERT", "UPDATE"]
    assert "executions" in sqls[1]


@pytest.mark.parametrize("kind,state", [("RFQ_EXPIRED", "EXPIRED"), ("RFQ_CANCELLED", "CANCELLED")])
def test_terminal_events_set_rfq_state(kind, state):
    (sql, params), = statements_for({"type": kind, "ts": 1, "rfq_id": 7})
    assert state in sql
    assert params == (7,)


def test_withdrawn_marks_quote():
    (sql, params), = statements_for({"type": "QUOTE_WITHDRAWN", "ts": 1, "rfq_id": 1, "quote_id": 9})
    assert "WITHDRAWN" in sql
    assert params == (9,)


def test_unknown_type_is_rejected():
    with pytest.raises(ValueError):
        statements_for({"type": "NOPE", "ts": 1, "rfq_id": 1})


def test_missing_field_is_rejected():
    with pytest.raises(KeyError):
        statements_for({"type": "RFQ_CREATED", "ts": 1, "rfq_id": 1})
