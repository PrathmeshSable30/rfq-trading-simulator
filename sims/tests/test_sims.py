import asyncio

import simplefix

from sims.dealer import Market, quote_fields
from sims.fix import FixSession, field
from sims.loadgen import Stats, markdown_report, percentile, report


class FakeWriter:
    def __init__(self):
        self.data = b""

    def write(self, b):
        self.data += b


def test_encoded_message_has_valid_framing_and_checksum():
    async def go():
        s = FixSession("h", 1, "CLIENT1")
        s._writer = FakeWriter()
        s.send("R", [(131, "REQ-1"), (146, 1), (55, "BOND"), (54, "1"), (38, 1000000)])
        return s._writer.data

    raw = asyncio.run(go())
    assert raw.startswith(b"8=FIX.4.4\x019=")
    assert b"\x0135=R\x01" in raw

    body, _, tail = raw.rpartition(b"\x0110=")
    checksum = sum(body + b"\x01") % 256
    assert int(tail.rstrip(b"\x01")) == checksum

    parser = simplefix.FixParser()
    parser.append_buffer(raw)
    msg = parser.get_message()
    assert field(msg, 131) == "REQ-1"
    assert field(msg, 49) == "CLIENT1" and field(msg, 56) == "GATEWAY"


def test_sequence_numbers_increment():
    async def go():
        s = FixSession("h", 1, "CLIENT1")
        s._writer = FakeWriter()
        s.send("0", [])
        s.send("0", [])
        return s._writer.data

    parser = simplefix.FixParser()
    parser.append_buffer(asyncio.run(go()))
    assert [field(parser.get_message(), 34) for _ in range(2)] == ["1", "2"]


def test_buyer_is_offered_above_mid_and_seller_bid_below():
    assert quote_fields("1", 100.0, 0.01, "5") == [(133, "100.010000"), (135, "5")]
    assert quote_fields("2", 100.0, 0.01, "5") == [(132, "99.990000"), (134, "5")]


def test_market_mid_is_stable_per_symbol():
    market = Market(["A"], seed=1)
    first = market.mid("A")
    assert abs(market.mid("A") - first) < 0.1


def test_markdown_report_contains_verdict_and_percentiles():
    stats = Stats(sent=4, executed=4)
    stats.total_ms = stats.first_quote_ms = stats.hit_to_exec_ms = [1.0, 2.0, 3.0, 4.0]
    md = markdown_report(report(stats, duration=2), "PASS")

    assert "**PASS**" in md
    assert "| rfq_to_exec |" in md
    assert "error rate 0.00%" in md


def test_percentile_and_report():
    assert percentile([], 0.5) != percentile([], 0.5)  # NaN
    assert percentile([1, 2, 3, 4], 0.99) == 4

    stats = Stats(sent=10, executed=9, no_quotes=1)
    stats.total_ms = [1.0, 2.0, 3.0]
    summary = report(stats, duration=3)
    assert summary["error_rate"] == 0.1
    assert summary["latency_ms"]["rfq_to_exec"]["max"] == 3.0
