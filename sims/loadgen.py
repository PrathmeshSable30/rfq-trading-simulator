"""Open-loop RFQ load generator: clients request quotes, hit the best one, and report latencies."""

import argparse
import asyncio
import json
import logging
import random
import sys
import time
from dataclasses import dataclass, field as dc_field

from sims.dealer import SYMBOLS, Dealer, Market
from sims.fix import FixSession, FixSessionClosed, field, utc_timestamp

log = logging.getLogger("sims.loadgen")


@dataclass
class Stats:
    sent: int = 0
    executed: int = 0
    no_quotes: int = 0
    exec_timeouts: int = 0
    rejected: int = 0
    other: int = 0
    first_quote_ms: list = dc_field(default_factory=list)
    hit_to_exec_ms: list = dc_field(default_factory=list)
    total_ms: list = dc_field(default_factory=list)

    @property
    def errors(self) -> int:
        return self.no_quotes + self.exec_timeouts + self.rejected + self.other


def percentile(values: list, p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


class Client:
    def __init__(self, name: str, host: str, port: int, dealers: int, window_s: float, stats: Stats):
        self.name = name
        self.session = FixSession(host, port, name)
        self.dealers = dealers
        self.window_s = window_s
        self.stats = stats
        self._quotes: dict[str, asyncio.Queue] = {}
        self._fills: dict[str, asyncio.Future] = {}

    async def connect(self) -> None:
        await self.session.connect()

    async def _reader(self) -> None:
        try:
            while True:
                msg = await self.session.recv()
                kind = msg.get(35)
                if kind == b"S":
                    q = self._quotes.get(field(msg, 131))
                    if q is not None:
                        q.put_nowait((time.perf_counter(), msg))
                elif kind == b"8":
                    fut = self._fills.get(field(msg, 11))
                    if fut is not None and not fut.done():
                        fut.set_result((time.perf_counter(), msg))
                elif kind == b"j":
                    self.stats.rejected += 1
        except FixSessionClosed:
            pass

    async def run(self, rate: float, duration: float) -> None:
        reader = asyncio.create_task(self._reader())
        loop = asyncio.get_running_loop()
        end = loop.time() + duration
        next_at = loop.time()
        inflight: set[asyncio.Task] = set()
        i = 0
        while loop.time() < end:
            i += 1
            task = asyncio.create_task(self._rfq(f"{self.name}-{i}"))
            inflight.add(task)
            task.add_done_callback(inflight.discard)
            next_at += 1.0 / rate
            await asyncio.sleep(max(0.0, next_at - loop.time()))

        if inflight:
            await asyncio.wait(inflight, timeout=5.0)
        reader.cancel()

    async def _rfq(self, req_id: str) -> None:
        stats = self.stats
        side = random.choice("12")
        symbol = random.choice(SYMBOLS)
        qty = random.choice([1_000_000, 5_000_000, 10_000_000])
        quotes: asyncio.Queue = asyncio.Queue()
        fill: asyncio.Future = asyncio.get_running_loop().create_future()
        self._quotes[req_id], self._fills[req_id] = quotes, fill

        try:
            stats.sent += 1
            t0 = time.perf_counter()
            self.session.send("R", [(131, req_id), (146, 1), (55, symbol), (54, side), (38, qty),
                                    (126, utc_timestamp(10))])

            received = []
            deadline = t0 + self.window_s
            while len(received) < self.dealers:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                try:
                    received.append(await asyncio.wait_for(quotes.get(), remaining))
                except asyncio.TimeoutError:
                    break

            if not received:
                stats.no_quotes += 1
                self.session.send("Z", [(131, req_id), (117, "*"), (298, 4)])
                return
            stats.first_quote_ms.append((received[0][0] - t0) * 1000)

            def price(item):
                msg = item[1]
                return float(field(msg, 133) or field(msg, 132))

            best = (min if side == "1" else max)(received, key=price)[1]
            t1 = time.perf_counter()
            self.session.send("AJ", [(693, f"{req_id}-R"), (694, 1), (117, field(best, 117)),
                                     (55, symbol), (54, side)])
            try:
                t2, report = await asyncio.wait_for(fill, 3.0)
            except asyncio.TimeoutError:
                stats.exec_timeouts += 1
                return

            if field(report, 150) == "F":
                stats.executed += 1
                stats.hit_to_exec_ms.append((t2 - t1) * 1000)
                stats.total_ms.append((t2 - t0) * 1000)
            else:
                stats.other += 1
        finally:
            self._quotes.pop(req_id, None)
            self._fills.pop(req_id, None)

    async def close(self) -> None:
        await self.session.close()


def report(stats: Stats, duration: float) -> dict:
    rows = {}
    for name, values in [("first_quote", stats.first_quote_ms), ("hit_to_exec", stats.hit_to_exec_ms),
                         ("rfq_to_exec", stats.total_ms)]:
        rows[name] = {label: round(percentile(values, p), 3)
                      for label, p in (("p50", 0.5), ("p90", 0.9), ("p99", 0.99), ("p99.9", 0.999))}
        rows[name]["max"] = round(max(values), 3) if values else float("nan")

    summary = {
        "sent": stats.sent, "executed": stats.executed, "errors": stats.errors,
        "error_rate": round(stats.errors / stats.sent, 5) if stats.sent else 0.0,
        "throughput_rfq_per_s": round(stats.executed / duration, 1),
        "no_quotes": stats.no_quotes, "exec_timeouts": stats.exec_timeouts,
        "rejected": stats.rejected, "other": stats.other,
        "latency_ms": rows,
    }
    return summary


def print_report(summary: dict) -> None:
    print(f"\nsent={summary['sent']} executed={summary['executed']} errors={summary['errors']} "
          f"(error rate {summary['error_rate']:.2%}) throughput={summary['throughput_rfq_per_s']}/s")
    print(f"{'latency (ms)':<14}{'p50':>9}{'p90':>9}{'p99':>9}{'p99.9':>9}{'max':>9}")
    for name, r in summary["latency_ms"].items():
        print(f"{name:<14}{r['p50']:>9}{r['p90']:>9}{r['p99']:>9}{r['p99.9']:>9}{r['max']:>9}")


def markdown_report(summary: dict, verdict: str) -> str:
    lines = [
        "### RFQ load gate",
        "",
        f"**{verdict}** - sent {summary['sent']}, executed {summary['executed']}, "
        f"error rate {summary['error_rate']:.2%}, throughput {summary['throughput_rfq_per_s']}/s",
        "",
        "| latency (ms) | p50 | p90 | p99 | p99.9 | max |",
        "|---|---|---|---|---|---|",
    ]
    for name, r in summary["latency_ms"].items():
        lines.append(f"| {name} | {r['p50']} | {r['p90']} | {r['p99']} | {r['p99.9']} | {r['max']} |")
    return "\n".join(lines) + "\n"


async def main_async(args) -> int:
    stats = Stats()
    market = Market(seed=args.seed)

    dealers = [Dealer(f"DEALER{n}", args.host, args.port, market, no_quote_rate=args.no_quote_rate,
                      seed=n) for n in range(1, args.dealers + 1)]
    clients = [Client(f"CLIENT{n}", args.host, args.port, args.dealers, args.window_ms / 1000, stats)
               for n in range(1, args.clients + 1)]

    await asyncio.gather(*(d.connect() for d in dealers))
    await asyncio.gather(*(c.connect() for c in clients))
    log.info("connected %d dealers and %d clients; running %.0fs at %.0f RFQ/s per client",
             len(dealers), len(clients), args.duration, args.rate)

    dealer_tasks = [asyncio.create_task(d.run()) for d in dealers]
    await asyncio.gather(*(c.run(args.rate, args.duration) for c in clients))

    for c in clients:
        await c.close()
    for d in dealers:
        await d.close()
    for t in dealer_tasks:
        t.cancel()

    summary = report(stats, args.duration)
    print_report(summary)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

    p99 = summary["latency_ms"]["rfq_to_exec"]["p99"]
    failure = None
    if summary["error_rate"] > args.max_error_rate:
        failure = f"error rate {summary['error_rate']:.2%} > {args.max_error_rate:.2%}"
    elif args.max_p99_ms and not p99 <= args.max_p99_ms:
        failure = f"rfq_to_exec p99 {p99} ms > {args.max_p99_ms} ms"

    if getattr(args, "summary_md", None):
        verdict = "PASS" if failure is None else f"FAIL ({failure})"
        with open(args.summary_md, "w", encoding="utf-8") as fh:
            fh.write(markdown_report(summary, verdict))

    if failure:
        print(f"FAIL: {failure}")
        return 1
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--dealers", type=int, default=3)
    p.add_argument("--clients", type=int, default=2)
    p.add_argument("--rate", type=float, default=50, help="RFQs per second per client")
    p.add_argument("--duration", type=float, default=10, help="seconds")
    p.add_argument("--window-ms", type=float, default=100, help="how long a client waits for quotes")
    p.add_argument("--no-quote-rate", type=float, default=0.0, help="share of requests a dealer ignores")
    p.add_argument("--seed", type=int)
    p.add_argument("--json", help="write the summary to this file")
    p.add_argument("--summary-md", help="write a markdown summary (for CI job summaries) to this file")
    p.add_argument("--max-error-rate", type=float, default=0.01)
    p.add_argument("--max-p99-ms", type=float, default=0.0, help="fail when rfq_to_exec p99 exceeds this")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
