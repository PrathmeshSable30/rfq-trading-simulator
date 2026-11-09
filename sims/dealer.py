"""Dealer simulator: answers QuoteRequests with randomised, market-like quotes."""

import asyncio
import logging
import random
from dataclasses import dataclass, field as dc_field

from sims.fix import FixSession, FixSessionClosed, field

log = logging.getLogger("sims.dealer")

SYMBOLS = ["US912828ZT58", "US91282CJL63", "DE0001102580", "GB00BM8Z2S21"]


class Market:
    """Shared mid prices following a small random walk, so dealers quote around the same level."""

    def __init__(self, symbols=SYMBOLS, seed: int | None = None):
        self._rng = random.Random(seed)
        self._mid = {s: 95.0 + self._rng.random() * 10 for s in symbols}

    def mid(self, symbol: str) -> float:
        self._mid[symbol] += self._rng.gauss(0, 0.002)
        return self._mid[symbol]


def quote_fields(side: str, mid: float, half_spread: float, qty: str):
    """Client buying (side 1) is offered a price above mid, a seller is bid below it."""
    if side == "1":
        return [(133, f"{mid + half_spread:.6f}"), (135, qty)]
    return [(132, f"{mid - half_spread:.6f}"), (134, qty)]


@dataclass
class DealerStats:
    requests: int = 0
    quotes: int = 0
    skipped: int = 0
    fills: int = 0


@dataclass
class Dealer:
    name: str
    host: str
    port: int
    market: Market
    delay_ms: tuple[float, float] = (0.5, 3.0)
    half_spread_bps: tuple[float, float] = (0.5, 3.0)
    no_quote_rate: float = 0.0
    seed: int | None = None
    stats: DealerStats = dc_field(default_factory=DealerStats)

    def __post_init__(self):
        self.session = FixSession(self.host, self.port, self.name)
        self._rng = random.Random(self.seed)
        self._quote_seq = 0

    async def connect(self) -> None:
        await self.session.connect()

    async def run(self) -> None:
        tasks: set[asyncio.Task] = set()
        try:
            while True:
                msg = await self.session.recv()
                kind = msg.get(35)
                if kind == b"R":
                    self.stats.requests += 1
                    task = asyncio.create_task(self._answer(msg))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                elif kind == b"8":
                    self.stats.fills += 1
        except FixSessionClosed:
            pass
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            raise

    async def _answer(self, req) -> None:
        if self._rng.random() < self.no_quote_rate:
            self.stats.skipped += 1
            return

        await asyncio.sleep(self._rng.uniform(*self.delay_ms) / 1000)
        symbol, side, qty = field(req, 55), field(req, 54), field(req, 38)
        mid = self.market.mid(symbol)
        half_spread = mid * self._rng.uniform(*self.half_spread_bps) / 10_000

        self._quote_seq += 1
        fields = [(117, f"{self.name}-{self._quote_seq}"), (131, field(req, 131)), (55, symbol)]
        fields += quote_fields(side, mid, half_spread, qty)
        self.session.send("S", fields)
        self.stats.quotes += 1

    async def close(self) -> None:
        await self.session.close()


async def main_async(args) -> None:
    market = Market(seed=args.seed)
    dealer = Dealer(args.name, args.host, args.port, market, no_quote_rate=args.no_quote_rate)
    await dealer.connect()
    log.info("%s logged on", args.name)
    try:
        await dealer.run()
    finally:
        await dealer.close()
        log.info("%s stats: %s", args.name, dealer.stats)


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", default="DEALER1")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--no-quote-rate", type=float, default=0.0)
    p.add_argument("--seed", type=int)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main_async(p.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
