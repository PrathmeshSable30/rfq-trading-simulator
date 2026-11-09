"""Starts the real C++ gateway and drives it over FIX; skipped unless RFQ_GATEWAY_EXE/CFG are set."""

import argparse
import asyncio
import os
import socket
import subprocess
import time

import pytest

from sims import loadgen

EXE = os.environ.get("RFQ_GATEWAY_EXE")
CFG = os.environ.get("RFQ_GATEWAY_CFG")
pytestmark = pytest.mark.skipif(not (EXE and CFG), reason="RFQ_GATEWAY_EXE / RFQ_GATEWAY_CFG not set")

PORT = 5001


def wait_for_port(timeout: float = 10.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("gateway did not start listening")


def args(**overrides):
    base = dict(host="127.0.0.1", port=PORT, dealers=3, clients=2, rate=25, duration=3, window_ms=100,
                no_quote_rate=0.0, seed=1, json=None, max_error_rate=0.0, max_p99_ms=250.0)
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.fixture(scope="module")
def gateway():
    proc = subprocess.Popen([EXE, CFG, "none", "none"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_port()
        # The first connections pay one-off costs (session stores, caches); keep them out of the assertions.
        asyncio.run(loadgen.main_async(args(rate=5, duration=1, max_error_rate=1.0, max_p99_ms=0.0)))
        yield proc
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_every_rfq_executes_end_to_end(gateway):
    assert asyncio.run(loadgen.main_async(args())) == 0


def test_gate_fails_when_the_latency_budget_is_impossible(gateway):
    assert asyncio.run(loadgen.main_async(args(duration=2, max_p99_ms=0.001))) == 1


def test_rfqs_still_execute_when_dealers_ignore_requests(gateway):
    # With 3 dealers each ignoring 30% of requests, at least one quote almost always arrives.
    assert asyncio.run(loadgen.main_async(args(no_quote_rate=0.3, max_error_rate=0.05))) == 0
