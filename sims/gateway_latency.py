"""Measures how much each FIX-layer setting costs, using the gateway's own latency histogram.

Runs the same load against several gateway variants and prints the per-message processing time
(p50/p99 interpolated from histogram buckets, mean exact). Needs a built rfq_gateway.
"""

import argparse
import asyncio
import re
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

from sims import loadgen

PORT, METRICS_PORT = 5001, 9100

# name -> settings that override the generated config ([DEFAULT] section)
VARIANTS = {
    "default": [],
    "file-store": ["StoreType=file"],
    "no-nodelay": ["SocketNodelay=N"],
    "no-log": ["LogType=none"],
    "file-store+no-log": ["StoreType=file", "LogType=none"],
}

BUCKET = re.compile(r'^rfq_message_processing_seconds_bucket\{msg_type="([^"]+)",le="([^"]+)"\} (\S+)$')
TOTAL = re.compile(r'^rfq_message_processing_seconds_(sum|count)\{msg_type="([^"]+)"\} (\S+)$')


def variant_config(base: Path, overrides: list[str], directory: Path, name: str) -> Path:
    text = base.read_text(encoding="utf-8")
    for setting in overrides:
        key = setting.split("=", 1)[0]
        # Replace an existing key in place, otherwise add it to [DEFAULT].
        if re.search(rf"^{key}=", text, flags=re.M):
            text = re.sub(rf"^{key}=.*$", setting, text, count=1, flags=re.M)
        else:
            text = text.replace("[DEFAULT]", "[DEFAULT]\n" + setting, 1)
    # Separate store/log folders keep variants from sharing sequence-number files.
    text = re.sub(r"(FileStorePath|FileLogPath)=(.*)", lambda m: f"{m.group(1)}={m.group(2)}-{name}", text)
    path = directory / f"gateway-{name}.cfg"
    path.write_text(text, encoding="utf-8")
    return path


def scrape(port: int = METRICS_PORT) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
        return resp.read().decode()


def quantile(buckets: list[tuple[float, float]], q: float) -> float:
    """Prometheus-style histogram_quantile over cumulative (upper_bound, count) pairs."""
    total = buckets[-1][1]
    if total == 0:
        return float("nan")
    rank, prev_bound, prev_count = q * total, 0.0, 0.0
    for bound, count in buckets:
        if count >= rank:
            if bound == float("inf"):
                return prev_bound
            span = count - prev_count
            return prev_bound + (bound - prev_bound) * ((rank - prev_count) / span if span else 0)
        prev_bound, prev_count = bound, count
    return prev_bound


def summarise(metrics_text: str) -> dict:
    """Aggregates all FIX message types into one histogram, in microseconds."""
    cumulative: dict[float, float] = {}
    total_sum = total_count = 0.0
    for line in metrics_text.splitlines():
        if m := BUCKET.match(line):
            bound = float("inf") if m.group(2) == "+Inf" else float(m.group(2))
            cumulative[bound] = cumulative.get(bound, 0.0) + float(m.group(3))
        elif m := TOTAL.match(line):
            if m.group(1) == "sum":
                total_sum += float(m.group(3))
            else:
                total_count += float(m.group(3))
    buckets = sorted(cumulative.items())
    return {
        "messages": int(total_count),
        "mean_us": total_sum / total_count * 1e6 if total_count else float("nan"),
        "p50_us": quantile(buckets, 0.50) * 1e6,
        "p99_us": quantile(buckets, 0.99) * 1e6,
    }


def wait_for_port(timeout: float = 20.0) -> None:
    import socket
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("gateway did not start")


def load_args(rate: float, duration: float, **overrides):
    base = dict(host="127.0.0.1", port=PORT, dealers=4, clients=2, rate=rate, duration=duration,
                window_ms=100, no_quote_rate=0.0, seed=1, json=None, summary_md=None,
                max_error_rate=1.0, max_p99_ms=0.0)
    base.update(overrides)
    return argparse.Namespace(**base)


def measure(exe: Path, config: Path, rate: float, duration: float) -> dict:
    proc = subprocess.Popen([str(exe), str(config), "none", "none", str(METRICS_PORT)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_port()
        asyncio.run(loadgen.main_async(load_args(5, 2)))  # warm-up
        before = scrape()
        asyncio.run(loadgen.main_async(load_args(rate, duration)))
        after = summarise(scrape())
        warm = summarise(before)
        # Subtract the warm-up so only the measured run is reported.
        measured_msgs = after["messages"] - warm["messages"]
        after["messages"] = measured_msgs
        return after
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--exe", required=True, type=Path, help="path to rfq_gateway")
    p.add_argument("--config", required=True, type=Path, help="path to the generated gateway.cfg")
    p.add_argument("--rate", type=float, default=100, help="RFQs/s per client")
    p.add_argument("--duration", type=float, default=15)
    p.add_argument("--repeat", type=int, default=2)
    p.add_argument("--only", nargs="*", help="variant names to run")
    args = p.parse_args()

    names = args.only or list(VARIANTS)
    rows = []
    with tempfile.TemporaryDirectory(dir=args.config.parent) as tmp:
        for round_no in range(1, args.repeat + 1):
            for name in names:
                cfg = variant_config(args.config, VARIANTS[name], Path(tmp), f"{name}-{round_no}")
                result = measure(args.exe, cfg, args.rate, args.duration)
                rows.append((name, round_no, result))
                print(f"  done {name} (run {round_no})", flush=True)

    print(f"\n{'variant':<14}{'run':>4}{'messages':>10}{'mean us':>10}{'p50 us':>9}{'p99 us':>9}")
    for name, round_no, r in rows:
        print(f"{name:<14}{round_no:>4}{r['messages']:>10}{r['mean_us']:>10.0f}{r['p50_us']:>9.0f}{r['p99_us']:>9.0f}")


if __name__ == "__main__":
    main()
