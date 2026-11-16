"""Throughput, latency and server CPU per request of http_idle_cost arms under wrk, interleaved.

Each round runs every arm (order alternating by round) through every row: a transport, a path,
a connection count and keep-alive or close. A row's server CPU per request is the server's own
rusage across the timed run divided by wrk's request count. The summary gives, per row and arm,
the median over rounds and the median paired ratio against the first arm with its range; the
"aa" arm (a copy of another arm's binary) gives the A/A band. With http_idle_count arms, each row
also carries the server's Asio handler and syscall counts per request ("per_request"); setting
IDLE_SYSCOUNT_DYLIB inserts that library into the server for the syscall counts.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import time
from pathlib import Path

UNITS = {"us": 1.0, "ms": 1000.0, "s": 1e6}


def latency_us(text: str, label: str) -> float:
    m = re.search(rf"^\s*{label}\s+([\d.]+)(us|ms|s)\s*$", text, re.MULTILINE)
    return float(m.group(1)) * UNITS[m.group(2)] if m else float("nan")


def server_env(port: int) -> dict[str, str]:
    env = {"HTTP_IDLE_BASE_PORT": str(port)}
    if dylib := os.environ.get("IDLE_SYSCOUNT_DYLIB"):
        env["DYLD_INSERT_LIBRARIES"] = dylib
    return env


def per_request(before: dict[str, object], after: dict[str, object], requests: int) -> dict[str, float]:
    counts: dict[str, float] = {}
    for group in ("asio", "syscalls"):
        b, a = before.get(group), after.get(group)
        if not isinstance(a, dict) or not requests:
            continue
        b = b if isinstance(b, dict) else {}
        for key, value in a.items():
            delta = int(value) - int(b.get(key, 0))
            if delta:
                counts[f"{group}: {key}"] = round(delta / requests, 3)
    return counts


def run_row(binary: Path, mode: str, path: str, conns: int, close: bool, seconds: int, port: int) -> dict[str, object]:
    server = subprocess.Popen(
        [str(binary), mode],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=server_env(port),
    )
    assert server.stdin and server.stdout
    config = json.loads(server.stdout.readline())
    server.stdout.readline()  # the baseline snapshot

    def snapshot() -> dict[str, object]:
        assert server.stdin and server.stdout
        server.stdin.write("load\n")
        server.stdin.flush()
        row: dict[str, object] = json.loads(server.stdout.readline())
        return row

    scheme = "https" if mode == "tls" else "http"
    url = f"{scheme}://127.0.0.1:{port}{path}"
    header = ["-H", "Connection: close"] if close else []
    threads = str(min(conns, 2))
    try:
        subprocess.run(["wrk", "-t1", f"-c{conns}", "-d1s", *header, url], capture_output=True, check=True)
        before = snapshot()
        out = subprocess.run(
            ["wrk", f"-t{threads}", f"-c{conns}", f"-d{seconds}s", "--latency", *header, url],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        after = snapshot()
    finally:
        server.stdin.write("quit\n")
        server.stdin.flush()
        server.wait(timeout=60)
    m = re.search(r"(\d+) requests in", out)
    requests = int(m.group(1)) if m else 0
    errors = re.search(r"Socket errors:.*|Non-2xx.*", out)
    rps = re.search(r"Requests/sec:\s+([\d.]+)", out)
    cpu = (int(str(after["cpu_us"])) - int(str(before["cpu_us"]))) / requests if requests else float("nan")
    return {
        "product_commit": config["product_commit"],
        "requests": requests,
        "rps": float(rps.group(1)) if rps else float("nan"),
        "p50_us": latency_us(out, "50%"),
        "p99_us": latency_us(out, "99%"),
        "cpu_us_per_req": cpu,
        "errors": errors.group(0) if errors else "",
        "per_request": per_request(before, after, requests),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", action="append", required=True, metavar="NAME=BINARY")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--seconds", type=int, default=3)
    parser.add_argument("--row", action="append", required=True, metavar="MODE,PATH,CONNS,keep|close")
    parser.add_argument("--port", type=int, default=26000)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    arms = [(a.split("=", 1)[0], Path(a.split("=", 1)[1])) for a in args.arm]
    rows = []
    for spec in args.row:
        mode, path, conns, kind = spec.split(",")
        rows.append((mode, path, int(conns), kind == "close"))
    results: list[dict[str, object]] = []
    port = args.port
    with args.out.open("w") as out:
        for r in range(args.rounds):
            order = arms if r % 2 == 0 else list(reversed(arms))
            for mode, path, conns, close in rows:
                for name, binary in order:
                    port = args.port + (port - args.port + 8) % 800
                    row = run_row(binary, mode, path, conns, close, args.seconds, port)
                    row.update({"arm": name, "round": r, "mode": mode, "path": path, "conns": conns, "close": close})
                    results.append(row)
                    out.write(json.dumps(row) + "\n")
                    out.flush()
                    print(json.dumps(row), flush=True)
                    time.sleep(0.5)
    first = arms[0][0]
    print(f"\n| row | arm | req/s | p50 us | p99 us | CPU us/req | req/s vs {first} (range) | CPU vs {first} (range) |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for mode, path, conns, close in rows:
        label = f"{mode} {path} c{conns} {'close' if close else 'keep'}"
        mine = [x for x in results if (x["mode"], x["path"], x["conns"], x["close"]) == (mode, path, conns, close)]
        base = {int(str(x["round"])): x for x in mine if x["arm"] == first}
        for name, _ in arms:
            xs = [x for x in mine if x["arm"] == name]

            def med(key: str, items: list[dict[str, object]] = xs) -> float:
                return statistics.median(float(str(x[key])) for x in items)

            def ratios(
                key: str, items: list[dict[str, object]] = xs, base: dict[int, dict[str, object]] = base
            ) -> list[float]:
                return [float(str(x[key])) / float(str(base[int(str(x["round"]))][key])) for x in items]

            rr, cr = ratios("rps"), ratios("cpu_us_per_req")
            print(
                f"| {label} | {name} | {med('rps'):,.0f} | {med('p50_us'):.0f} | {med('p99_us'):.0f} | {med('cpu_us_per_req'):.2f} "
                f"| {statistics.median(rr):.3f} ({min(rr):.3f}-{max(rr):.3f}) | {statistics.median(cr):.3f} ({min(cr):.3f}-{max(cr):.3f}) |"
            )


if __name__ == "__main__":
    main()
