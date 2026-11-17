"""Throughput, latency and server CPU per request of http_idle_cost arms under wrk, interleaved.

Each round runs every arm (order alternating by round) through every row: a transport, a path,
a connection count and keep-alive or close. A row's server CPU per request is the server's own
rusage across the timed run divided by wrk's request count. The summary gives, per row and arm,
the median over rounds and the median paired ratio against the first arm with its range; the
"aa" arm (a copy of another arm's binary) gives the A/A band. With http_idle_count arms, each row
also carries the server's Asio handler and syscall counts per request ("per_request"); setting
IDLE_SYSCOUNT_DYLIB inserts that library into the server for the syscall counts. With
--sample-dir, /usr/bin/sample records the server's stacks through the timed run, one file per
run (sample_summary.py reads them); sampling slows the server, so such rows are profiles, not
A/B timing.
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


def run_row(
    binary: Path, mode: str, path: str, conns: int, close: bool, seconds: int, port: int, sample_file: Path | None
) -> dict[str, object]:
    server = subprocess.Popen(
        [str(binary), mode], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=server_env(port)
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
        wrk = subprocess.Popen(
            ["wrk", f"-t{threads}", f"-c{conns}", f"-d{seconds}s", "--latency", *header, url],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        sampler = None
        if sample_file:
            # Inside the timed run: half a second in, ending a second and a half before wrk does.
            time.sleep(0.5)
            sampler = subprocess.Popen(
                ["sample", str(server.pid), str(max(1, seconds - 2)), "1", "-mayDie", "-file", str(sample_file)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        out, _ = wrk.communicate()
        if wrk.returncode:
            raise subprocess.CalledProcessError(wrk.returncode, wrk.args)
        after = snapshot()
        if sampler and sampler.wait(timeout=120):
            raise subprocess.CalledProcessError(sampler.returncode, sampler.args)
    finally:
        server.stdin.write("quit\n")
        server.stdin.flush()
        server.wait(timeout=60)
    m = re.search(r"(\d+) requests in", out)
    requests = int(m.group(1)) if m else 0
    errors = re.search(r"Socket errors:.*|Non-2xx.*", out)
    rps = re.search(r"Requests/sec:\s+([\d.]+)", out)

    def per_req(key: str) -> float:
        if key not in after or not requests:
            return float("nan")
        return (int(str(after[key])) - int(str(before[key]))) / requests

    return {
        "product_commit": config["product_commit"],
        "requests": requests,
        "rps": float(rps.group(1)) if rps else float("nan"),
        "p50_us": latency_us(out, "50%"),
        "p99_us": latency_us(out, "99%"),
        "cpu_us_per_req": per_req("cpu_us"),
        "user_us_per_req": per_req("user_us"),
        "system_us_per_req": per_req("system_us"),
        "voluntary_switches_per_req": per_req("voluntary_switches"),
        "involuntary_switches_per_req": per_req("involuntary_switches"),
        "sample_file": sample_file.name if sample_file else "",
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
    parser.add_argument("--sample-dir", type=Path, help="record each run's stacks with /usr/bin/sample here")
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
                    sample_file = None
                    if args.sample_dir:
                        args.sample_dir.mkdir(parents=True, exist_ok=True)
                        kind = "close" if close else "keep"
                        sample_file = args.sample_dir / f"{name}-{mode}-c{conns}-{kind}-{r}.txt"
                    row = run_row(binary, mode, path, conns, close, args.seconds, port, sample_file)
                    row.update({"arm": name, "round": r, "mode": mode, "path": path, "conns": conns, "close": close})
                    results.append(row)
                    out.write(json.dumps(row) + "\n")
                    out.flush()
                    print(json.dumps(row), flush=True)
                    time.sleep(0.5)
    first = arms[0][0]
    print(
        f"\n| row | arm | req/s | p50 us | p99 us | CPU us/req | user | system | switches/req "
        f"| req/s vs {first} (range) | CPU vs {first} (range) |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
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
                f"| {med('user_us_per_req'):.2f} | {med('system_us_per_req'):.2f} | {med('voluntary_switches_per_req'):.3f} "
                f"| {statistics.median(rr):.3f} ({min(rr):.3f}-{max(rr):.3f}) | {statistics.median(cr):.3f} ({min(cr):.3f}-{max(cr):.3f}) |"
            )


if __name__ == "__main__":
    main()
