"""Summarize ab.sh runs: bytes per idle connection per arm, the A/A spread, and the owners.

A run's value for a metric is the median over its three idle snapshots of (idle - baseline)
/ connections; an arm's is the median over its rounds, and its spread is max - min over them.
Owners come from the owners-profile run: live heap blocks per connection by size class and
OpenSSL's requested bytes per connection by allocating file.
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

METRICS = ("footprint", "rss", "allocator_live", "openssl_live")
GIB = 1024**3


def load(path: Path) -> dict[str, object]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]
    head = rows[0]
    config = next(r for r in rows if "product_commit" in r)
    baseline = next(r for r in rows if r.get("stage") == "baseline")
    idle = [r for r in rows if str(r.get("stage", "")).startswith("idle-")]
    if len(idle) != 3:
        raise SystemExit(f"{path}: {len(idle)} idle snapshots, not 3")
    return {"head": head, "config": config, "baseline": baseline, "idle": idle}


def per_conn(run: dict[str, object], key: str) -> float:
    count = int(run["head"]["count"])  # type: ignore[index]
    base = float(run["baseline"][key])  # type: ignore[index]
    return statistics.median((float(r[key]) - base) / count for r in run["idle"])  # type: ignore[union-attr]


def wakeups(run: dict[str, object]) -> float:
    a, b = run["idle"][0], run["idle"][1]  # type: ignore[index]
    seconds = float(b["idle_seconds_before"]) or 1.0
    return (
        b["voluntary_switches"] + b["involuntary_switches"] - a["voluntary_switches"] - a["involuntary_switches"]
    ) / seconds


def main() -> None:
    runs = [load(p) for p in sorted(Path(sys.argv[1]).glob("*.jsonl"))]
    timed: dict[tuple[str, str, int], list[dict[str, object]]] = defaultdict(list)
    owners: dict[tuple[str, str, int], dict[str, object]] = {}
    for run in runs:
        arm, mode, round_ = str(run["head"]["label"]).split("/")  # type: ignore[index]
        key = (mode, arm, int(run["head"]["count"]))  # type: ignore[index]
        if round_ == "owners":
            owners[key] = run
        else:
            timed[key].append(run)
    print(
        "| transport | arm | count | warm B | rounds | footprint B/conn | spread | RSS B/conn | spread | allocator B/conn | spread | OpenSSL B/conn | idle TLS-or-plain conns per GiB | wakeups/s | commit |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for (mode, arm, count), group in sorted(timed.items()):
        values = {m: [per_conn(r, m) for r in group] for m in METRICS}
        med = {m: statistics.median(v) for m, v in values.items()}
        spread = {m: max(v) - min(v) for m, v in values.items()}
        commit = str(group[0]["config"]["product_commit"])[:12]  # type: ignore[index]
        warm = group[0]["head"]["warm_bytes"]  # type: ignore[index]
        wake = statistics.median(wakeups(r) for r in group)
        print(
            f"| {mode} | {arm} | {count} | {warm} | {len(group)} | {med['footprint']:,.0f} | {spread['footprint']:,.0f} | {med['rss']:,.0f} | {spread['rss']:,.0f} "
            f"| {med['allocator_live']:,.0f} | {spread['allocator_live']:,.0f} | {med['openssl_live']:,.0f} | {GIB / med['footprint']:,.0f} | {wake:.1f} | {commit} |"
        )
    for (mode, arm, count), run in sorted(owners.items()):
        n = int(run["head"]["count"])  # type: ignore[index]
        base_heap = {c: k for c, k in run["baseline"]["heap"]}  # type: ignore[index]
        idle = run["idle"][0]  # type: ignore[index]
        print(f"\n#### Owners: {mode}, {arm} ({str(run['config']['product_commit'])[:12]}), {count} connections")  # type: ignore[index]
        print(
            f"\ntask frames per connection: {(idle['task_frames'] - run['baseline']['task_frames']) / n:.2f}; "  # type: ignore[index]
            f"kernel TCP sockets: {idle.get('kernel_sockets', 'n/a')}, receive mbuf bytes {idle.get('kernel_receive_mbuf_bytes', 'n/a')}, "
            f"send mbuf bytes {idle.get('kernel_send_mbuf_bytes', 'n/a')}\n"
        )
        print("| heap block (usable B) | blocks/conn | B/conn |")
        print("|---:|---:|---:|")
        rows = []
        for c, k in idle["heap"]:
            d = (k - base_heap.get(c, 0)) / n
            if abs(d) >= 0.05:
                size = (idle["heap_large_bytes"] - run["baseline"]["heap_large_bytes"]) / n / d if c < 0 else c  # type: ignore[index]
                rows.append((size * d, c, d))
        total = 0.0
        for bytes_, c, d in sorted(rows, reverse=True):
            total += bytes_
            print(f"| {'>1 MiB' if c < 0 else c} | {d:.2f} | {bytes_:,.0f} |")
        print(f"| all listed | | {total:,.0f} |")
        if mode == "tls":
            base_ssl = {o["file"]: o["bytes"] for o in run["baseline"]["openssl_owners"]}  # type: ignore[index]
            print("\n| OpenSSL file | requested B/conn |")
            print("|---|---:|")
            ssl_rows = sorted(
                ((o["bytes"] - base_ssl.get(o["file"], 0)) / n, o["file"]) for o in idle["openssl_owners"]
            )
            for bytes_, file in reversed(ssl_rows):
                if abs(bytes_) >= 1:
                    print(f"| {file} | {bytes_:,.0f} |")
            print(f"| all files | {sum(b for b, _ in ssl_rows):,.0f} |")


if __name__ == "__main__":
    main()
