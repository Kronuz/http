#!/usr/bin/env python3
"""Markdown tables for the T6 results: per compiler, metric, kind and connection count, the medians of A, AA, B0, B1 over
the rounds, the paired B/A ratios (median and range), the A/AA band (range of the paired AA/A ratios), and a verdict:
"within" when the median paired ratio lies inside the band, else "better" or "worse" in the metric's direction."""
import re
import statistics
import sys
from pathlib import Path

ROW = re.compile(
    r"^(?P<kind>\S+)\s+(?P<label>\S+)\s+R=\d+ C=(?P<c>\d+)\s+\S+\s+req/s\s+(?P<rps>[\d.]+)\s+avg\s+\S+\s+p50\s+\S+\s+p99\s+(?P<p99>[\d.]+)(?P<u>us|ms|s)\s+reqs\s+\d+\s+server CPU\s+(?P<cpu>[\d.]+) us/req.*?proc CPU\s+(?P<proc>[\d.]+)"
)
UNIT = {"us": 1.0, "ms": 1000.0, "s": 1e6}
METRICS = [("req/s", "rps", True), ("p99 (us)", "p99", False), ("server CPU per request (us)", "cpu", False)]
SERIES = ["a", "aa", "b0", "b1"]


def load(d: Path):
    data = {}  # (series, kind, c) -> list of dicts by round
    for s in SERIES:
        for f in sorted((d / "runs" / s).glob("r*.txt")):
            for line in f.read_text().splitlines():
                m = ROW.match(line)
                if not m:
                    continue
                v = {"rps": float(m["rps"]), "p99": float(m["p99"]) * UNIT[m["u"]], "cpu": float(m["cpu"]), "proc": float(m["proc"])}
                data.setdefault((s, m["kind"], int(m["c"])), []).append(v)
    return data


def table(data, key, higher, kinds, conns):
    out = ["| kind | C | A | AA/A band | AA | B0/A | B0 | B1/A | B1 | n |", "|---|---|---|---|---|---|---|---|---|---|"]
    for k in kinds:
        for c in conns:
            col = {s: [r[key] for r in data.get((s, k, c), [])] for s in SERIES}
            n = min(len(v) for v in col.values())
            if n == 0:
                continue
            a = col["a"][:n]
            ratio = lambda s: [x / y for x, y in zip(col[s][:n], a)]
            q = ratio("aa")
            lo, hi = min(q), max(q)

            def cell(s):
                r = ratio(s)
                med = statistics.median(r)
                if lo <= med <= hi:
                    v = "within"
                else:
                    good = (med > hi) if higher else (med < lo)
                    v = "better" if good else "worse"
                return f"{(med - 1) * 100:+.1f}% ({(min(r) - 1) * 100:+.1f}..{(max(r) - 1) * 100:+.1f}) {v}"

            med = lambda s: statistics.median(col[s][:n])
            fmt = (lambda x: f"{x:,.0f}") if key in ("rps", "p99") else (lambda x: f"{x:.2f}")
            out.append(
                f"| {k} | {c} | {fmt(med('a'))} | {(lo - 1) * 100:+.1f}..{(hi - 1) * 100:+.1f}% | {fmt(med('aa'))} | {cell('b0')} | {fmt(med('b0'))} | {cell('b1')} | {fmt(med('b1'))} | {n} |"
            )
    return "\n".join(out)


def main():
    root = Path(sys.argv[1])
    kinds = ["http", "httpr", "httpt", "httpb"]
    conns = [8, 64]
    for comp in sys.argv[2:]:
        data = load(root / comp)
        for name, key, higher in METRICS:
            print(f"\n#### {comp}: {name} ({'higher' if higher else 'lower'} is better)\n")
            print(table(data, key, higher, kinds, conns))


main()
