#!/usr/bin/env python3
"""Medians of bench/compare.sh output, read from standard input.

For each connection count, kind and build: the number of runs, the median requests per second, the median average
latency in microseconds, the spread of that latency (max - min over the median) and the median server CPU per request.
"""
import collections
import re
import statistics
import sys

LINE = re.compile(
    r"(\w+)\s+(\S+)\s+R=\d+ C=(\d+)\s+keepalive req/s\s+([\d.]+)\s+avg\s+([\d.]+)(us|ms)\s+.*server CPU ([\d.]+)")
runs = collections.defaultdict(list)
for line in sys.stdin:
    m = LINE.match(line)
    if not m:
        continue
    kind, build, conns, rps, avg, unit, cpu = m.groups()
    runs[(int(conns), kind, build)].append((float(rps), float(avg) * (1000 if unit == "ms" else 1), float(cpu)))

print("conns kind   build                    runs   req/s   avg us  spread  cpu us")
for (conns, kind, build), v in sorted(runs.items()):
    rps, avg, cpu = ([x[i] for x in v] for i in range(3))
    spread = (max(avg) - min(avg)) / statistics.median(avg) * 100
    print(f"{conns:5d} {kind:6s} {build:24s} {len(v):4d} {statistics.median(rps):7.0f} {statistics.median(avg):8.1f} {spread:5.0f}%  {statistics.median(cpu):6.2f}")
