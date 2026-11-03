import re, statistics, sys
from pathlib import Path
sys.argv_save = sys.argv
exec(open('tables.py').read().split('def main')[0])
root = Path(sys.argv[1])
def abv(comp, b, section):
    txt = (root/comp/f"report-a-vs-{b}.txt").read_text()
    sec = txt.split("## ")
    for s in sec:
        if s.startswith(section):
            out = {}
            for line in s.splitlines():
                m = re.match(r"^(http\w*) (\d+)\s+\S+\s+\S+\s+[\d.]+\s+\S+\s+[\d.]+\s+\d+\s+(\w+)", line)
                if m: out[(m[1], int(m[2]))] = m[3]
            return out
def cell(data, key, higher, k, c, s, ab):
    col = {x: [r[key] for r in data[(x, k, c)]] for x in SERIES}
    n = min(map(len, col.values()))
    a = col["a"][:n]
    q = [x/y for x, y in zip(col["aa"][:n], a)]
    lo, hi = min(q), max(q)
    r = [x/y for x, y in zip(col[s][:n], a)]
    med = statistics.median(r)
    if lo <= med <= hi: m = "="
    else: m = "+" if ((med > hi) if higher else (med < lo)) else "-"
    abn = ab.get((k, c)) if ab else None
    flag = "" if (abn is None or (abn == "noise") == (m == "=")) else "*"
    return f"{(med-1)*100:+.1f}% {m}{flag}"
for s in ("b1", "b0"):
    print(f"\n##### {s.upper()} against A (median of 7 paired ratios; + better, - worse, = inside the A/AA band; * ab_compare says otherwise)\n")
    print("| kind | C | clang req/s | clang p99 | clang CPU/req | g++ req/s | g++ p99 | g++ CPU/req |")
    print("|---|---|---|---|---|---|---|---|")
    D = {c: load(root/c) for c in ("clang", "gcc")}
    AB = {c: (abv(c, s, "req/s"), abv(c, s, "p99"), abv(c, s, "server CPU per request (us, getrusage")) for c in D}
    for k in ["http", "httpr", "httpt", "httpb"]:
        for c in (8, 64):
            cells = []
            for comp in ("clang", "gcc"):
                for i, (key, higher) in enumerate((("rps", True), ("p99", False), ("cpu", False))):
                    cells.append(cell(D[comp], key, higher, k, c, s, AB[comp][i]))
            print(f"| {k} | {c} | " + " | ".join(cells) + " |")
