"""Where the server's time goes per request, by arm, from load_ab.py --sample-dir runs.

Each /usr/bin/sample file is a call tree per thread with a sample count per node; a node's own
samples are its count less its children's. A sample whose own frame is a blocking wait (kevent,
condition waits, the stdin read) is idle; every other sample is CPU, the syscall stubs
(libsystem_kernel) being time in the kernel. A count becomes microseconds per request as
samples / ticks * 1e6 / rps, ticks being the samples per thread over the run. Tables: own time
by library, inclusive time under named frames, and the functions whose own time moved most;
each cell is the median over rounds, and the delta is each arm's median less the first arm's.

    python3 bench/idle-cost/sample_summary.py LOAD_JSONL SAMPLE_DIR
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

IDLE = {"kevent", "__psynch_cvwait", "__semwait_signal", "read", "__workq_kernreturn", "mach_msg2_trap", "__select"}
LINE = re.compile(r"^([ +!:|]*)(\d+) (.*)$")
FRAME = re.compile(r"^(.*?)  \(in (.*?)\)")
# Inclusive time under these frames, outermost occurrence only.
ANCHORS = {
    "SSL_read_ex": r"\bSSL_read_ex\b",
    "SSL_write_ex": r"\bSSL_write_ex\b",
    "SSL_do_handshake": r"\bSSL_do_handshake\b",
    "SSL_new": r"\bSSL_new\b",
    "SSL_free": r"\bSSL_free\b",
    "SSL_shutdown": r"\bSSL_shutdown\b",
    "BIO read (TlsStream::bio_read, bio_pair)": r"bio_read|bio_pair|bio_bread|BIO_read",
    "BIO write (TlsStream::bio_write, bio_pair)": r"bio_write|bio_bwrite|BIO_write",
    "asio ssl engine": r"asio::ssl::detail::engine",
    "TlsStream": r"TlsStream",
    "ConnectionInput": r"ConnectionInput",
    "serve_connection_body": r"serve_connection_body",
    "request parse": r"RequestParser|llhttp|http_parser",
    "handler": r"Handler::handle",
    "accept": r"\baccept\b|async_accept|reactive_socket_accept",
    "malloc family": r"^(malloc|calloc|realloc|free|_malloc_zone|_free|szone_|tiny_|small_|nanov2_|_nanov2|mvm_|operator new|operator delete|CRYPTO_malloc|CRYPTO_free|CRYPTO_zalloc|ssl_malloc|ssl_free|ssl_realloc)",
    "memcpy/memmove/memset": r"_platform_mem(move|set|cpy)|__bzero|^memset|^memcpy|^memmove",
    "syscall: recvfrom": r"^__recvfrom",
    "syscall: sendto": r"^__sendto",
    "syscall: close": r"^(close|__close_nocancel)$",
    "ERR state": r"\bERR_|ossl_err_|err_get_state|CRYPTO_THREAD_get_local",
}


def strip_templates(name: str) -> str:
    out: list[str] = []
    depth = 0
    for ch in name:
        if ch == "<":
            depth += 1
        elif ch == ">" and depth:
            depth -= 1
        elif not depth:
            out.append(ch)
    return re.sub(r"\s+", " ", "".join(out))[:140]


class Node:
    __slots__ = ("children", "count", "library", "symbol")

    def __init__(self, count: int, symbol: str, library: str) -> None:
        self.count, self.symbol, self.library = count, symbol, library
        self.children: list[Node] = []

    def own(self) -> int:
        return self.count - sum(c.count for c in self.children)


def parse(path: Path) -> list[Node]:
    """The call graph's threads, each a tree."""
    threads: list[Node] = []
    stack: list[tuple[int, Node]] = []
    in_graph = False
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("Call graph:"):
            in_graph = True
            continue
        if not in_graph:
            continue
        if not line.strip():
            if threads:
                break
            continue
        m = LINE.match(line)
        if not m:
            continue
        depth, count, rest = len(m.group(1)), int(m.group(2)), m.group(3)
        f = FRAME.match(rest)
        symbol, library = (f.group(1).strip(), f.group(2)) if f else (rest.strip(), "")
        node = Node(count, symbol, library)
        while stack and stack[-1][0] >= depth:
            stack.pop()
        if stack:
            stack[-1][1].children.append(node)
        else:
            threads.append(node)
        stack.append((depth, node))
    return threads


def measure(path: Path, rps: float) -> dict[str, float]:
    """Microseconds per request by key: 'lib <name>', 'anchor <name>', 'fn <name>', 'cpu'."""
    threads = parse(path)
    ticks = max((t.count for t in threads), default=0)
    if not ticks or not rps:
        return {}
    scale = 1e6 / rps / ticks
    out: dict[str, float] = defaultdict(float)
    anchors = {k: re.compile(v) for k, v in ANCHORS.items()}

    def walk(node: Node, active: frozenset[str]) -> None:
        here = set(active)
        for key, pattern in anchors.items():
            if key not in active and pattern.search(node.symbol):
                out[f"anchor {key}"] += node.count * scale
                here.add(key)
        own = node.own()
        if own > 0 and node.symbol not in IDLE and not node.symbol.startswith("Thread_"):
            library = node.library or "?"
            out[f"lib {library}"] += own * scale
            out[f"fn {strip_templates(node.symbol)} ({library})"] += own * scale
            out["cpu"] += own * scale
        for child in node.children:
            walk(child, frozenset(here))

    for thread in threads:
        for child in thread.children:
            walk(child, frozenset())
    # Anchor time spent in idle frames (a wait under a named frame) is not CPU.
    for thread in threads:
        _subtract_idle(thread, anchors, out, scale, frozenset())
    return dict(out)


def _subtract_idle(
    node: Node, anchors: dict[str, re.Pattern[str]], out: dict[str, float], scale: float, active: frozenset[str]
) -> None:
    here = set(active)
    for key, pattern in anchors.items():
        if pattern.search(node.symbol):
            here.add(key)
    own = node.own()
    if own > 0 and node.symbol in IDLE:
        for key in here:
            out[f"anchor {key}"] -= own * scale
    for child in node.children:
        _subtract_idle(child, anchors, out, scale, frozenset(here))


def main() -> None:
    load, sample_dir = Path(sys.argv[1]), Path(sys.argv[2])
    runs: dict[tuple[str, str], list[dict[str, float]]] = defaultdict(list)
    rusage: dict[tuple[str, str], list[dict[str, float]]] = defaultdict(list)
    arms: list[str] = []
    for line in load.read_text().splitlines():
        r = json.loads(line)
        if not r.get("sample_file"):
            continue
        row = f"{r['mode']} {r['path']} c{r['conns']} {'close' if r['close'] else 'keep'}"
        if r["arm"] not in arms:
            arms.append(r["arm"])
        runs[(row, r["arm"])].append(measure(sample_dir / r["sample_file"], float(r["rps"])))
        rusage[(row, r["arm"])].append(
            {k: float(r[k]) for k in ("rps", "cpu_us_per_req", "user_us_per_req", "system_us_per_req")}
        )
    rows = sorted({row for row, _ in runs})
    first = arms[0]

    def med(row: str, arm: str, key: str) -> float:
        return statistics.median(m.get(key, 0.0) for m in runs[(row, arm)])

    for row in rows:
        print(f"\n## {row}: microseconds per request, median of {len(runs[(row, first)])} sampled runs\n")
        print("| measure | " + " | ".join(arms) + " | " + " | ".join(f"{a} - {first}" for a in arms[1:]) + " |")
        print("|---|" + "---:|" * (2 * len(arms) - 1))
        for key in ("rps", "cpu_us_per_req", "user_us_per_req", "system_us_per_req"):
            values = [statistics.median(u[key] for u in rusage[(row, a)]) for a in arms]
            fmt = "{:,.0f}" if key == "rps" else "{:.2f}"
            print(
                f"| rusage {key} | "
                + " | ".join(fmt.format(v) for v in values)
                + " | "
                + " | ".join(fmt.format(v - values[0]) for v in values[1:])
                + " |"
            )
        keys = {k for a in arms for m in runs[(row, a)] for k in m}

        def table(prefix: str, limit: int | None = None, row: str = row, keys: set[str] = keys) -> None:
            mine = [k for k in keys if k.startswith(prefix)]
            if limit is not None:
                mine.sort(key=lambda k: -max(abs(med(row, a, k) - med(row, first, k)) for a in arms[1:]))
                mine = mine[:limit]
            else:
                mine.sort(key=lambda k: -med(row, first, k))
            for k in mine:
                values = [med(row, a, k) for a in arms]
                print(
                    f"| {k} | "
                    + " | ".join(f"{v:.2f}" for v in values)
                    + " | "
                    + " | ".join(f"{v - values[0]:+.2f}" for v in values[1:])
                    + " |"
                )

        table("cpu")
        table("lib ")
        table("anchor ")
        print(f"\nFunctions whose own time moved most ({', '.join(arms[1:])} against {first}):\n")
        print("| function | " + " | ".join(arms) + " | " + " | ".join(f"{a} - {first}" for a in arms[1:]) + " |")
        print("|---|" + "---:|" * (2 * len(arms) - 1))
        table("fn ", 30)


if __name__ == "__main__":
    main()
