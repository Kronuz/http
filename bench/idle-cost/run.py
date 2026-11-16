"""Open N idle HTTP connections to an http_idle_cost server and print its memory snapshots.

One run: start the server (plain or TLS), open every connection from this process (one GET,
or one POST of --warm-bytes, each reply checked), wait, snapshot three times while idle, then
reset every connection (SO_LINGER 0, so no FIN_WAIT or TIME_WAIT outlives the run) and stop
the server. Every line printed is JSON; a failed or short run raises and prints no idle rows.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import socket
import ssl
import struct
import subprocess
import sys
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event, Lock

LISTENERS = 8


def fnv1a(data: bytes) -> int:
    digest = 14695981039346656037
    for byte in data:
        digest = ((digest ^ byte) * 1099511628211) & ((1 << 64) - 1)
    return digest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--base-port", type=int, default=24000)
    parser.add_argument("--source-port", type=int, default=30000)
    parser.add_argument("--warm-bytes", type=int, default=0)
    parser.add_argument("--idle-seconds", type=float, default=5)
    parser.add_argument("--client-workers", type=int, default=8)
    parser.add_argument("--socket-timeout", type=float, default=60)
    parser.add_argument("--shutdown-timeout", type=float, default=120)
    parser.add_argument("--max-total-rss-gib", type=float, default=20)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.count <= 0 or (args.count - 1) // LISTENERS + args.source_port > 65535:
        parser.error("the count exceeds the explicit source-port range")
    if not 1 <= args.client_workers <= 16:
        parser.error("--client-workers must be between 1 and 16")
    if not 0 <= args.warm_bytes <= 1024 * 1024:
        parser.error("--warm-bytes must be at most 1 MiB")
    payload = (bytes(range(256)) * ((args.warm_bytes + 255) // 256))[: args.warm_bytes]
    request = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
    expected = b"ok"
    if payload:
        request = (
            f"POST /verify HTTP/1.1\r\nHost: localhost\r\nContent-Length: {len(payload)}\r\n\r\n".encode() + payload
        )
        expected = f"{len(payload)}:{fnv1a(payload)}".encode()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    command = [str(args.binary.resolve()), "tls" if args.tls else "plain"]
    print(
        json.dumps(
            {
                "label": args.label,
                "binary": str(args.binary),
                "count": args.count,
                "tls": args.tls,
                "warm_bytes": args.warm_bytes,
                "base_port": args.base_port,
                "source_port": args.source_port,
                "client_workers": args.client_workers,
            }
        ),
        flush=True,
    )
    server = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "HTTP_IDLE_BASE_PORT": str(args.base_port)},
    )
    assert server.stdout and server.stdin
    clients: list[socket.socket] = []
    clients_lock = Lock()
    bind_lock = Lock()
    stop = Event()
    started = time.monotonic()
    timing: dict[str, float] = {}

    def progress(what: str) -> None:
        # The job log's record of where a run spends its time, as it happens.
        print(f"# {args.label} {time.monotonic() - started:.1f}s: {what}", file=sys.stderr, flush=True)

    def ask(stage: str) -> dict[str, object]:
        assert server.stdin and server.stdout
        server.stdin.write(stage + "\n")
        server.stdin.flush()
        line = server.stdout.readline()
        if not line:
            raise RuntimeError(f"server ended (exit {server.poll()}) at {stage}")
        row: dict[str, object] = json.loads(line)
        return row

    def connect_one(index: int) -> None:
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        peer.settimeout(args.socket_timeout)
        try:
            # macOS refuses concurrent binds of one source port: bind and connect one at a time.
            with bind_lock:
                peer.bind(("127.0.0.1", args.source_port + index // LISTENERS))
                peer.connect(("127.0.0.1", args.base_port + index % LISTENERS))
            if args.tls:
                peer = context.wrap_socket(peer, server_hostname="localhost")
            peer.sendall(request)
            response = b""
            while b"\r\n\r\n" not in response:
                chunk = peer.recv(4096)
                if not chunk:
                    raise RuntimeError("connection closed before the response")
                response += chunk
            head, body = response.split(b"\r\n\r\n", 1)
            if not head.startswith(b"HTTP/1.1 200"):
                raise RuntimeError(f"unexpected response: {head!r}")
            while len(body) < len(expected):
                chunk = peer.recv(len(expected) - len(body))
                if not chunk:
                    raise RuntimeError("connection closed in the response body")
                body += chunk
            if body != expected:
                raise RuntimeError(f"response mismatch: {body!r} != {expected!r}")
        except BaseException:
            peer.close()
            raise
        with clients_lock:
            clients.append(peer)

    def worker(lane: int) -> None:
        try:
            for index in range(lane, args.count, args.client_workers):
                if stop.is_set():
                    return
                connect_one(index)
        except BaseException:
            stop.set()
            raise

    def memory_check() -> None:
        row = ask("memory-check")
        client_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform != "darwin":
            client_rss *= 1024
        combined = int(str(row["rss"])) + client_rss
        if combined > args.max_total_rss_gib * 1024**3:
            stop.set()
            raise RuntimeError(f"combined RSS {combined} over {args.max_total_rss_gib} GiB at {len(clients)}")

    try:
        print(server.stdout.readline(), end="", flush=True)  # configuration
        print(server.stdout.readline(), end="", flush=True)  # baseline
        with ThreadPoolExecutor(max_workers=args.client_workers) as pool:
            futures = [pool.submit(worker, lane) for lane in range(args.client_workers)]
            checked = 0
            while True:
                done, _ = wait(futures, timeout=1, return_when=FIRST_EXCEPTION)
                if len(clients) - checked >= 5000:
                    memory_check()
                    checked = len(clients)
                for future in done:
                    future.result()
                if len(done) == len(futures):
                    break
        if len(clients) != args.count:
            raise RuntimeError(f"setup stopped at {len(clients)} of {args.count}")
        timing["setup_s"] = time.monotonic() - started
        progress(f"{len(clients)} connections open")
        time.sleep(2)
        rows = []
        for stage in ("idle-0", "idle-1", "idle-2"):
            if stage == "idle-1":
                time.sleep(args.idle_seconds)
            row = ask(stage)
            if args.tls and row["release_connections"] != args.count:
                raise RuntimeError(f"RELEASE_BUFFERS seen on {row['release_connections']} of {args.count}")
            if row["served"] != args.count or row["active"] != args.count or row["rejected"] or row["tls_failures"]:
                raise RuntimeError(f"effective load mismatch: {row}")
            row["idle_seconds_before"] = args.idle_seconds if stage == "idle-1" else 0
            rows.append(row)
            print(json.dumps(row), flush=True)
        timing["measured_s"] = time.monotonic() - started
        progress("idle snapshots taken")
    except BaseException:
        print(json.dumps({"invalid_run_connections": len(clients)}), flush=True)
        raise
    finally:
        progress(f"teardown: closing {len(clients)} connections")
        linger = struct.pack("ii", 1, 0)
        for closed, peer in enumerate(clients, 1):
            try:
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
            except OSError:
                pass
            peer.close()
            if closed % 10000 == 0:
                progress(f"teardown: {closed} closed")
        timing["closed_s"] = time.monotonic() - started
        progress("teardown: connections closed; stopping the server")
        if server.poll() is None:
            server.stdin.write("quit\n")
            server.stdin.flush()
            try:
                server.wait(timeout=args.shutdown_timeout)
            except subprocess.TimeoutExpired:
                print(json.dumps({"server_killed_by_driver": True}), flush=True)
                server.kill()
                server.wait()
        progress("teardown: server stopped")
        timing["total_s"] = time.monotonic() - started
        print(json.dumps({"timing": timing, "server_exit": server.returncode}), flush=True)
    if server.returncode:
        raise RuntimeError(f"server exit {server.returncode}")


if __name__ == "__main__":
    main()
