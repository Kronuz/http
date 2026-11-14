"""Keep load-generator memory outside the measured HTTP server process."""

from __future__ import annotations

import argparse
import json
import resource
import socket
import ssl
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("--tls", nargs=2, metavar=("CERT", "KEY"))
    parser.add_argument("--counts", nargs="+", type=int, default=[10000, 100000])
    parser.add_argument("--max-total-rss-gib", type=float, default=16)
    args = parser.parse_args()
    if args.max_total_rss_gib <= 0:
        parser.error("--max-total-rss-gib must be positive")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    command = [str(args.binary.resolve()), "plain"]
    if args.tls:
        command = [str(args.binary.resolve()), "tls", *args.tls]
    server = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    clients: list[socket.socket] = []
    assert server.stdout and server.stdin
    started = time.monotonic()
    try:
        print(server.stdout.readline(), end="", flush=True)
        print(server.stdout.readline(), end="", flush=True)
        for count in args.counts:
            while len(clients) < count:
                peer = socket.create_connection(
                    ("127.0.0.1", 18920 + len(clients) % 8), timeout=10
                )
                if args.tls:
                    peer = context.wrap_socket(peer, server_hostname="localhost")
                peer.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                response = b""
                while b"\r\n\r\n" not in response:
                    chunk = peer.recv(4096)
                    if not chunk:
                        raise RuntimeError("connection closed before response")
                    response += chunk
                head, body = response.split(b"\r\n\r\n", 1)
                if not head.startswith(b"HTTP/1.1 200"):
                    raise RuntimeError(f"unexpected response: {head!r}")
                while len(body) < 2:
                    chunk = peer.recv(2 - len(body))
                    if not chunk:
                        raise RuntimeError("connection closed in response body")
                    body += chunk
                clients.append(peer)
                if len(clients) % 1000 == 0:
                    server.stdin.write("memory-check\n")
                    server.stdin.flush()
                    memory = json.loads(server.stdout.readline())
                    client_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    if sys.platform != "darwin":
                        client_rss *= 1024
                    combined = memory["rss"] + client_rss
                    if combined > args.max_total_rss_gib * 1024**3:
                        raise RuntimeError(
                            f"invalid row: combined process RSS {combined} exceeds "
                            f"{args.max_total_rss_gib} GiB at {len(clients)} connections"
                        )
            time.sleep(2)
            for repeat in range(3):
                server.stdin.write(f"idle-{count}-{repeat}\n")
                server.stdin.flush()
                row = json.loads(server.stdout.readline())
                if (
                    row["served"] != count
                    or row["active"] != count
                    or row["rejected"]
                    or row["tls_failures"]
                ):
                    raise RuntimeError(f"effective load mismatch: {row}")
                print(json.dumps(row), flush=True)
            print(
                json.dumps(
                    {"elapsed_s": time.monotonic() - started, "connections": count}
                ),
                flush=True,
            )
    finally:
        for peer in clients:
            peer.close()
        if server.poll() is None:
            server.stdin.write("quit\n")
            server.stdin.flush()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    if server.returncode:
        raise RuntimeError(f"server exit {server.returncode}")


if __name__ == "__main__":
    main()
