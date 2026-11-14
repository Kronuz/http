"""Keep load-generator memory outside the measured HTTP server process."""

from __future__ import annotations

import argparse
import json
import os
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
    parser.add_argument("--base-port", type=int, default=24000)
    parser.add_argument("--source-port", type=int, default=30000)
    parser.add_argument("--socket-timeout", type=float, default=60)
    parser.add_argument("--shutdown-timeout", type=float, default=120)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--download-bytes", type=int, default=0)
    parser.add_argument("--max-total-rss-gib", type=float, default=16)
    args = parser.parse_args()
    if args.socket_timeout <= 0 or args.shutdown_timeout <= 0:
        parser.error("socket/shutdown timeouts must be positive")
    if args.max_total_rss_gib <= 0:
        parser.error("--max-total-rss-gib must be positive")
    if not 1024 <= args.base_port <= 65527 or not 1024 <= args.source_port <= 65535:
        parser.error("listener/source ports must be unprivileged and in range")
    if any(
        count <= 0 or (count - 1) // 8 + args.source_port > 65535
        for count in args.counts
    ):
        parser.error("connection counts exceed the explicit source-port range")
    if not 0 <= args.payload_bytes <= 16 * 1024 * 1024 or (
        args.payload_bytes and args.counts != [1]
    ):
        parser.error("payload validation requires --counts 1 and at most16MiB")
    payload = (bytes(range(256)) * ((args.payload_bytes + 255) // 256))[
        : args.payload_bytes
    ]
    expected = b"ok"
    if payload:
        digest = 14695981039346656037
        for byte in payload:
            digest = ((digest ^ byte) * 1099511628211) & ((1 << 64) - 1)
        expected = f"{len(payload)}:{digest}".encode()
    if not 0 <= args.download_bytes <= 16 * 1024 * 1024 or (
        args.download_bytes and (args.counts != [1] or args.payload_bytes)
    ):
        parser.error(
            "download validation requires --counts 1, at most16MiB and no request payload"
        )
    if args.download_bytes:
        expected = (bytes(range(256)) * ((args.download_bytes + 255) // 256))[
            : args.download_bytes
        ]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    command = [str(args.binary.resolve()), "plain"]
    if args.tls:
        command = [str(args.binary.resolve()), "tls", *args.tls]
    server = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "HTTP_IDLE_BASE_PORT": str(args.base_port)},
    )
    clients: list[socket.socket] = []
    assert server.stdout and server.stdin
    started = time.monotonic()
    try:
        print(server.stdout.readline(), end="", flush=True)
        print(server.stdout.readline(), end="", flush=True)
        for count in args.counts:
            while len(clients) < count:
                peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                peer.settimeout(args.socket_timeout)
                try:
                    peer.bind(("127.0.0.1", args.source_port + len(clients) // 8))
                    peer.connect(("127.0.0.1", args.base_port + len(clients) % 8))
                except BaseException:
                    peer.close()
                    raise
                if args.tls:
                    peer = context.wrap_socket(peer, server_hostname="localhost")
                if payload:
                    peer.sendall(
                        f"POST /verify HTTP/1.1\r\nHost: localhost\r\nContent-Length: {len(payload)}\r\n\r\n".encode()
                    )
                    peer.sendall(payload)
                elif args.download_bytes:
                    peer.sendall(
                        f"GET /download/{args.download_bytes} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
                    )
                else:
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
                while len(body) < len(expected):
                    chunk = peer.recv(len(expected) - len(body))
                    if not chunk:
                        raise RuntimeError("connection closed in response body")
                    body += chunk
                if body != expected:
                    raise RuntimeError(
                        f"response payload mismatch: {body!r} != {expected!r}"
                    )
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
                    args.tls
                    and "release_connections" in row
                    and row["release_connections"] != count
                ):
                    raise RuntimeError(f"RELEASE_BUFFERS observation mismatch: {row}")
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
    except BaseException:
        print(json.dumps({"invalid_row_connections": len(clients)}), flush=True)
        raise
    finally:
        for peer in clients:
            peer.close()
        if server.poll() is None:
            server.stdin.write("quit\n")
            server.stdin.flush()
            try:
                server.wait(timeout=args.shutdown_timeout)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    if server.returncode:
        raise RuntimeError(f"server exit {server.returncode}")


if __name__ == "__main__":
    main()
