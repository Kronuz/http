#!/usr/bin/env python3
"""Check that every benchmark server answers the bench request with the same bytes.

  python3 bench/verify.py KIND=BINARY [KIND=BINARY ...]

Each server is started on one reactor, sent the request wrk sends, twice on one connection (keep-alive), and its
answer is compared with the reference reply byte for byte, headers included. Prints every mismatch and exits 1,
so a comparison never times servers that answer differently.
"""
import os
import socket
import subprocess
import sys
import time

# The same bytes as RESP in bench_common.h.
REFERENCE = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\nHello, World!\n"


def read_reply(sock, extra_wait):
    """The bytes the server sends for one request: until the reference length, then whatever else follows."""
    sock.settimeout(5)
    data = b""
    while len(data) < len(REFERENCE):
        chunk = sock.recv(65536)
        if not chunk:
            break
        data += chunk
    sock.settimeout(extra_wait)
    try:
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        pass
    return data


def check(kind, binary):
    port = 20000 + os.getpid() % 20000
    proc = subprocess.Popen([binary, kind, str(port), "1"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        if "ready" not in proc.stdout.readline():
            print(f"verify: {kind}: the server did not start", file=sys.stderr)
            return False
        request = f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode()
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            for n in (1, 2):
                sock.sendall(request)
                got = read_reply(sock, 0.2)
                if got != REFERENCE:
                    print(f"verify: {kind}: request {n} on one connection got different bytes\n"
                          f"  want {REFERENCE!r}\n  got  {got!r}", file=sys.stderr)
                    return False
        print(f"# verify: {kind}: {len(REFERENCE)} bytes, identical on two keep-alive requests")
        return True
    finally:
        proc.terminate()
        proc.wait()
        time.sleep(0.1)


ok = True
for arg in sys.argv[1:]:
    kind, _, binary = arg.partition("=")
    ok = check(kind, binary) and ok
sys.exit(0 if ok else 1)
