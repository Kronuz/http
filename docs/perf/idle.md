# Idle connections: where the bytes go

An idle HTTP connection is one that served its request and waits for the next. This note says what
such a connection holds, plain and TLS, what changed to make it cheaper, and the measured numbers.

## What an idle connection holds

| Owner | Before (http:a17b197) | After |
|---|---|---|
| Connection coroutine frame (`serve_connection_body`) | about 70 KiB: the 64 KiB read scratch lived in it | about 2.5 KiB: the scratch is a heap buffer that exists only while a body is read |
| `ConnectionInput` (the read-ahead watch, 4,096-byte buffer inline) | 4,272 B | the same; it now also carries the idle read |
| Parse buffer (`buffered`) | whatever capacity the last body grew it to | released at the idle wait when it grew past 8 KiB |
| TLS: Asio `ssl::stream` staging (`stream_core`) | two 17,408 B vectors, zero-filled | gone |
| TLS: Asio's BIO pair | two 17,408 B halves (OpenSSL `bss_bio.c`, 34,928 B requested) | gone |
| TLS: Asio engine timers | two `steady_timer` objects | gone |
| TLS: OpenSSL session state (SSL object, record layers, key share, cipher contexts) | about 28 KiB requested | the same |
| TLS: OpenSSL record buffers | released while idle (`SSL_MODE_RELEASE_BUFFERS`) | the same |
| Kernel socket | one TCP socket, no queued bytes | the same |

## What changed

**TLS: `detail::TlsStream` (http_asio_tls.h).** `asio::ssl::stream` decrypts through memory: it
reads ciphertext into its own staging buffer, writes it into a BIO pair, and lets OpenSSL read
from the pair. Both the staging and the pair are allocated at construction and live as long as
the connection, about 70 KiB per connection whether it moves a byte or not. `TlsStream` hands
OpenSSL the socket itself through a small BIO whose read and write are the socket's own
non-blocking `read_some` and `write_some`; when OpenSSL says it wants to read or write, the
operation waits for the socket's readiness (`async_wait`) and calls OpenSSL again. The kernel's
socket buffer is the staging, so an idle TLS connection holds the SSL object and its session
state, and OpenSSL's record buffers are released between records. The error mapping is
asio's (a `close_notify` is `eof`, a TCP close without it `stream_truncated`, OpenSSL errors
in the ssl category), a result found without waiting is posted rather than delivered inside the
initiating call, and one read and one write may be in flight at once, as before.

**Plain and TLS: no read buffer while idle (http_asio.h).** The connection read into a 64 KiB
array in its coroutine frame, so every connection carried it for life, and a connection that
once received a large body kept those pages dirty. Headers are now read through
`ConnectionInput::read_ahead()`, the watch's own read into its 4 KiB buffer, appended to the
parse buffer; on plain TCP that read waits for readability and then reads without waiting. A
body is read through a 64 KiB scratch allocated by its first read and released at the next idle
wait.

## Numbers

Host: macOS 15.8.1 on an i9-9980HK (16 threads, 32 GiB), Apple clang 17, Release, OpenSSL 3.6.5,
Asio 1.36. `bench/idle-cost` holds N connections against eight one-reactor services; a separate
client process opens them, each serves one request, and the server snapshots itself three times
while idle. B/conn is (idle - fresh baseline) / N, the median of the three snapshots, then the
median over rounds; the spread is max - min over rounds (the A/A band). Footprint is macOS
`phys_footprint`, what the system charges the process; RSS undercounts it under compression.

Pending: jobs 261115-214321-2a3f2c (10K plain), 261115-214324-a3838f (10K TLS),
261115-214329-4d7b79 (100K TLS), 261115-214335-6ff7f4 (100K plain), 261115-214340-d15502 (100K
owners), 261115-214345-65ed84 (after a 64 KiB POST), 261115-214349-d25963 (wrk load) and
261115-214354-19da72 (watch_ab).

## Reproduce

```sh
quiet check
ARMS="base=<checkout at the before commit> head=." sh bench/idle-cost/ab.sh build
ARMS="base=... head=." MODES="plain tls" COUNT=10000 ROUNDS=3 quiet exclusive idle 10 'idle A/B' -- sh bench/idle-cost/ab.sh run
ARMS="base=... head=." quiet exclusive load 10 'load A/B' -- sh bench/idle-cost/ab.sh load
```

`COUNT=100000 ROUNDS=1 OWNERS=0` gives a 100K row, `ROUNDS=0 OWNERS=1` its owners, `WARM=65536`
has each connection POST 64 KiB before it idles. The client resets each connection at the end
(`SO_LINGER` 0), so no `FIN_WAIT` backlog outlives a run.
