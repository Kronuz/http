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
initiating call, one read and one write may be in flight at once, and a write joins up to 8 KiB
of small buffers into one record, as before: a reply's head and body leave in one record and one
send. The joined copy belongs to the write and is released when it completes.

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

The arms are http:a17b197 (before), http:d92104d (TlsStream alone) and http:f1e173f (after, both
changes). The bench is http:f1e173f's in every arm.

### 10K idle connections

Exclusive jobs 261115-214324-a3838f (TLS) and 261115-214321-2a3f2c (plain), three interleaved
rounds per arm.

| transport | arm | footprint B/conn | spread | OpenSSL B/conn | idle connections per GiB |
|---|---|---:|---:|---:|---:|
| TLS | before (a17b197) | 96,727 | 50 | 63,612 | 11,101 |
| TLS | TlsStream (d92104d) | 45,801 | 31 | 28,540 | 23,444 |
| TLS | after (f1e173f) | 39,941 | 33 | 28,540 | 26,883 |
| plain | before (a17b197) | 14,376 | 8 | 0 | 74,689 |
| plain | TlsStream (d92104d) | 14,365 | 28 | 0 | 74,745 |
| plain | after (f1e173f) | 8,668 | 9 | 0 | 123,869 |

An idle TLS connection costs 58.7% less (56,786 B fewer, 2.42x as many per GiB); TlsStream takes
50,926 B of that and the idle read buffer 5,860 B. A plain one costs 39.7% less (5,708 B fewer,
1.66x per GiB). Every difference is over 600 times its spread. The plain TlsStream arm
does not change the plain path and lands 11 B from the before arm, inside its 28 B spread: the
A/A control.

After each connection POSTs 64 KiB before it idles (job 261115-214345-65ed84, two rounds), the
before arm keeps the pages the body touched and the after arm does not:

| transport | before B/conn | spread | after B/conn | spread | change |
|---|---:|---:|---:|---:|---|
| plain | 135,301 | 1,210 | 9,149 | 10 | 14.8x smaller |
| TLS | 169,046 | 336 | 40,348 | 38 | 4.19x smaller; 6,352 to 26,612 per GiB |

### By owner, 10K

From the owners profile (live operator-new blocks by usable size; OpenSSL's requested bytes by
allocating file), in the same jobs. Heap blocks count reserved bytes, so the before arm's
98,304 B frame shows whole although only its touched pages reach the footprint.

| owner | TLS before | TLS after | plain before | plain after |
|---|---:|---:|---:|---:|
| connection coroutine frame | 98,304 | 2,560 | 98,304 | 2,560 |
| Asio `ssl::stream` staging (2 x 17,408) | 34,816 | 0 | | |
| `ConnectionInput` (4 KiB read-ahead) | 4,608 | 4,608 | 4,608 | 4,608 |
| other heap blocks (socket, timers, parser, request) | 3,056 | 2,336 | 1,408 | 1,360 |
| OpenSSL `bss_bio.c` (the BIO pair) | 34,928 | 0 | | |
| OpenSSL session state (`tls_common.c` 8,848, `ml_kem.c` 6,336, `ssl_lib.c` 5,620, the rest) | 28,048 | 27,920 | | |
| kernel socket (receive and send mbufs) | 0 | 0 | 0 | 0 |

Task frames are five per TLS connection and four per plain one in every arm. What is left of an
idle TLS connection is OpenSSL's: its session state is 70% of the footprint, and the
X25519MLKEM768 key share (`ml_kem.c`) is 6,336 B of it.

### 100K idle connections

Pending: see the journal.

### Throughput and latency

`bench/watch_ab.sh` (job 261115-214354-19da72, five rounds, http and httpb at 1 and 64
connections, after against before with an A/A arm): every row of requests per second, p99 and CPU
per request is noise (geomeans 1.001, 0.983 and 1.009; A/A 1.000, 1.019 and 0.999).

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
