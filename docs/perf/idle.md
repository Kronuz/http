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
OpenSSL a small BIO of its own. A read that needs bytes receives them with Asio's `async_receive`
into the caller's buffer, which holds nothing until the plaintext lands in it, and the BIO serves
OpenSSL a copy that is freed once OpenSSL has taken it. The receive is speculative, like the one
`asio::ssl::stream` makes; a readiness wait (`async_wait`) would cost a `kevent` per request,
because the kqueue reactor re-arms its filter on every non-speculative start. Writes, the
handshake and reads under 512 B go straight to the non-blocking socket and wait for readiness
when it is full or empty. OpenSSL reads ahead, so on a kept-alive connection a record's header
and body arrive in one `recv`, and its record buffers are released between records. A handshake
still takes twice the old stream's `recv` calls (see "Small TLS replies" below). An idle TLS
connection holds the SSL object and its session state.

The error mapping is asio's (a `close_notify` is `eof`, a TCP close without it `stream_truncated`,
OpenSSL errors in the ssl category). A result found without waiting never completes inside the
initiating call: it completes through a zero-byte `async_send` on the socket, which Asio queues as
an immediate completion on the loop's own queue, with no syscall, lock or wake. That is the
speculative send `asio::ssl::stream` makes, so a small reply runs the same handlers it did.
One read and one write may be in flight at once, and a write joins up to 8 KiB of small buffers
into one record, as `asio::ssl::stream` does: a reply's head and body leave in one record and one
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
`phys_footprint`, what the system charges the process, compressed pages included; RSS undercounts
it under compression.

The before arm is http:a17b197. The after arm is the commit each row names: TLS at 10K is
http:ad6771f, the final product; TLS at 100K is http:47d6726, whose idle state differs from
ad6771f's only in the receive's handler blocks (112 B per connection, measured at 10K); plain is
http:76f698d, since plain connections do not include `http_asio_tls.h`. Every job is exclusive.
The runs are in `docs/perf/idle/`; `python3 bench/idle-cost/summarize.py docs/perf/idle/<row>`
prints each table below.

### 10K and 100K idle connections

| transport | idle | arm | rounds | footprint B/conn | spread | OpenSSL B/conn | idle connections per GiB | wakeups/s |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| TLS | 10K | before | 3 | 96,608 | 100 | 63,612 | 11,114 | 0.2 |
| TLS | 10K | after (ad6771f) | 3 | 40,054 | 20 | 28,540 | 26,808 | 0.2 |
| TLS | 100K | before | 2 | 96,471 | 47 | 63,601 | 11,130 | 0.5 |
| TLS | 100K | after (47d6726) | 2 | 39,797 | 13 | 28,529 | 26,981 | 0.2 |
| plain | 10K | before | 3 | 14,367 | 8 | 0 | 74,736 | 0.2 |
| plain | 10K | after (76f698d) | 3 | 8,661 | 8 | 0 | 123,980 | 0.2 |
| plain | 100K | before | 2 | 14,327 | 1 | 0 | 74,945 | 0.2 |
| plain | 100K | after (76f698d) | 2 | 8,637 | 0 | 0 | 124,318 | 0.3 |

Jobs: 10K TLS 261116-090554-1c2b25, 10K plain 261116-012345-cba126 (three interleaved rounds
each); 100K one arm and round per job in ABBA order, TLS 261116-060000-d56d24, -060007-067b1b,
-060012-60b2e7, -060018-27237b, plain 261116-012410-6d7174, -012414-ba9ee6, -012418-4d5602,
-012423-dcb492.

- **TLS:** an idle connection costs 58.5% less at 10K (56,554 B fewer) and 58.7% less at 100K
  (56,674 B fewer): 2.41x as many idle TLS connections per GiB. Per round at 100K, after over
  before is 0.4126 and 0.4125. Each difference is over 500 times the larger arm's spread.
- **Plain:** 39.7% less at both counts (5,706 and 5,690 B fewer), 1.66x as many per GiB.
- **Per 100K idle connections:** TLS goes from 9.65 GB (8.98 GiB) to 3.98 GB (3.71 GiB), 5.27 GiB
  saved; plain from 1.43 GB (1.33 GiB) to 0.86 GB (0.80 GiB), 0.53 GiB saved. A 16 GiB budget
  holds about 431K idle TLS connections after, 178K before.
- **Under pressure:** at 100K TLS the before arm's 9.65 GB pushes macOS to compress several GB of
  it (RSS 3.0 GB median); the after arm's 3.98 GB is not compressed. Footprint counts both.
- **Wakeups:** 0.2 to 0.5 per second idle in both arms; the change does not move them.

The TlsStream change alone (http:d92104d, job 261115-214324-a3838f, 10K, bench http:f1e173f)
measured 45,801 B against 96,727 B before: TlsStream takes about 50.9 KB of the TLS saving and the
idle read buffer the remaining 5.9 KB. The plain path at that commit measured 14,365 B against
14,376 B, inside its 28 B spread: the A/A control.

After each connection POSTs 64 KiB before it idles (job 261115-214345-65ed84, two rounds, at
http:f1e173f, whose idle path the write join does not touch), the before arm keeps the pages the
body touched and the after arm does not:

| transport | before B/conn | spread | after B/conn | spread | change |
|---|---:|---:|---:|---:|---|
| plain | 135,301 | 1,210 | 9,149 | 10 | 14.8x smaller |
| TLS | 169,046 | 336 | 40,348 | 38 | 4.19x smaller; 6,352 to 26,612 per GiB |

### By owner

From the owners profile (live operator-new blocks by usable size; OpenSSL's requested bytes by
allocating file): 10K in the jobs above (TLS after at ad6771f), 100K in 261116-012427-2c38f4
(TLS before), 261116-060022-c30d0a (TLS after, 47d6726) and 261116-012436-2ce3c9, -012441-481714
(plain). The blocks are the same at both counts but for the receive's handler blocks: TLS after's
other heap blocks are 2,448 B at ad6771f, 2,336 B at 47d6726. Heap blocks
count reserved bytes, so the before arm's 98,304 B frame shows whole although only its touched
pages reach the footprint.

| owner | TLS before | TLS after | plain before | plain after |
|---|---:|---:|---:|---:|
| connection coroutine frame | 98,304 | 2,560 | 98,304 | 2,560 |
| Asio `ssl::stream` staging (2 x 17,408) | 34,816 | 0 | | |
| `ConnectionInput` (4 KiB read-ahead) | 4,608 | 4,608 | 4,608 | 4,608 |
| other heap blocks (socket, timers, parser, request) | 3,056 | 2,448 | 1,408 | 1,360 |
| OpenSSL `bss_bio.c` (the BIO pair) | 34,928 | 0 | | |
| OpenSSL session state (`tls_common.c` 8,848, `ml_kem.c` 6,336, `ssl_lib.c` 5,620, the rest) | 28,048 | 27,920 | | |
| kernel socket (receive and send mbufs) | 0 | 0 | 0 | 0 |

OpenSSL's requested bytes per connection are 62,976 before and 27,920 after at 10K, 62,965 and
27,909 at 100K. Task frames are five per TLS connection and four per plain one in every arm. What
is left of an idle TLS connection is OpenSSL's: its session state is 70% of the footprint, and the
X25519MLKEM768 key share (`ml_kem.c`) is 6,336 B of it.

### Throughput and latency

`bench/idle-cost/ab.sh load` (wrk against the bench server), five interleaved rounds of before
(http:a17b197), after (http:ad6771f) and an A/A arm (before again). Each cell is the median over
rounds of after over before, with the min-max over rounds: for five rounds that is the
distribution-free interval at 93.8%. p50 and p99 are after's median over before's. Jobs
261116-090524-ec6fc0 (TLS keep-alive, with http:47d6726 as a third arm) and 261116-090550-a7dfdc
(TLS 1 MiB, TLS close, plain 1 MiB); runs in `docs/perf/idle/load/`.

| row | req/s after | req/s A/A | CPU/req after | CPU/req A/A | p50 after | p99 after |
|---|---:|---:|---:|---:|---:|---:|
| TLS `/` c1 keep-alive | 0.935 (0.914-1.056) | 0.986 (0.915-1.106) | **1.100 (0.997-1.144)** | 1.009 (0.910-1.084) | 1.070 | 1.038 |
| TLS `/` c64 keep-alive | 0.920 (0.910-1.004) | 0.990 (0.888-1.022) | 1.088 (0.996-1.099) | 1.011 (0.978-1.124) | 1.051 | 1.111 |
| TLS 1 MiB c8 keep-alive | 1.138 (1.093-1.222) | 1.038 (1.024-1.053) | 0.902 (0.844-0.939) | 0.962 (0.951-0.978) | 0.876 | 0.850 |
| TLS `/` c8 close | **0.967 (0.948-0.981)** | 0.988 (0.968-1.021) | 1.052 (1.038-1.067) | 1.032 (0.994-1.053) | 0.897 | 0.916 |
| plain 1 MiB c8 keep-alive | 1.005 (0.997-1.039) | 1.017 (1.001-1.036) | 0.995 (0.963-1.003) | 0.983 (0.967-0.998) | 0.977 | 0.959 |

- **Outside the A/A band, accepted (D103):** two cells, each by a hair. At one kept-alive
  connection a small TLS reply costs 10.0% more CPU (31.07 against 27.87 us per request; the
  band ends at 1.084), while its requests per second (0.935x) stay inside the band. With a
  handshake per request, TLS serves 3.3% fewer requests per second (the band starts at 0.968).
  The owner accepted both for the idle saving; "Small TLS replies" below attributes them.
- **Inside the band:** small TLS replies at 64 connections (0.920x requests per second, 1.088x CPU,
  +0.9 us per request) and plain 1 MiB downloads.
- **Better, outside the band:** 1 MiB TLS downloads, 13.8% more per second for 9.8% less CPU.

Syscalls and Asio handlers per request (`ab.sh count`, http_idle_count with libidle_syscount
interposed, job 261116-081537-cfec36, median of two runs; counts, not timing):

| per request | c1 before | c1 after | c64 before | c64 after |
|---|---:|---:|---:|---:|
| `recvfrom` (of them `EAGAIN`) | 2 (1) | 2 (1) | 2.006 (1.002) | 2.013 (1.002) |
| `sendto` | 1 | 1 | 1.004 | 1.006 |
| `kevent` | 1.998 | 1.999 | 0.020 | 0.021 |
| socket `async_receive` | 1 | 1 | 1.006 | 1.003 |
| `io_context` execute | 0 | 1 | 0.001 | 1.003 |

The syscalls match. What the after arm adds per request is one deferred handler (the write's
immediate completion), a copy of the received bytes for the BIO and the joined reply's copy.

`bench/watch_ab.sh` (job 261115-214354-19da72, http:f1e173f, five rounds, http and httpb at 1
and 64 connections, after against before with an A/A arm): every row of requests per second, p99
and CPU per request is noise (geomeans 1.001, 0.983 and 1.009; A/A 1.000, 1.019 and 0.999).

### Small TLS replies

Where http:ad6771f's extra CPU on a small kept-alive TLS reply went, from `/usr/bin/sample` on
the load rows (job 261116-190828-78aa9b, three interleaved rounds of 10 s with an A/A arm; own time
per request, after minus before):

| cost per request | us | what it is |
|---|---:|---|
| the immediate result's `asio::defer` | about 0.85 | an `executor_function` built through `any_io_executor`, the composed op moved in and out; one `io_context` execute per request that the old stream did not make. Allocation under it 0.05 |
| the received bytes' copy for the BIO | 0.34 | `take()`: what lets an idle connection hold no receive buffer |
| the joined reply's copy | 0.20 | head and body in one record; without it a reply is two records and two sends |
| OpenSSL's record path | spread | libssl +0.75 and libcrypto +0.64, no function above 0.11 |

All of it is user time; syscalls per request are equal. http:0eb4c52 replaces the defer with the
zero-byte send above: per request the c1 row now runs one socket send completion and no
`io_context` execute, as the old stream did (job 261116-200646-529fc3). The two copies stay: they
are the price of holding no buffers while idle.

`ab.sh load` at http:0eb4c52 (job 261116-200643-d30410, exclusive, five interleaved rounds of
before http:a17b197, prev http:ad6771f, after http:0eb4c52 and an A/A arm; median over rounds of
the ratio to before, min-max in parentheses; runs in `docs/perf/idle/small-reply/`):

| row | req/s prev | req/s after | req/s A/A | CPU/req prev | CPU/req after | CPU/req A/A |
|---|---:|---:|---:|---:|---:|---:|
| TLS `/` c1 keep-alive | 0.954 (0.860-1.049) | 0.989 (0.814-1.033) | 1.015 (0.929-1.157) | 1.094 (0.999-1.222) | 1.037 (0.999-1.268) | 0.982 (0.874-1.066) |
| TLS `/` c64 keep-alive | 0.953 (0.891-1.085) | 1.001 (0.939-1.112) | 1.017 (0.885-1.172) | 1.046 (0.923-1.121) | 1.000 (0.901-1.060) | 0.986 (0.854-1.129) |
| TLS `/` c8 close | 1.052 (0.944-1.111) | 1.037 (0.981-1.073) | 1.055 (0.878-1.218) | 0.943 (0.919-1.059) | 0.980 (0.897-1.018) | 0.980 (0.832-1.128) |

- **Every cell of after is inside the A/A band;** ad6771f's c1 CPU (1.094) is not. At one connection a small reply costs +1.17 us CPU per request
  over before (median of the per-round differences: user +1.37, system -0.20) on a 32.1 us request;
  ad6771f's cost +2.61 us in the same job. The two kept copies account for 0.54 us of it.
- **After against ad6771f:** at 64 connections 3.2% less CPU per request (0.968, 0.945-0.995) and
  3.3% more requests per second (1.033, 1.006-1.054), every round in the same direction but inside
  the A/A band, so not counted; at one connection 1.009 (0.882-1.038).
- **The close path:** ad6771f's 0.967x requests per second does not reproduce (1.052 here, 1.049 in
  job 261116-190832-1dac8f), and after is 1.037. A handshake takes 9.51 `recvfrom` per connection
  against 4.75 before (`EAGAIN` 2.10 against 2.00), which costs about 3.5 us of system time per
  connection (system 1.064x) and is outweighed by 15.6 us less user time. Setting read-ahead after
  the handshake state (http:8368ea9) leaves them at 9.20 (job 261116-200140-8e96ee); their cause
  is open.

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
