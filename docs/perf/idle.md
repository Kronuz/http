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
`phys_footprint`, what the system charges the process, compressed pages included; RSS undercounts
it under compression.

The arms are http:a17b197 (before) and http:76f698d (after, both changes and the TLS write join),
built from http:76f698d's bench. Every job is exclusive. The runs are in `docs/perf/idle/`;
`python3 bench/idle-cost/summarize.py docs/perf/idle/<row>` prints each table below.

### 10K and 100K idle connections

| transport | idle | arm | rounds | footprint B/conn | spread | OpenSSL B/conn | idle connections per GiB | wakeups/s |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| TLS | 10K | before | 3 | 96,742 | 152 | 63,612 | 11,099 | 0.2 |
| TLS | 10K | after | 3 | 39,932 | 31 | 28,540 | 26,889 | 0.2 |
| TLS | 100K | before | 2 | 96,527 | 44 | 63,601 | 11,124 | 0.2 |
| TLS | 100K | after | 2 | 39,788 | 5 | 28,529 | 26,987 | 0.2 |
| plain | 10K | before | 3 | 14,367 | 8 | 0 | 74,736 | 0.2 |
| plain | 10K | after | 3 | 8,661 | 8 | 0 | 123,980 | 0.2 |
| plain | 100K | before | 2 | 14,327 | 1 | 0 | 74,945 | 0.2 |
| plain | 100K | after | 2 | 8,637 | 0 | 0 | 124,318 | 0.3 |

Jobs: 10K TLS 261116-012341-188fac, 10K plain 261116-012345-cba126 (three interleaved rounds each);
100K one arm and round per job in ABBA order, TLS 261116-012350-66ef55, -012355-6cbb4e,
-012359-7fcd7c, -012405-815b06, plain 261116-012410-6d7174, -012414-ba9ee6, -012418-4d5602,
-012423-dcb492.

- **TLS:** an idle connection costs 58.7% less at 10K (56,810 B fewer) and 58.8% less at 100K
  (56,739 B fewer): 2.42x as many idle TLS connections per GiB. Per round at 100K, after over
  before is 0.4123 and 0.4121. Each difference is over 370 times the larger arm's spread.
- **Plain:** 39.7% less at both counts (5,706 and 5,690 B fewer), 1.66x as many per GiB.
- **Per 100K idle connections:** TLS goes from 9.65 GB (8.99 GiB) to 3.98 GB (3.71 GiB), 5.28 GiB
  saved; plain from 1.43 GB (1.33 GiB) to 0.86 GB (0.80 GiB), 0.53 GiB saved. A 16 GiB budget
  holds about 432K idle TLS connections after, 178K before.
- **Under pressure:** at 100K TLS the before arm's 9.65 GB pushed macOS to compress 5.8 to 6.1 GB
  of it (RSS 3.5 to 3.8 GB); the after arm's 3.98 GB was not compressed. Footprint counts both.
- **Wakeups:** 0.2 per second in both arms, idle; the change does not move them.

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
allocating file): 10K in the jobs above, 100K in 261116-012427-2c38f4, -012431-fec7c5 (TLS) and
261116-012436-2ce3c9, -012441-481714 (plain). The blocks are the same at both counts. Heap blocks
count reserved bytes, so the before arm's 98,304 B frame shows whole although only its touched
pages reach the footprint.

| owner | TLS before | TLS after | plain before | plain after |
|---|---:|---:|---:|---:|
| connection coroutine frame | 98,304 | 2,560 | 98,304 | 2,560 |
| Asio `ssl::stream` staging (2 x 17,408) | 34,816 | 0 | | |
| `ConnectionInput` (4 KiB read-ahead) | 4,608 | 4,608 | 4,608 | 4,608 |
| other heap blocks (socket, timers, parser, request) | 3,056 | 2,336 | 1,408 | 1,360 |
| OpenSSL `bss_bio.c` (the BIO pair) | 34,928 | 0 | | |
| OpenSSL session state (`tls_common.c` 8,848, `ml_kem.c` 6,336, `ssl_lib.c` 5,620, the rest) | 28,048 | 27,920 | | |
| kernel socket (receive and send mbufs) | 0 | 0 | 0 | 0 |

OpenSSL's requested bytes per connection are 62,976 before and 27,920 after at 10K, 62,965 and
27,909 at 100K. Task frames are five per TLS connection and four per plain one in every arm. What
is left of an idle TLS connection is OpenSSL's: its session state is 70% of the footprint, and the
X25519MLKEM768 key share (`ml_kem.c`) is 6,336 B of it.

### Throughput and latency

`bench/idle-cost/ab.sh load` (wrk against the bench server), five interleaved rounds of before,
after and an A/A arm (before again). Each cell is the median over rounds of after over before,
with the min-max over rounds: for five rounds that is the distribution-free interval at 93.8%.
Jobs 261116-012332-c66999 (TLS keep-alive) and 261116-012337-d0d5f6 (TLS close, plain 1 MiB).

| row | req/s after | req/s A/A | CPU/req after | CPU/req A/A | p50 after | p99 after |
|---|---:|---:|---:|---:|---:|---:|
| TLS `/` c1 keep-alive | **0.924 (0.850-0.985)** | 1.019 (0.963-1.079) | **1.177 (1.086-1.257)** | 0.991 (0.931-1.042) | 1.140 | 1.056 |
| TLS `/` c64 keep-alive | **0.760 (0.753-0.851)** | 0.992 (0.941-1.055) | **1.311 (1.177-1.324)** | 1.008 (0.950-1.059) | 1.281 | 1.508 |
| TLS 1 MiB c8 keep-alive | 1.137 (1.045-1.164) | 1.017 (0.903-1.040) | 0.906 (0.889-0.978) | 0.982 (0.965-1.095) | 0.885 | 0.815 |
| TLS `/` c8 close | 0.989 (0.949-1.016) | 1.024 (0.956-1.059) | 1.012 (0.998-1.061) | 0.997 (0.940-1.065) | 0.857 | 0.894 |
| plain 1 MiB c8 keep-alive | 0.978 (0.965-0.994) | 0.996 (0.945-1.032) | 1.023 (1.006-1.036) | 1.004 (0.968-1.058) | 1.008 | 0.977 |

- **Regressed, outside the A/A band:** small TLS replies on a kept-alive connection. At one
  connection the after arm serves 7.6% fewer requests per second for 17.7% more CPU per request;
  at 64 connections 24.0% fewer for 31.1% more CPU, with p50 1.28x. The write join (http:76f698d)
  narrowed this from 0.616x and 0.596x at http:4279a49 (jobs 261115-235032-963543,
  261115-235036-fc949b) but did not close it.
- **Better, outside the band:** 1 MiB TLS downloads, 13.7% more per second for 9.4% less CPU.
- **Noise:** TLS with a handshake per request and plain 1 MiB downloads.

`bench/watch_ab.sh` (job 261115-214354-19da72, http:f1e173f, five rounds, http and httpb at 1
and 64 connections, after against before with an A/A arm): every row of requests per second, p99
and CPU per request is noise (geomeans 1.001, 0.983 and 1.009; A/A 1.000, 1.019 and 0.999).

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
