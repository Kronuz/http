# Idle connection cost baseline

Status: partial. The Release benchmark builds and its plain/TLS smoke cases complete with eight active connections each. The required exclusive 10K/100K measurements have **not run**: other campaign light holds occupy the host. A five-second exclusive acquisition attempt was canceled before it started. No RSS slope, allocator slope, TLS buffer cost, A/A spread or measured saving is claimed.

Product baseline: `http:09b644d`; benchmark: `http:e2dbc35`, with active-connection checks and RSS budgeting through `http:c89db09`. Host: native x86_64 macOS 15.8.1 (Darwin 24.6.0); Apple clang 17, Release `-O3 -DNDEBUG`, OpenSSL 3.6.5. `file .scratch/build/http_idle_cost` reports a Mach-O 64-bit x86_64 executable. No product header changes.

## Measurement rows

| Transport | Active connections | Server RSS delta | Allocator live delta | OpenSSL live delta | Bytes/connection | A/A spread |
|---|---:|---|---|---|---|---|
| Plain | 10,000 | Pending exclusive run | Pending | N/A | Unmeasured | Unmeasured |
| Plain | 100,000 | Pending exclusive run | Pending | N/A | Unmeasured | Unmeasured |
| TLS | 10,000 | Pending exclusive run | Pending | Pending | Unmeasured | Unmeasured |
| TLS | 100,000 | Pending exclusive run | Pending | Pending | Unmeasured | Unmeasured |

The smoke output is `.scratch/smoke.log` in this worktree. It is correctness evidence, collected under `quiet light`, and is deliberately excluded from cost estimates. The blocked acquisition and contemporaneous hold list are `.scratch/exclusive-attempt.log` (2026-11-13, about 17:01 UTC): x7-gate had light holds ending at 18:25, 18:50 and 22:20 UTC. The benchmark leaves no server/client running after completion or a normal exception.

## Ownership and candidates

These are source/storage estimates, not a measured owner decomposition. Object sizes come from the benchmark's `sizeof` output on this toolchain. An enclosing coroutine owns several of these objects; **do not add them to the coroutine allocation again**.

| Owner | Evidence | Known storage | Measurement still needed |
|---|---|---:|---|
| Socket | Asio socket object | 88 B in userspace | Kernel socket, send/receive queue and protocol control-block memory; these are outside process RSS |
| Read buffer | `http_asio.h:854`, `serve_connection_body` | `char tmp[64*1024]`: 65,536 B per suspended connection | Actual coroutine allocation size and how many buffer pages become resident |
| Read-ahead input | `http_asio.h:437,663,1112` | Input object 4,272 B including 4,096-byte inline buffer | Shared-owner allocation overhead and retained asynchronous read handlers |
| Parser/request | `http_asio.h:834`; `http_request_parser.h` | Parser 544 B; Request 280 B (may be nested/overlap) | Retained dynamic strings/maps after first response |
| Coroutine frames | `serve_connection`, `serve_connection_body`, TLS wrapper, awaitable reads | Includes the above local read buffer/parser and suspended await state | Allocator stack attribution/frame-pool capacities and cached blocks |
| Timers | `asio::steady_timer` object | 120 B per actual timer instance | Live deadline/control count; baseline service uses default disabled request/idle/header/inactivity timeouts |
| TLS | `http_asio_tls.h:96`; Asio SSL engine and OpenSSL | Benchmark hooks report rounded live OpenSSL allocations separately | Split SSL/context/session/BIO/read/write buffers by allocation stack; OpenSSL hooks alone aggregate them |
| Write storage | `http_asio.h:855` reused response head, buffered writer and TLS engine | Depends on prior response; benchmark uses `ok` | Retained response capacities and TLS record buffers |

1. Move the 64 KiB read scratch out of every idle coroutine into bounded active-read storage. The source-level ceiling is 65,536 B per idle connection, or 6,553,600,000 B at 100K; this is an allocation-capacity ceiling, **not** an RSS prediction. Buffer lifetime must span the original asynchronous read, and admission must bound simultaneous active reads.
2. Allocate the read-ahead buffer when watch/read-ahead actually needs it. The inline-array ceiling is 4,096 B per idle connection (409,600,000 B at 100K), less owner bookkeeping. Preserve the 4 KiB pipeline bound, TLS eager-watch behavior and cancellation lifetime.
3. Evaluate OpenSSL's idle record-buffer release policy and Asio's SSL engine buffers separately. A rough hypothesis is one or two 16 KiB record buffers (16–32 KiB/connection), but ownership and eligibility must be measured before claiming that saving. Check handshake throughput, idle-to-active latency, partial records and renegotiation behavior in the follow-up quest.

## Reproduce

From the benchmark branch worktree, use an available OpenSSL 3 prefix (this host's prefix is shown):

```sh
quiet check
quiet light codex-http-idle-build 10 'Configure idle-cost Release' -- cmake -S bench/idle-cost -B .scratch/build -DCMAKE_BUILD_TYPE=Release -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@3
quiet light codex-http-idle-build 10 'Build idle-cost Release' -- cmake --build .scratch/build -j4 --target http_idle_cost
file .scratch/build/http_idle_cost
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost -keyout .scratch/key.pem -out .scratch/cert.pem -days 1
quiet light codex-http-idle-smoke 2 'Plain smoke' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 8
quiet light codex-http-idle-smoke 2 'TLS smoke' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 8 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive codex-http-idle-plain 10 'Plain 10K and 100K idle' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost > .scratch/idle-plain.jsonl
quiet exclusive codex-http-idle-tls 10 'TLS 10K and 100K idle' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --tls .scratch/cert.pem .scratch/key.pem > .scratch/idle-tls.jsonl
```

Run each exclusive command three times with a fresh server for run-level A/A spread; the three snapshots per connection count only test within-run stability. No product change exists, so the before/after comparison is the same named product commit. Any future cut needs its own commit and interleaved A/B runs.

The client lives in a separate process; only server memory is sampled. Eight listeners (18920–18927, one reactor each) avoid the single-destination ephemeral-port ceiling at 100K connections. The driver completes one request per connection, asserts cumulative responses **and active server connections** equal the requested count, rejects any TLS/admission failure, waits two seconds and collects three snapshots. It keeps all connections open through both rows. The server reports RSS, allocator live/reserved bytes and rounded OpenSSL live bytes/blocks. Subtract each fresh server's zero-connection baseline, then divide by the exact active count. A failed/timed-out row is invalid, not a lower connection count result.

Remaining attribution requires allocation stacks/frame-pool accounting and a kernel socket estimate from host network-memory counters under the same exclusive hold. RSS and allocator-live numbers describe different ownership layers; never sum them. OpenSSL allocations are a subset of allocator live bytes. This benchmark currently aggregates SSL/BIO/record allocations, so it cannot satisfy the full owner-breakdown requirement by itself.

Validation: Release build; eight plain and eight TLS active-connection smoke cases; `ruff format`, `ruff check`, and `pyrefly check --config bench/idle-cost/pyrefly.toml --python-interpreter-path /usr/local/bin/python3` (strict, zero errors).

## Queued exact-commit evidence

The global campaign queue holds the following jobs at `http:c89db09`; no quest or campaign-state file changes. Measurements wait for the machine and run with `quiet exclusive`, at most ten minutes per job. Each row uses three fresh-server A/A runs. The branch remains partial until their actual output is collected and the missing ownership attribution is supplied.

| Job | Purpose | Dependency |
|---|---|---|
| `261113-170955-a13003` | Initial Release build at `http:de4fadf` | None |
| `261113-171110-9fcfc2` | Build and 1,000-connection plain/TLS correctness smoke with RSS budget | Initial build |
| `261113-171146-9e2dfc` | Plain 10K, three runs | Smoke |
| `261113-171152-409f9b` | Plain 100K, three runs | Smoke |
| `261113-171157-e6b63b` | TLS 10K, three runs | Smoke |
| `261113-171203-e73448` | TLS 100K, three runs | Smoke |

Use `campaign -C Detent job show JOB` to read the command, state and output. The retained slot is `codex-http-idle-baseline`. The driver checks combined client/server RSS every 1,000 connections and aborts above `--max-total-rss-gib` (default 16 GiB) to leave headroom on this shared 32 GiB host. An aborted 100K row remains an invalid/missing 100K result; do not relabel it as a smaller successful row. Kernel socket allocations are outside that process-RSS budget and still require independent attribution.
