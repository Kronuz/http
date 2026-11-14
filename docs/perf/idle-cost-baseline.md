# Idle connection cost baseline

Status: partial. Both 10K connection rows have three successful exclusive A/A runs. The 100K plain row timed out during setup; the 100K TLS row reached all connections and captured snapshots but exceeded the driver's 15 s teardown allowance. Neither failed job supplies an accepted 100K A/A baseline. `http:2e8aeb3` gives large rows bounded 120 s cleanup, reports setup progress on failure, and records macOS physical footprint/compressed memory alongside RSS; replacement 100K runs use separate ten-minute holds.

Product baseline: `http:09b644d`; benchmark: `http:e2dbc35`, with active-connection checks and RSS budgeting through `http:c89db09`; explicit source-port reuse and independent listener ranges at `http:6b90898`. Host: native x86_64 macOS 15.8.1 (Darwin 24.6.0); Apple clang 17, Release `-O3 -DNDEBUG`, OpenSSL 3.6.5. `file .scratch/build/http_idle_cost` reports a Mach-O 64-bit x86_64 executable. No product header changes.

## Measurement rows

Bytes per connection are the run median of `(idle - fresh zero-connection baseline) / active connections`; A/A spread is max minus min across three run medians. Source commit is `http:6b90898` for both accepted rows; product remains `http:09b644d`. OpenSSL is a subset of allocator live bytes, and RSS describes resident pages rather than allocated capacity.

| Transport | Active connections | RSS B/connection | RSS A/A spread | Allocator live B/connection | Allocator A/A spread | OpenSSL B/connection |
|---|---:|---:|---:|---:|---:|---:|
| Plain | 10,000 | 14,360.986 | 7.782 | 104,385.792 | 0.000 | 0 |
| Plain | 100,000 | Not yet accepted | — | Not yet accepted | — | 0 |
| TLS | 10,000 | 96,598.426 | 161.382 | 204,536.434 | 7.526 | 63,611.915 |
| TLS | 100,000 | Not yet accepted | — | Not yet accepted | — | Not yet accepted |

OpenSSL's A/A spread at 10K is zero. TLS adds 100,150.642 allocated bytes per connection over plain at 10K (about 97.8 KiB); 63,611.915 B is inside OpenSSL and 36,538.726 B remains outside that allocator subset. That remainder includes Asio TLS engine storage, wrapper frames, and runtime ownership; it is not attributed to a single owner.

Raw accepted runs, bytes after subtracting each run's baseline:

| Row/run | RSS delta | Allocator live delta | OpenSSL live delta |
|---|---:|---:|---:|
| Plain 10K/1 | 143,609,856 | 1,043,857,920 | 0 |
| Plain 10K/2 | 143,663,104 | 1,043,857,920 | 0 |
| Plain 10K/3 | 143,585,280 | 1,043,857,920 | 0 |
| TLS 10K/1 | 965,681,152 | 2,045,303,920 | 636,119,152 |
| TLS 10K/2 | 967,294,976 | 2,045,379,184 | 636,119,152 |
| TLS 10K/3 | 965,984,256 | 2,045,364,336 | 636,119,152 |

Evidence: `261113-192215-1205cc` and `261113-192220-de33ab`, both exit 0. The raw baseline and all three idle snapshots for every run are in each job's log. The 100K TLS failed-cleanup job `261113-192222-9bd3a8` reported 20,445,698,672 additional allocated bytes but only 616,157,184 additional RSS bytes at its completed snapshot. That large allocation/RSS gap motivates the added compressed/physical-footprint counters; do not extrapolate RSS linearly across scales or use the failed-cleanup row as an accepted A/A baseline.

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

The client lives in a separate process; only server memory is sampled. Eight listeners (default 24000–24007, one reactor each) share explicit client source ports (default 30000–42499 for 100K connections), with `SO_REUSEADDR` set before bind. Every A/A run selects a fresh listener range to avoid TIME_WAIT tuple reuse. Simply spreading implicit connects across listeners was insufficient on macOS: its implicit source-port allocator still exhausted the range on the original jobs. The driver completes one request per connection, asserts cumulative responses **and active server connections** equal the requested count, rejects any TLS/admission failure, waits two seconds and collects three snapshots. It keeps all connections open through both rows. The server reports RSS, allocator live/reserved bytes and rounded OpenSSL live bytes/blocks. Subtract each fresh server's zero-connection baseline, then divide by the exact active count. A failed/timed-out row is invalid, not a lower connection count result.

Remaining attribution requires allocation stacks/frame-pool accounting and a kernel socket estimate from host network-memory counters under the same exclusive hold. RSS and allocator-live numbers describe different ownership layers; never sum them. OpenSSL allocations are a subset of allocator live bytes. This benchmark currently aggregates SSL/BIO/record allocations, so it cannot satisfy the full owner-breakdown requirement by itself.

Validation: Release build; eight plain and eight TLS active-connection smoke cases; `ruff format`, `ruff check`, and `pyrefly check --config bench/idle-cost/pyrefly.toml --python-interpreter-path /usr/local/bin/python3` (strict, zero errors).

## Queued exact-commit evidence

The global campaign queue holds the following jobs at `http:c89db09`; no quest or campaign-state file changes. Measurements wait for the machine and run with `quiet exclusive`, at most ten minutes per job. Each row uses three fresh-server A/A runs. The branch remains partial until successful row output is collected and the missing ownership attribution is supplied. The first four measurement jobs below failed with `errno 49`; their incomplete A/A sets do not establish a baseline.

| Job | Purpose | Dependency |
|---|---|---|
| `261113-170955-a13003` | Initial Release build at `http:de4fadf` | None |
| `261113-171110-9fcfc2` | Build and 1,000-connection plain/TLS correctness smoke with RSS budget | Initial build |
| `261113-171146-9e2dfc` | Plain 10K, three runs | Smoke |
| `261113-171152-409f9b` | Plain 100K, three runs | Smoke |
| `261113-171157-e6b63b` | TLS 10K, three runs | Smoke |
| `261113-171203-e73448` | TLS 100K, three runs | Smoke |

Use `campaign -C Detent job show JOB` to read the command, state and output. The retained slot is `codex-http-idle-baseline`. The driver checks combined client/server RSS every 1,000 connections and aborts above `--max-total-rss-gib` (default 16 GiB) to leave headroom on this shared 32 GiB host. An aborted 100K row remains an invalid/missing 100K result; do not relabel it as a smaller successful row. Kernel socket allocations are outside that process-RSS budget and still require independent attribution.

## Source-port correction and replacement jobs

`http:6b90898` binds client sockets with `SO_REUSEADDR`, reuses each source port across the eight different destinations, and exposes `--base-port`/`--source-port`. The client source range is outside this host's implicit ephemeral range. The explicit source-port bound also rejects impossible requested connection counts before starting. Raw baseline output prints the effective listener base.

| Job | Purpose | Result |
|---|---|---|
| `261113-191959-d6aa99` | Release build; three 1K plain runs on fresh listener ranges; 1K TLS | Passed, exit 0, 85.9 s |
| `261113-192215-1205cc` | Plain 10K, three A/A runs | Queued |
| `261113-192217-78e811` | Plain 100K, three A/A runs | Queued |
| `261113-192220-de33ab` | TLS 10K, three A/A runs | Queued |
| `261113-192222-9bd3a8` | TLS 100K, three A/A runs | Queued |

The measurement jobs use listener bases `25000 + 32*row + offset`, where row is 0..3 in table order and offset is 0, 8, 16 across repeats. For example, plain 10K uses bases 25000, 25008 and 25016. Reproduce a repeat with `python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 10000 --base-port 25000`; retain `quiet exclusive` around the full three-run row. The 10K jobs passed; both 100K jobs failed as described above. Their complete logs preserve the actual commands. Subsequent 100K reruns use one run per hold because the original TLS setup alone took 335 s, so three runs cannot reliably fit one ten-minute job.

## Concrete controls and private reduction experiments

`SSL_MODE_RELEASE_BUFFERS` is already set on each SSL instance by pinned Asio's `asio/ssl/detail/impl/engine.ipp:47-51`. Setting it again on the HTTP TLS context is not a reduction. The benchmark handshake observer records the actual per-instance mode and counts instances with that bit. `BIO_get_write_buf_size(SSL_get_wbio(ssl), 0)` observes the real BIO write capacity through its public getter.

| Owner/control | Current source | Private experiment | Capacity difference before allocator rounding |
|---|---|---|---:|
| HTTP coroutine read scratch | `http_asio.h:854`, 64 KiB inline local | Generated private HTTP header changes this one local to 4 KiB; `http_idle_small_read` | 60 KiB/connection, plain and TLS |
| Asio encrypted input/output staging | `asio/ssl/detail/stream_core.hpp:35,41,43`, two eager 17 KiB vectors | Generated private Asio copy sets staging to 2 KiB; `http_idle_small_tls` | 30 KiB/TLS connection |
| BIO pair buffers | `asio/ssl/detail/impl/engine.ipp:55,72`, `BIO_new_bio_pair(...,0,...,0)` | Same private copy constructs both BIO halves with 2,048-byte write buffers | About 30 KiB/TLS connection if the observed default is 17 KiB/half |
| Both changes | Above | `http_idle_small_both` combines private HTTP and Asio copies | Expected 120 KiB/TLS connection before rounding; not yet measured |

These are generated build-directory copies; the product headers and fetched dependency trees remain unchanged. CMake refuses to configure an experiment when its expected source statement is absent. Source capacities are hypotheses for measured allocation changes, not measured RSS savings.

Asio's `engine::put_input` returns the unconsumed suffix after a partial BIO write; `get_output` drains whatever output buffer space is available. The reduced staging size therefore does not impose a smaller TLS protocol-record limit. The experiment must still pass real transfers using records larger than 2 KiB: `run.py --counts 1 --payload-bytes 8388608` verifies upload length and FNV-1a digest; `--counts 1 --download-bytes 8388608` verifies every returned byte. Both run over plain and TLS as applicable. Smaller buffers can increase read/write/parse iterations, so passing these checks establishes payload correctness only, not a throughput or CPU-cost verdict.

`http_idle_owners` is a separate instrumented target. Its OpenSSL hooks attach a small allocation header and track **live requested bytes** by OpenSSL allocation source filename. `http_idle_owners_small_tls` applies the same tracking to the small-TLS variant. Profile output also reports instrumentation header bytes, live task frames and native per-socket mbuf allocations and configured queue capacities. Those profiles are not interchangeable with the ordinary baseline's rounded allocator numbers; their purpose is owner attribution. Kernel queue capacity is a limit, not allocated resident memory, and the process socket API does not expose socket/PCB object allocation sizes.

A readiness-first TLS read is a further proposal, not this experiment: delay calling `SSL_read` while idle until buffered plaintext/encrypted input exists or the underlying socket is readable. Merely wrapping the public stream with a socket-readiness wait is insufficient because Asio can retain unconsumed encrypted bytes in `stream_core::input_`. The decision belongs beside that engine state and must preserve buffered plaintext, control records, close-notify/truncation, and cancellation. The native-SSL socket adapter alternative changes a larger transport/security surface and needs its own design review.
