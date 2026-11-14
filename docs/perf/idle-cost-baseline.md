# Idle connection cost and reduction experiments

At 10K idle TLS connections, private buffer experiments reduce allocated bytes by **74.60%** and macOS charged footprint by **44.82%**. The TLS staging/BIO change alone gives **43.89%** lower charged footprint; shrinking only the HTTP scratch gives **0.94%**. These are measured memory results, not throughput claims or a landed product change.

All six 10K arms have three independent completed samples. The 100K plain baseline completes. The 100K TLS baseline reaches its ten-minute deadline with no valid idle snapshot (exit 124); its result is invalid. C7 remains partial: the TLS scale row and some owner-size attribution are missing. The measurement batch is closed, with no further retries or holds.

## Configuration and evidence

Product baseline: `http:09b644d`. All compared binaries embed `http:ff8cc93`; executable and driver SHA-256 digests are retained in [the raw results](../../bench/idle-cost/results-2026-11-14.json). Native x86_64 macOS 15.8.1, 32 GiB RAM, Apple clang 17, Release `-O3 -DNDEBUG`, runtime OpenSSL 3.6.5, pinned Asio 1.36. The existing transitive include order selects `/usr/local/include/openssl` headers (4.1.0) while linking the 3.6.5 library. All arms hold that same configuration; these local results are not a cross-version portability claim.

The server runs eight listeners/reactors. A separate client process establishes every connection, completes one GET/`ok` response, holds the connections open, waits two seconds and takes three snapshots. Every accepted sample asserts the exact requested active/served count, no admission/TLS failure, and `SSL_MODE_RELEASE_BUFFERS` observed on every TLS connection. Defaults disable HTTP idle/header/inactivity/request timeouts. All measurements use `quiet exclusive`; builds and payload checks use `quiet light`, at most four build jobs. No product header or fetched dependency checkout changes.

## Measured 10K costs

Bytes per connection: subtract the fresh zero-connection baseline from each idle snapshot, take the run median, then the median over three runs. A/A spread is max minus min of those run medians. OpenSSL is a subset of allocator bytes, not an additional charge. Footprint is native `TASK_VM_INFO.phys_footprint`.

| Arm | Allocator B/conn | A/A spread | Footprint B/conn | A/A spread | RSS B/conn | RSS A/A spread |
|---|---:|---:|---:|---:|---:|---:|
| Plain baseline | 104,385.894 | 0.000 | 14,356.890 | 15.155 | 14,372.045 | 15.155 |
| Plain 4 KiB HTTP | 12,737.894 | 0.000 | 12,805.734 | 17.203 | 12,820.890 | 17.203 |
| TLS baseline | 204,535.778 | 2.970 | 96,584.909 | 382.566 | 96,676.659 | 382.566 |
| TLS 4 KiB HTTP | 113,401.518 | 3.328 | 95,672.730 | 161.382 | 95,764.480 | 161.382 |
| TLS 2 KiB staging/BIO | 143,095.470 | 1.587 | 54,194.176 | 24.986 | 54,285.926 | 24.986 |
| TLS both | 51,960.187 | 3.942 | 53,296.333 | 35.635 | 53,354.906 | 33795.277 |

Every reduction clears the corresponding A/A band. One combined-arm sample compresses 33,797.734 B/conn: RSS falls to 19,592.806 B/conn while charged footprint stays 53,263.155 B/conn. That explains its wide RSS band; RSS alone is a poor comparison under compression. The combined experiment saves 432,885,760 charged bytes at 10K (about 413 MiB) and 1,525,755,904 allocated bytes. No dollar or power saving is inferred.

The HTTP-only allocation reduction exceeds the literal 60 KiB array change because allocator-rounded cost changes sharply on this host. The measured plain reduction is 91,648 B/conn; its charged-footprint reduction is only 1,551.155 B/conn. Avoid treating allocated capacity as fully resident RAM.

## 100K scale

| Arm | Samples | Allocator B/conn | Footprint B/conn | RSS B/conn | OpenSSL B/conn |
|---|---:|---:|---:|---:|---:|
| Plain baseline | 1 | 104,384.189 | 14,328.136 | 14,329.651 | 0.000 |

The plain scale row has one sample and no A/A spread; it is not an optimization acceptance result. The TLS scale row is a stated miss, not a pass or a capacity estimate. A read-only progress observation at 13:40:39 UTC finds 73,512 established server sockets (PID 79595, elapsed 7:51); that is not a validated idle/served count. No 100K TLS snapshot finishes before the ten-minute deadline, so no memory number is published for that attempt. The server is gone and the hold is released after exit 124. This does not establish a host-capacity limit. Each 100K run uses its own ten-minute hold; an earlier TLS setup took 335 s, so putting three runs in one hold cannot reliably meet the deadline. An earlier completed TLS snapshot ended with signal 9 during cleanup; its old driver had a 15 s hard-kill branch, but the log cannot distinguish that branch from an OS kill. It is excluded from accepted scale results. The current driver allows bounded 120 s cleanup and logs its own kill explicitly.

## Owner attribution

Separate owner-profile binaries add allocation headers and count **requested** OpenSSL bytes by allocation filename. Their totals are not the ordinary baseline's rounded allocator figures. Every snapshot's owner-byte/block sums are checked against its OpenSSL totals. Profile-only headers add about 865.6 B/conn and are excluded from the ordinary baseline comparison.

| OpenSSL allocation file | Baseline requested B/conn | Small TLS B/conn |
|---|---:|---:|
| `bss_bio.c` | 34,928.000 | 4,208.000 |
| `tls_common.c` | 8,848.000 | 8,848.000 |
| `ml_kem.c` | 6,336.000 | 6,336.000 |
| `ssl_lib.c` | 5,620.000 | 5,620.000 |
| `cipher_aes_gcm.c` | 1,920.000 | 1,920.000 |
| `ssl_sess.c` | 928.000 | 928.000 |
| `threads_pthread.c` | 800.000 | 800.000 |
| `ssl_cert.c` | 664.000 | 664.000 |
| `p_lib.c` | 456.000 | 456.000 |
| All allocation files | 62,975.661 | 32,255.661 |

The BIO file drops exactly 30,720 requested bytes per connection; other major owners remain unchanged. Allocation filenames identify provenance, not a complete SSL-type decomposition. ML-KEM allocations are observed under the existing negotiated cryptographic defaults; the experiment changes no cryptographic policy.

| Other owner | Evidence and cost |
|---|---|
| Asio encrypted staging | Two eager 17,408-byte vectors, reduced to 2,048 each: 30,720 bytes less capacity per TLS connection. Compiled staging size is printed in every run. |
| BIO pair | Public `BIO_get_write_buf_size` reports 17,408 normally and 2,048 in the experiment. Two buffers plus bookkeeping account for the measured BIO file delta. |
| HTTP read scratch | `http_asio.h:854` contains a 65,536-byte local retained in the suspended connection coroutine. The private copy uses 4,096 bytes. |
| Read-ahead input | `ConnectionInput` is 4,272 bytes, including its inline 4,096-byte buffer (`http_asio.h:437,663,1112`). No lazy-buffer change is implemented. |
| Parser/request | Parser is 544 bytes and contains a 280-byte Request (`http_request_parser.h:185`). These sit inside enclosing ownership and must not be added to it twice. |
| Coroutine frames | Profiles observe four live task frames per plain connection and five per TLS connection, above the common baseline. Exact bytes of every smaller frame remain unattributed. |
| Timers | `asio::steady_timer` object is 120 bytes. HTTP timeouts are disabled in this baseline; TLS handshake deadlines end before idle capture. This is not a measured count of retained timer allocations. |
| Socket | Userspace socket object is 88 bytes. Native process socket profiles observe one additional TCP socket per connection and zero receive/send mbuf bytes at idle. TCP/socket control-block allocation sizes are not exposed by this API. |

Configured socket queue capacities are approximately 407 KB receive and 147 KB send per connection in these profiles; they are limits, **not allocated idle memory**. Do not add them to RSS or allocator bytes. `allocator_reserved` in raw output is the native `malloc_statistics_t.size_allocated` counter; it is not a process-wide reserved-VM measurement.

## Concrete controls and prototypes

1. OpenSSL release-buffers mode is already enabled by pinned Asio `engine.ipp:47-50`. Runtime mode is 23 (`0x17`) and every measured TLS connection includes the release bit. Setting the same flag on the context again provides no reduction.
2. `http_idle_small_read` builds against generated private HTTP headers with only the 64 KiB scratch changed to 4 KiB. This yields a large allocation-budget reduction but a small physical-footprint reduction for this workload.
3. `http_idle_small_tls` builds against generated private Asio copies: `stream_core.hpp:34,41,43` staging changes 17 KiB→2 KiB and `engine.ipp:54,72` constructs each BIO half with 2,048 bytes instead of the default. `http_idle_small_both` combines the two experiments.

CMake refuses to generate an experiment if its expected source statement is absent. Original product headers and fetched dependencies remain unchanged. Asio `put_input` handles partial BIO writes by returning the unconsumed suffix; `get_output` drains arbitrary available output space. Smaller staging does not impose a smaller TLS protocol-record limit.

All four binaries pass 8 MiB uploads (length plus FNV-1a digest) and exact 8 MiB downloads over both plain and TLS: 16 transfer checks. The two owner binaries also pass eight-connection accounting smoke checks. These transfers exercise TLS records larger than the small buffers and output backpressure/partial-BIO paths. They establish payload correctness, not throughput or CPU cost. Before production adoption, compare bulk transfer CPU/throughput and the existing transport/cancellation/security suites with matched headers and libraries.

Recommendation: prioritize configurable smaller Asio staging/BIO capacity. It accounts for almost all measured TLS physical-footprint improvement without changing cryptographic policy. Treat the HTTP scratch change as an allocation-budget tradeoff until bulk CPU/throughput is measured. A readiness-first TLS read remains a separate hypothesis: Asio may retain encrypted bytes in `stream_core::input_`, so a public socket-readiness wrapper alone can stall buffered data. Its memory benefit is not established by this experiment.

## Setup failures and limits

The original implicit client-port allocator exhausted its range (`errno 49`). Explicit source binding with `SO_REUSEADDR` and separate destination ranges permits successful 100K setup. Later runs also encounter a distinct **first-connection timeout** despite live IPv4 listeners. Fresh source ranges do not consistently remove it, so TCP reuse is not an established cause.

Captured examples: `127.0.0.1:13750 → 127.0.0.1:28824`, server PID 72890, and `127.0.0.1:12500 → 127.0.0.1:28408`, server PID 75823. In both, `lsof` confirms all eight expected IPv4 listeners while the server remains alive; no connection has completed. Exact errors and endpoint/PID evidence are retained in the raw results and local capture logs. These are setup failures, not evidence of a 100K capacity limit. Failed cases are excluded. Completed independent cases preceding a later batch setup failure are retained; no accepted case comes from an expired hold.

Full kernel socket/PCB costs, every coroutine allocation size, alternate TLS negotiations and production throughput remain unmeasured. Measurements describe this host and stack. The prototypes are review inputs for the owning idle-cost quest.

## Reproduce

From this branch's worktree:

```sh
quiet check
quiet light c7-build 10 'Build baseline and private variants' -- sh -c 'cmake -S bench/idle-cost -B .scratch/build -DCMAKE_BUILD_TYPE=Release -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@3 && cmake --build .scratch/build -j4 --target http_idle_cost http_idle_small_read http_idle_small_tls http_idle_small_both http_idle_owners http_idle_owners_small_tls'
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost -keyout .scratch/key.pem -out .scratch/cert.pem -days 7
quiet light c7-payload 2 'Verify8MiB TLS download' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_small_both --counts 1 --download-bytes 8388608 --base-port 27000 --source-port 30000 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive c7-tls10k 5 'TLS10K baseline' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 10000 --base-port 28000 --source-port 30000 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive c7-tls10k-small 5 'TLS10K private small buffers' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_small_both --counts 10000 --base-port 28008 --source-port 40000 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive c7-tls100k 10 'TLS100K baseline' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 100000 --base-port 19032 --source-port 20000 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive c7-tls-owners 5 'TLS allocation files' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_owners --counts 10000 --base-port 28960 --source-port 45000 --tls .scratch/cert.pem .scratch/key.pem
```

For plain transport omit `--tls CERT KEY`. Repeat each comparison three times, reversing arm order in the middle round; use unused listener ranges and record source ranges. Exact ports, binary/driver digests, source stamps, raw baseline/snapshots and elapsed setup times are in the committed JSON. The driver rejects wrong response contents, wrong active counts, TLS/admission failures, impossible source-port bounds and a combined client/server RSS above 16 GiB. It logs and fails a shutdown exceeding 120 s. A failed/shortened run never becomes a smaller successful row.

Local captures are under `.scratch/`: `variant-validation.log`, `memory-round1.log`, `memory-round2.log`, `memory-round3.log`, `memory-missing.log`, `memory-last2.log`, `owner-profiles.log`, `owner-tls.log` and `final-plain100k.log`/`final-tls100k.log`. Validation also passes Ruff formatting/checks and strict Pyrefly for the driver.
