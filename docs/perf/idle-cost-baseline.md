# Idle connection cost and reduction experiments

At 10K idle TLS connections, private buffer experiments reduce allocated bytes by **74.60%** and macOS charged footprint by **44.82%**. The TLS staging/BIO change alone gives **43.89%** lower charged footprint; shrinking only the HTTP scratch gives **0.94%**. These are measured memory results, not throughput claims or a landed product change.

All six 10K arms have three independent completed samples. Both 100K baselines now complete, one sample each. C7 remains partial because exact kernel socket/PCB and smaller coroutine-frame allocation sizes are unavailable; the 100K owner detail is extrapolated from 10K profiles, not measured independently. No additional 100K runs are planned on this host because their teardown leaves a large kernel socket backlog.

## Configuration and evidence

Product baseline: `http:09b644d`. All compared binaries embed `http:ff8cc93`; executable and driver SHA-256 digests are retained in [the raw results](../../bench/idle-cost/results-2026-11-14.json). Native x86_64 macOS 15.8.1, 32 GiB RAM, Apple clang 17, Release `-O3 -DNDEBUG`, runtime OpenSSL 3.6.5, pinned Asio 1.36. The existing transitive include order selects `/usr/local/include/openssl` headers (4.1.0) while linking the 3.6.5 library. All arms hold that same configuration; these local results are not a cross-version portability claim.

The server runs eight listeners/reactors. A separate client process establishes every connection, completes one GET/`ok` response, holds the connections open, waits two seconds and takes three snapshots. The 100K TLS run uses eight client threads with a lock around bind/connect; TLS handshakes and responses overlap. Every accepted sample asserts the exact requested active/served count, no admission/TLS failure, and `SSL_MODE_RELEASE_BUFFERS` observed on every TLS connection. Defaults disable HTTP idle/header/inactivity/request timeouts. All measurements use `quiet exclusive`; builds and payload checks use `quiet light`, at most four build jobs. No product header or fetched dependency checkout changes.

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
| TLS baseline | 1 | 204,456.593 | 96,504.259 | 19,552.215 | 63,601.192 |

Each scale row has one independent sample and no A/A spread, so neither is an optimization acceptance result. The TLS row's three idle snapshots agree exactly and assert 100,000 served/active connections, 100,000 release-buffer observations, and zero rejects or TLS failures. Setup and snapshots finish in 89.574 s using eight client workers; the exclusive hold exits 0 after clean process shutdown. The TLS server has 7,696,121,856 compressed bytes at capture. Charged footprint is 9,652,690,944 bytes and RSS is only 1,959,890,944 bytes: compression makes RSS unsuitable as a stand-alone cost number. The 20,446,365,520-byte live allocator count is allocated capacity, not additional resident memory.

The previous serial 100K TLS setup reached only 73,512 established sockets at 7:51, then exited 124 at the ten-minute deadline without a valid snapshot. That miss remains in the raw results. An earlier completed TLS snapshot ended with signal 9 during cleanup; its old driver had a 15 s hard-kill branch, but the log cannot distinguish that branch from an OS kill. It is excluded from accepted scale results. The current driver permits 120 s for shutdown and logs its own kill explicitly.

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

At 100K TLS, the measured OpenSSL total is 63,601.192 B/conn. The allocation-file distribution above, the two 17,408-byte Asio staging vectors, the 65,536-byte HTTP scratch, and the object sizes are **10K-derived owner estimates** at this scale. No allocation-file or coroutine-size profile was taken at 100K, and their projected sums must not be presented as exact 100K accounting.

## Concrete controls and prototypes

1. OpenSSL release-buffers mode is already enabled by pinned Asio `engine.ipp:47-50`. Runtime mode is 23 (`0x17`) and every measured TLS connection includes the release bit. Setting the same flag on the context again provides no reduction.
2. `http_idle_small_read` builds against generated private HTTP headers with only the 64 KiB scratch changed to 4 KiB. This yields a large allocation-budget reduction but a small physical-footprint reduction for this workload.
3. `http_idle_small_tls` builds against generated private Asio copies: `stream_core.hpp:34,41,43` staging changes 17 KiB→2 KiB and `engine.ipp:54,72` constructs each BIO half with 2,048 bytes instead of the default. `http_idle_small_both` combines the two experiments.

CMake refuses to generate an experiment if its expected source statement is absent. Original product headers and fetched dependencies remain unchanged. Asio `put_input` handles partial BIO writes by returning the unconsumed suffix; `get_output` drains arbitrary available output space. Smaller staging does not impose a smaller TLS protocol-record limit.

All four binaries pass 8 MiB uploads (length plus FNV-1a digest) and exact 8 MiB downloads over both plain and TLS: 16 transfer checks. The two owner binaries also pass eight-connection accounting smoke checks. These transfers exercise TLS records larger than the small buffers and output backpressure/partial-BIO paths. They establish payload correctness, not throughput or CPU cost. Before production adoption, compare bulk transfer CPU/throughput and the existing transport/cancellation/security suites with matched headers and libraries.

Recommendation: prioritize configurable smaller Asio staging/BIO capacity. It accounts for almost all measured TLS physical-footprint improvement without changing cryptographic policy. Treat the HTTP scratch change as an allocation-budget tradeoff until bulk CPU/throughput is measured. A readiness-first TLS read remains a separate hypothesis: Asio may retain encrypted bytes in `stream_core::input_`, so a public socket-readiness wrapper alone can stall buffered data. Its memory benefit is not established by this experiment.

## Adoption path for the owning quest

The library controls the 65,536-byte HTTP coroutine scratch directly. The TLS buffers live in
upstream Asio 1.36, fetched from `chriskohlhoff/asio` by this repository. Its `ssl::stream`
constructors take a context or native SSL handle, and its internal `stream_core` allocates two
fixed 17 KiB vectors; the BIO pair also uses internal defaults. The HTTP call site cannot set
these capacities through that interface. A production 2 KiB setting needs an upstream Asio
change or a maintained fork, followed by a pin update here. Replacing the TLS transport is a
larger alternative. The private header copies prove the buffer-size mechanism on this workload;
they are an experiment, not a configuration API to ship.

1. Expose one TLS staging/BIO capacity on Asio stream construction and use 2,048 bytes for HTTP idle connections. Keep the two staging vectors and both BIO halves in sync through that setting. Preserve partial-write and drain behavior for records larger than the capacity; the 8 MiB transfers already exercise it functionally.
2. Evaluate replacing the HTTP coroutine's 64 KiB scratch with 4 KiB. The 10K prototype saves about 91.6 KiB of allocator capacity per plain connection but only 1.55 KiB of charged footprint; throughput and CPU determine whether this cut earns a product change.
3. Measure both changes against the same baseline for sustained bulk traffic, handshakes, idle 10K A/A and a 100K Linux scale run with owner profiling. The 10K TLS charged-footprint delta would project to about 4.03 GiB at 100K if it scaled linearly; this is a sizing hypothesis, not a measured 100K saving. Report CPU per request and host power or cost only when those are measured.

The 4,272-byte read-ahead object is a later candidate. Socket/PCB memory is under the kernel's
control; this code can change connection count, socket options and lifetime, but the native
socket profile here does not expose allocated PCB bytes. A smaller userspace buffer therefore
does not establish a kernel-memory reduction.

## Setup failures and limits

The original implicit client-port allocator exhausted its range (`errno 49`). Explicit source binding with `SO_REUSEADDR` and separate destination ranges permits successful 100K setup. Later runs also encounter a distinct **first-connection timeout** despite live IPv4 listeners. Fresh source ranges do not consistently remove it, so TCP reuse is not an established cause.

The eight-worker driver first failed at 146 connections because macOS rejects concurrent binds of one source port (`errno 48`). Binding to `127.0.0.2` and later loopback addresses failed with `errno 49` because those aliases are not configured on this host. Serializing only bind/connect permits concurrent handshakes and completes the 100K TLS sample. Immediately after both processes exited, `netstat` showed 183,795 FIN_WAIT_1 and 2,789 CLOSING sockets on the benchmark ports; the exclusive hold had exited 0. This large kernel teardown backlog violates the task's near-zero cleanup criterion even though the memory snapshot is valid. No further 100K runs should use this macOS setup; configure multiple loopback aliases with bounded staggered teardown, or use the existing Linux VM. Verify the states have decayed before other network-sensitive work.

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
quiet exclusive c7-tls100k-parallel 10 'TLS100K baseline with concurrent client handshakes' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_cost --counts 100000 --client-workers 8 --base-port 19100 --source-port 21000 --tls .scratch/cert.pem .scratch/key.pem
quiet exclusive c7-tls-owners 5 'TLS allocation files' -- python3 bench/idle-cost/run.py .scratch/build/http_idle_owners --counts 10000 --base-port 28960 --source-port 45000 --tls .scratch/cert.pem .scratch/key.pem
```

For plain transport omit `--tls CERT KEY`. Repeat each comparison three times, reversing arm order in the middle round; use unused listener ranges and record source ranges. Exact ports, binary/driver digests, source stamps, raw baseline/snapshots and elapsed setup times are in the committed JSON. The driver rejects wrong response contents, wrong active counts, TLS/admission failures, impossible source-port bounds and a combined client/server RSS above 16 GiB. It logs and fails a shutdown exceeding 120 s. A failed/shortened run never becomes a smaller successful row.

Local captures are under `.scratch/`: `variant-validation.log`, `memory-round1.log`, `memory-round2.log`, `memory-round3.log`, `memory-missing.log`, `memory-last2.log`, `owner-profiles.log`, `owner-tls.log`, `final-plain100k.log`, `final-tls100k.log` and `parallel-tls100k.log`. Validation also passes Ruff formatting/checks and strict Pyrefly for the driver.
