> Status: CURRENT (2026-11-02) the ledger for step H1, http's buffered path on task (http `docs/design/task-handlers.md` revision 2, "Steps" 4).

# task-h1 (http)

- Design: `docs/design/task-handlers.md` revision 2 (branch `design/task-handlers`, http:694fb47), D1-D5, D7-D11, the gate rows, step 4; reviews `docs/reviews/task-handlers-rev2-design.md` (APPROVE WITH CONDITIONS: H1 may start against task C2 and R's interfaces), `docs/reviews/task-handlers-design.md`; the ledger `docs/status/task-handlers.md` (K1-K10; the lead's ruling on the four teardown phases).
- Branch / worktree: `phase/task-h1` at `~/code/Kronuz/http-wt/task-h1`, from `origin/phase/task-handlers-s1` (http:03467b1, S1's counting and S2's pin).
- Pins: task main task:6558c2e (C3, the B1 pin; `CMakeLists.txt:60`, the one `FetchContent_Declare(task ...)` line, declared before reactor so reactor builds against it too), reactor main reactor:2fd30c8 (step R after its fix round: bounded `stop()`, each phase running the earlier ones; `CMakeLists.txt:72`).
- State: fix round for the code review done (CR1-CR4); CI in the Log; ready for the re-check

## Done criteria (from the design's H1 row and K1-K10 as they apply; tick only with evidence)

- [x] D1 handler and router API, the factories and TLS on `task::task` (one `ConnectionFactory`, one `tls_connection_factory`): `http_handler.h:186`, `http_router.h:120`, `http_asio.h:1050`, `http_asio_tls.h:113`
- [x] D2 the handler is a child task of the connection; no post, no driver, no shared block: `http_asio.h:998` (buffered), `:937` (streamed, D6 minimum); mutant D2-post (a `task::yield()` before the handler) killed by `handler_count` (a)
- [x] D3/D4 the session token reaches only the next request's reads and the TLS handshake; timeouts are `task::deadline`s linked to it: `http_asio.h:858` (`bounded(until, stop)`), `http_asio_tls.h` handshake deadline; mutants D3-idle (read without the session token: coroutine_test hangs, rc 124) and D4-timedout (no timed_out mapping: the new streamed-timeout test in `timeouts_test`) killed
- [x] D5 `Request::cancel_token()` and `cancel(reason)` (`http_message.h:144,149`), lazy watch (`http_asio.h:993`); `cancelled()`, `cancel_reason()`, `on_cancel()` and `CancelReason` gone; mutants D5-eager (`handler_count` (a)/(f)) and D5-engage (coroutine_test [C]) killed
- [x] D7 deferred and owned writes: a `task::cancel_source` per reply control (`http_asio.h:149`, `http_owned_stream_asio.h:65`); mutants D7-deferred and D7-owned killed by the new 2 s shutdown bounds in `deferred_asio_test` and `owned_stream_asio_test` (both survived before those tests)
- [x] D8 `task::cancelled` while the request is cancelled, unanswered: 503 + close (`http_asio.h:679`); mutant D8-503 killed (T4 (d), (e))
- [x] D9 affinity assert in Debug (`http_asio.h:279`); mutant D9-affine killed (T4 (h) death test)
- [x] D10 pins: task C3 and reactor R, `task::asio` linked (`CMakeLists.txt:60,82`)
- [x] Teardown ruling: `HttpAsioService::stop()`, `close()`, `destroy_roots()` (`http_asio.h:1123-1125`), each running the earlier ones; mutants X-close and X-destroy killed (coroutine_test [X], task's `destroy_roots` assert)
- [x] K1 (http's share) no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http's headers, tests and examples: `git grep -nE 'asio::awaitable|use_awaitable|awaitable_operators|co_spawn' -- '*.h' '*.cc' ':!bench'` is empty (the raw-Asio comparator moved to `bench/bench_asio.cc`)
- [x] K2 every ctest at the base passes, none removed: 12 base tests plus `handler_count`, 13/13 (Debug, macOS, task C3)
- [x] K4 T4 (a)-(h) without the streamed cases, each failing with its decision mutated (above; (a) and (f) are `handler_count_test`, (b)-(h) and [X] are `coroutine_test`)
- [x] K5 ASan+UBSan and TSan separate builds clean: macOS Homebrew LLVM 23 (`-stdlib++-isystem`, zlib headers copied out of the SDK) 13/13 each, 0 reports; Linux (lease) g++ 13 Debug ASan+UBSan and clang 18 TSan 13/13 each, 0 reports; CI linux-asan-ubsan and linux-tsan green
- [x] K7 allocations and io handlers per request at most A (S1), counting build: see "Counts" (table below: B1 on task C3, reactor 4f5a0b5; rerun at reactor 2fd30c8 in the fix round: same B1 counts)
- [x] K9 zero live task frames after the teardown phases: T4 (d) "no task frame outlives the service (0 left)", [X] "after destroy_roots() on both, no task frame is left (0)"
- [x] K10 CI green on every job (run 41585840133 at http:f0f293b: linux gcc/clang/arm64, asan-ubsan, tsan, macOS arm64/x86_64, FreeBSD)
- [x] Consumer break list (below)

## Mutants (`.scratch/mutants/`, each built and run, the source restored after)

| Mutant | Change | Result |
|---|---|---|
| D2-post | `co_await task::yield()` before the handler | killed: handler_count (a) |
| D5-eager | the watch armed eagerly | killed: handler_count (a), (f) |
| D5-engage | `cancel_token()` does not engage the watch | killed: coroutine_test [C] |
| D8-503 | never 503 | killed: coroutine_test T4 (d) |
| D9-affine | empty `affine()` | killed: coroutine_test T4 (h) |
| X-destroy | `destroy_roots()` skips `close()` | killed: abort in task's `destroy_roots` |
| X-close | `close()` skips `stop()` | killed: same |
| D3-idle | idle read without the session token | killed: coroutine_test times out |
| D4-timedout | no timed_out mapping | survived, then killed by the new streamed-timeout test |
| D7-owned, D7-deferred | no write-token request on Closing | survived, then killed by the new shutdown bounds |

## Counts (Release, `-DHTTP_BENCH_COUNT_ALLOCS=ON`, macOS, R=2, 3 s per row; `.scratch/counts/`)

A is S1 (http:03467b1) built from a `git archive`; B1 is this branch on task C3. Counts only, no timing.

| kind | conns | A allocs/req | B1 allocs/req | A handlers/req | B1 handlers/req |
|---|---|---|---|---|---|
| http | 1 / 8 / 64 | 3.00 / 3.00 / 3.01 | 3.00 / 3.00 / 3.00 | 1.00 | 1.00 |
| httpr | 1 / 8 / 64 | 7.00 / 7.26 / 8.27 | 5.00 / 5.03 / 5.49 | 2.00 | 2.00 |
| httpt | 1 / 8 / 64 | 22.00 / 22.25 / 22.94 | 4.00 / 4.00 / 4.00 | 2.00 | 2.00 |

The counting build leaves task's frame pool on, as A's `asio::awaitable` frames stay on Asio's frame
recycling (`ASIO_DISABLE_SMALL_BLOCK_RECYCLING` does not reach `awaitable_frame_tag`). With the pool
off (`TASK_FRAME_POOL=0`), `http` counted 4.00: a call-site probe (`.scratch/allocprobe/`) shows A's
three (a header field, the response header vector, the recv op) and B's same three plus the handler's
frame, which A's build never counted. `handler_count_test` pins handlers per request against a raw
reactor task session: at C3, 1.000 for the raw session, /flat and /deep, 3.000 when the handler asks (2.000 and 4.000 at C2, where every completion was posted).

## Consumer break list (grep at each repository's origin head, fetched 2026-11-02; code review CR4)

Each consumer breaks when it moves its http pin to this branch; none moves in H1. Grepped for `asio::awaitable|use_awaitable|co_spawn|tls_connection_factory_timed|CancelReason|on_cancel(|cancelled()|async_read(`.

- http-log (origin/main http-log:431b878, pins http:679c0f8): `http_log.h:371` `AccessLog::handle` returns `asio::awaitable<void>` (becomes `task::task<void>`); `examples/demo.cc:5,8,14` (`:14` the handler, `:5,:8` the `co_spawn` drive); `test/completion_test.cc:11,14,21`; `test/test.cc:35,38,44,55,244` (handlers `:44,:55,:244`; `co_spawn` drives `:35,:38`); `test/completion_test.cc:95` constructs `HttpAsioService(handler, 1, 16)` (still valid).
- prism (origin/main prism:da169a9, pins http:d76c5ef): the route lambdas `src/main.cc:186,197,207,217,224,229` (each becomes `task::task<void>`), `:234` `DemoApp::handle`, `:324` `tls_connection_factory_timed` (gone: `tls_connection_factory`, same arguments plus the failure hook).
- Detent (github/main Detent:cce3602; the `origin` remote is a stale local path; pins an older http whose handler is `void handle(const Request&, ...)`): `src/detent_authority/service.hh:55`, `include/detent_relay/service.hh:40`, `src/detentd/detentd.hh:118` handlers and `src/detentd/detentd.cc:51-63` routes; `tls_connection_factory_timed` at `src/detent_relay/host.cc:244`, `src/detentd/main.cc:261`, `tests/detent_authority/test_distribution_service.cc:149`, `tests/detent_authority/test_transport_https.cc:55,119,166`, `tests/detent_delivery/bootstrap_tls_fixture.hh:48`, `tests/detent_delivery/relay_network_fixture.hh:220`. `tests/detent_authority/test_transport_https.cc:136` calls `tls_connection_factory` with 4 arguments, which still compiles. Constructors such as `HttpAsioService(app, 1, 0, 16, factory, 4)` belong to the earlier-http gap, which is not H1's.
- cluster (origin/main cluster:428d57d, pins reactor:ade618d): no http use; nothing breaks from http. It breaks from reactor's Session change, step R's list.
- Xapiand has two heads, and which one Xapiand migrates from is the Xapiand lead's to say:
  - origin/master Xapiand:3fe13a2fb (Oct 28, 312 commits behind local): the handler is synchronous (`src/server/search_service.h:49`); the factory `tls_connection_factory_timed` is at `src/manager.cc:1059`.
  - local master Xapiand:88892b4cb (unpushed): `src/server/search_service.h:54` `SearchService::handle`; `src/server/search_views.cc:3075` (the definition), `:3085` `hreq.on_cancel(... http::CancelReason)` (becomes `cancel_token()` with a `task::cancel_callback`), `:2951` `BodyStream::async_read` (breaks in H2); `src/manager.cc:934` `tls_connection_factory_timed`; `src/server/host_asio.h:131-142` `Run::await` (T1's `async_initiate` over a task; design "What Xapiand changes", item 2).

## Fix round (code review CR1-CR4)

- CR2 (D9 on the loop thread): `AsioResponseWriter::affine()` (`http_asio.h:280`) also accepts no current task while `reactor_->on_loop()` holds, which covers a cancel callback that runs from a raw Asio handler (the watch completion, the reactor's abort walk). Tests: `coroutine_test` `/cb-write` + [T] (h), hang-up and `stop()` cases (a callback sets a header and wakes the handler; the 200 carries `X-Cancelled`); the death tests stay: `/affine-body` (a body moved to another scheduler) and the new `/affine-thread` (a raw thread, no task, off the loop). Mutants: M1 old assertion: abort (rc 134) in the new tests; M2 `!task::current()` without `on_loop()`: killed by the `/affine-thread` death test.
- CR3: `bench_asio.cc` moved to `bench/` (CMake and AGENTS.md updated; the T1 grep is empty); `Request::scheduler()` (`http_message.h`, a forward-declared `task::asio_scheduler&`, set by the transport at `http_asio.h:874,984`, throws `std::logic_error` with no transport) replaces `this_loop(service)` in every handler (`test_loop.h` no longer has it). Test: `/scheduler` in `coroutine_test` (same scheduler as the reactor's, a timer and a `task::deadline` that ends a 30 s timer on it) and the detached-request throw. Mutants: M3 streamed path not setting it, M3b buffered path not setting it: killed (19 failures for M3b).
- CR4: the consumer break list above is regrepped at the origin heads the review names.
- CR1: the reactor pin moves to reactor:2fd30c8 (`CMakeLists.txt:72`); no http code changed for its API. Test: `coroutine_test` [T] (d) `/ignore` (a handler with no token on a 1 h timer, grace 200 ms): `stop()` returns in 419 ms locally (the test's bound is 2 x grace + 300 ms: macos-x86_64 CI measured 502 ms against 2 x grace + 100 ms = 500; the probe keeps + 100 ms on the Linux lease, 401 ms), `close()` and `destroy_roots()` leave 0 live frames. Against the old pin reactor:4f5a0b5 (`REACTOR_INCLUDE_DIR` on a `git archive`), `coroutine_test` does not finish (rc 124 at 60 s, stopped in a `stop()` before it reaches (d)'s new case: the run's last line is the [H] chunked case). Probes (Linux lease, `.scratch/review-h1/run-review.sh`, all scenarios): `ignore` `stop()` 401 ms, 0 frames; `hop-b` 0 frames; `cbwrite`, `cbwrite-stop` and the other nine pass; suite 13/13; ASan+UBSan (g++ 13 Debug) and TSan (clang 18 Debug) both clean, 0 reports.
- Counts rerun at reactor:2fd30c8 (macOS, R=2, 3 s per row, counting build, same procedure): B1 allocs/req http 3.00 / 3.00 / 3.00, httpr 5.00 / 5.02 / 5.49, httpt 4.00 / 4.00 / 4.00 at C=1 / 8 / 64; A (S1) 3.00 / 3.00 / 3.00, 7.00 / 7.23 / 8.29, 22.00 / 22.23 / 22.85; handlers/req equal (1.00, 2.00, 2.00). B1 <= A on every row; same as before the pin move.
- CI at 5a2dd3a: every job green except macos-arm64, where `timeouts` failed "a slow but moving body completes" (a timing case; it passed on the same commit's x86_64 job, on the prior commit, and 13/13 locally after the pin move). The same commit's later push (reactor pin, run 41591547293) passed macos-arm64; its only failure was macos-x86_64's 502 ms against the 500 ms ignore bound above.

## Done
- The port (http:3ed0010, 17de113, 74f931a, f82c6ba), the C3 pin (c5e0354), the D4/D7 tests (f0f293b), the counting build's frame treatment (0085e1b). See Log.

## Doing
- Nothing running; in review.

## Next
1. Re-check of CR1-CR4.
2. H2: the streamed path (D6); this branch carries only the minimum the buffered loop needs to compile.

## Blocked on
- Nothing.

## Log
- 2026-11-02: worktree and branch from http:03467b1; pins moved (task:d3d94f3, reactor:4f5a0b5); headers, tests, examples and the bench ported to task.
- 2026-11-02: Debug suite 13/13 on C2; mutants run, D4 and D7 survived and gained tests; pin moved to C3 (task:6558c2e) on the lead's word, suite 13/13; macOS ASan+UBSan and TSan 13/13, 0 reports; Linux g++ ASan+UBSan and clang TSan on the lease 13/13, 0 reports (`remote.sh` failed: `lease list` shows two detent-build leases, so its IP lookup yields two lines; ran against the provisioned one by hand); counts A vs B1 recorded; CI run 41585840133 green on every job at f0f293b.

## Lead rulings (2026-11-03)
- `examples/bench_asio.cc` (raw-Asio comparator) moves under `bench/` with the other measurement code, so K1 holds with no new exception.
- A handler reaches its loop through the request: `Request` exposes the connection's reactor scheduler as `task::asio_scheduler& scheduler()` (its `executor()` gives the loop's executor for timers, sockets and `task::deadline`), replacing `co_await asio::this_coro::executor`. One way, explicit, and it is the scheduler D9's affinity rule already names. Lands in the H1 fix round, with a test.
- 2026-11-03: code review fix round: CR2 (affine on the loop thread), CR3 (`bench_asio` under `bench/`, `Request::scheduler()`), CR4 (break list) at http:5a2dd3a; CR1 on reactor:2fd30c8 with T4 (d)'s ignoring handler. Suites and probes clean under ASan+UBSan and TSan; counts rerun.
- 2026-11-03: CI run 41592301358 at the head of the fix round: every job green (linux gcc/clang/arm64, asan-ubsan, tsan, macOS arm64/x86_64, FreeBSD).
