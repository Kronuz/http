> Status: CURRENT (2026-11-02) the ledger for step H1, http's buffered path on task (http `docs/design/task-handlers.md` revision 2, "Steps" 4).

# task-h1 (http)

- Design: `docs/design/task-handlers.md` revision 2 (branch `design/task-handlers`, http:694fb47), D1-D5, D7-D11, the gate rows, step 4; reviews `docs/reviews/task-handlers-rev2-design.md` (APPROVE WITH CONDITIONS: H1 may start against task C2 and R's interfaces), `docs/reviews/task-handlers-design.md`; the ledger `docs/status/task-handlers.md` (K1-K10; the lead's ruling on the four teardown phases).
- Branch / worktree: `phase/task-h1` at `~/code/Kronuz/http-wt/task-h1`, from `origin/phase/task-handlers-s1` (http:03467b1, S1's counting and S2's pin).
- Pins: task main task:6558c2e (C3, the B1 pin; `CMakeLists.txt:60`, the one `FetchContent_Declare(task ...)` line, declared before reactor so reactor builds against it too), reactor `phase/task-sessions` reactor:4f5a0b5 (step R; its fix round, with bounded `stop()` and the phase chaining, moves the pin when it lands).
- State: in review (functional work done; the reactor pin moves when R's fix round lands)

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
- [ ] K1 (http's share) no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http's headers, tests and examples: all gone except `examples/bench_asio.cc`, the raw-Asio comparator, which sits in `examples/`, outside T1's `:!bench` exception (question for the lead: move it to `bench/` or except it)
- [x] K2 every ctest at the base passes, none removed: 12 base tests plus `handler_count`, 13/13 (Debug, macOS, task C3)
- [x] K4 T4 (a)-(h) without the streamed cases, each failing with its decision mutated (above; (a) and (f) are `handler_count_test`, (b)-(h) and [X] are `coroutine_test`)
- [x] K5 ASan+UBSan and TSan separate builds clean: macOS Homebrew LLVM 23 (`-stdlib++-isystem`, zlib headers copied out of the SDK) 13/13 each, 0 reports; Linux (lease) g++ 13 Debug ASan+UBSan and clang 18 TSan 13/13 each, 0 reports; CI linux-asan-ubsan and linux-tsan green
- [x] K7 allocations and io handlers per request at most A (S1), counting build: see "Counts" (B1 on task C3, reactor 4f5a0b5; re-measure when R's fix round lands)
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

## Consumer break list (grep at each repository's local HEAD, 2026-11-02)

Each consumer breaks when it moves its http pin to this branch; none moves in H1.

- http-log (http-log:d2315b8, pins http:fce24c6): `http_log.h:371` `AccessLog::handle` returns `asio::awaitable<void>` (becomes `task::task<void>`); `examples/demo.cc:14`, `test/completion_test.cc:21`, `test/test.cc:44,55,244` handlers; `examples/demo.cc:8`, `test/completion_test.cc:14`, `test/test.cc:38` drive handlers with `asio::co_spawn`; `test/completion_test.cc:95` constructs `HttpAsioService(handler, 1, 16)` (still valid).
- prism (prism:bdaf3e6, pins http:fce24c6): `src/main.cc:234` `DemoApp::handle` and its router routes; `src/main.cc:324` `tls_connection_factory_timed` (gone: `tls_connection_factory`, same arguments plus the failure hook).
- Detent (Detent:bd7c76a, pins an older http whose handler is `void handle(const Request&, ...)`): `src/detent_authority/service.hh:55`, `include/detent_relay/service.hh:40`, `src/detentd/detentd.hh:118` handlers and `src/detentd/detentd.cc:51-63` routes; `tls_connection_factory_timed` at `src/detent_relay/host.cc:244`, `src/detentd/main.cc:261`, `tests/detent_authority/test_distribution_service.cc:149`, `tests/detent_authority/test_transport_https.cc:55,119,166`, `tests/detent_delivery/bootstrap_tls_fixture.hh:48`, `tests/detent_delivery/relay_network_fixture.hh:220`. Its gap to this branch includes earlier http changes, not only H1's.
- cluster (cluster:f053a82): no http use (it pins reactor only); nothing breaks from http. It breaks from reactor's Session change, step R's list.
- Xapiand (Xapiand:88892b4cb): `src/server/search_service.h:54` `SearchService::handle`; `src/server/search_views.cc:3085` `hreq.on_cancel(... http::CancelReason)` (becomes `cancel_token()` with a `task::cancel_callback`); `src/manager.cc:934` `tls_connection_factory_timed`.

## Done
- The port (http:3ed0010, 17de113, 74f931a, f82c6ba), the C3 pin (c5e0354), the D4/D7 tests (f0f293b), the counting build's frame treatment (0085e1b). See Log.

## Doing
- Nothing running; in review.

## Next
1. Code review of this branch.
2. When reactor's fix round lands (bounded `stop()`, each phase running the earlier ones, the +2 allocations per connection fixed): move the reactor pin, add T4 (d)'s handler that ignores cancellation (bounded by the grace), re-measure the counts.
3. H2: the streamed path (D6); this branch carries only the minimum the buffered loop needs to compile.

## Blocked on
- Lead: `examples/bench_asio.cc` (raw-Asio comparator in `examples/`, outside T1's `:!bench` exception): move to `bench/` or except it (K1).
- Design gap, for the designer or the lead: a handler has no way to reach its loop's executor (formerly `co_await asio::this_coro::executor`); tests use `this_loop(service)` (`test/test_loop.h`). An app wanting a timer must reach `service.reactor(i)`; `task::current()` gives the scheduler but not its `asio_scheduler&`.

## Log
- 2026-11-02: worktree and branch from http:03467b1; pins moved (task:d3d94f3, reactor:4f5a0b5); headers, tests, examples and the bench ported to task.
- 2026-11-02: Debug suite 13/13 on C2; mutants run, D4 and D7 survived and gained tests; pin moved to C3 (task:6558c2e) on the lead's word, suite 13/13; macOS ASan+UBSan and TSan 13/13, 0 reports; Linux g++ ASan+UBSan and clang TSan on the lease 13/13, 0 reports (`remote.sh` failed: `lease list` shows two detent-build leases, so its IP lookup yields two lines; ran against the provisioned one by hand); counts A vs B1 recorded; CI run 41585840133 green on every job at f0f293b.

## Lead rulings (2026-11-03)
- `examples/bench_asio.cc` (raw-Asio comparator) moves under `bench/` with the other measurement code, so K1 holds with no new exception.
- A handler reaches its loop through the request: `Request` exposes the connection's reactor scheduler as `task::asio_scheduler& scheduler()` (its `executor()` gives the loop's executor for timers, sockets and `task::deadline`), replacing `co_await asio::this_coro::executor`. One way, explicit, and it is the scheduler D9's affinity rule already names. Lands in the H1 fix round, with a test.
