> Status: CURRENT (2026-11-02) the ledger for step H1, http's buffered path on task (http `docs/design/task-handlers.md` revision 2, "Steps" 4).

# task-h1 (http)

- Design: `docs/design/task-handlers.md` revision 2 (branch `design/task-handlers`, http:694fb47), D1-D5, D7-D11, the gate rows, step 4; reviews `docs/reviews/task-handlers-rev2-design.md` (APPROVE WITH CONDITIONS: H1 may start against task C2 and R's interfaces), `docs/reviews/task-handlers-design.md`; the ledger `docs/status/task-handlers.md` (K1-K10; the lead's ruling on the four teardown phases).
- Branch / worktree: `phase/task-h1` at `~/code/Kronuz/http-wt/task-h1`, from `origin/phase/task-handlers-s1` (http:03467b1, S1's counting and S2's pin).
- Pins: task main task:d3d94f3 (C2; `CMakeLists.txt`, the one `FetchContent_Declare(task ...)` line, declared before reactor so reactor builds against it too; moves to C3's commit), reactor `phase/task-sessions` reactor:4f5a0b5 (step R, in code review).
- State: implementing

## Done criteria (from the design's H1 row and K1-K10 as they apply; tick only with evidence)

- [ ] D1 handler and router API, the factories and TLS on `task::task` (one `ConnectionFactory`, one `tls_connection_factory`)
- [ ] D2 the handler is a child task of the connection; no post, no driver, no shared block
- [ ] D3/D4 the session token reaches only the next request's reads and the TLS handshake; timeouts are `task::deadline`s linked to it
- [ ] D5 `Request::cancel_token()` and `cancel(reason)`; `cancelled()`, `cancel_reason()`, `on_cancel()` and `CancelReason` gone
- [ ] D7 deferred and owned writes: a `task::cancel_source` per reply control, linked into the write's deadline
- [ ] D8 exceptions: `task::cancelled` while the request is cancelled, unanswered, is 503 + close; otherwise as before
- [ ] D9 affinity: the writer asserts in Debug that it runs on the connection's scheduler
- [ ] D10 pins: task and reactor, `task::asio` linked
- [ ] Teardown ruling: `HttpAsioService::stop()`, `close()`, `destroy_roots()`, destruction; each phase runs the earlier ones not yet run
- [ ] K1 (http's share) no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http's headers, tests and examples (bench comparators excepted)
- [ ] K2 every ctest at the base passes, none removed (T2)
- [ ] K4 T4 (a)-(h) without the streamed cases, each failing with its decision mutated
- [ ] K5 ASan+UBSan and TSan separate builds clean (T5, http)
- [ ] K7 allocations and io handlers per request at most A (S1), counting build, exact numbers
- [ ] K9 zero live task frames after the teardown phases (T4 (d), [X])
- [ ] K10 CI green on every job
- [ ] Consumer break list (http-log, prism, Detent, cluster, Xapiand)

## Consumer break list (grep at each repository's local HEAD, 2026-11-02)

Each consumer breaks when it moves its http pin to this branch; none moves in H1.

- http-log (http-log:d2315b8, pins http:fce24c6): `http_log.h:371` `AccessLog::handle` returns `asio::awaitable<void>` (becomes `task::task<void>`); `examples/demo.cc:14`, `test/completion_test.cc:21`, `test/test.cc:44,55,244` handlers; `examples/demo.cc:8`, `test/completion_test.cc:14`, `test/test.cc:38` drive handlers with `asio::co_spawn`; `test/completion_test.cc:95` constructs `HttpAsioService(handler, 1, 16)` (still valid).
- prism (prism:bdaf3e6, pins http:fce24c6): `src/main.cc:234` `DemoApp::handle` and its router routes; `src/main.cc:324` `tls_connection_factory_timed` (gone: `tls_connection_factory`, same arguments plus the failure hook).
- Detent (Detent:bd7c76a, pins an older http whose handler is `void handle(const Request&, ...)`): `src/detent_authority/service.hh:55`, `include/detent_relay/service.hh:40`, `src/detentd/detentd.hh:118` handlers and `src/detentd/detentd.cc:51-63` routes; `tls_connection_factory_timed` at `src/detent_relay/host.cc:244`, `src/detentd/main.cc:261`, `tests/detent_authority/test_distribution_service.cc:149`, `tests/detent_authority/test_transport_https.cc:55,119,166`, `tests/detent_delivery/bootstrap_tls_fixture.hh:48`, `tests/detent_delivery/relay_network_fixture.hh:220`. Its gap to this branch includes earlier http changes, not only H1's.
- cluster (cluster:f053a82): no http use (it pins reactor only); nothing breaks from http. It breaks from reactor's Session change, step R's list.
- Xapiand (Xapiand:88892b4cb): `src/server/search_service.h:54` `SearchService::handle`; `src/server/search_views.cc:3085` `hreq.on_cancel(... http::CancelReason)` (becomes `cancel_token()` with a `task::cancel_callback`); `src/manager.cc:934` `tls_connection_factory_timed`.

## Done
- See Log.

## Doing
- Building the ported tree (Debug, macOS) and fixing compile errors.

## Next
1. Debug suite green; T4 rows and mutants.
2. ASan+UBSan, TSan builds; the counting build A vs B (C2 pin, and C3's head for B1).
3. Linux build (lease or local), push, CI.

## Blocked on
- nothing yet.

## Log
- 2026-11-02: worktree and branch from http:03467b1; pins moved (task:d3d94f3, reactor:4f5a0b5); headers, tests, examples and the bench ported to task.
