> Status: CURRENT (2026-10-30) the L3 ledger for Phase 7c (Xapiand `docs/design/phase7c.md` D2, D3, D12, D16, section 5).

# phase7c-l3 (http, reactor, http-log, prism)

- Design: Xapiand `docs/design/phase7c.md` D2, D3, D12, D16 and section 5's L3; reviews: Xapiand
  `docs/reviews/phase7c-design.md` R3, R6, C14, C17; owner ruling Q3 (strata
  `docs/status/xapiand-on-strata.md`): the offload path is removed from http entirely.
- Branches / worktrees (all `phase7c/l3-http` unless noted):
  - reactor: `~/code/Kronuz/reactor-wt/l3-http`
  - http: `~/code/Kronuz/http-wt/l3-http`
  - http-log: `~/code/Kronuz/http-log-wt/l3-http`
  - prism: `phase7c/l3-prism` at `~/code/Kronuz/prism-wt/l3-prism`
- State: implementing (library work, tests, mutants, ASan+UBSan done; TSan and preflight under way)
- Done criteria: the lead's list was the task message; they are the checkboxes below.
  - [x] Offload removed: worker pool, `should_offload`, body hand-off, `ChannelBodyReader`, `Reactor::pool` (reactor 67e22a6, http bda9b5a)
  - [x] `HttpHandler::handle` is a coroutine; `Router` routes are coroutines
  - [x] `BodyStream` read with `co_await` on the loop (asio_test [K] one thread; coroutine_test [B])
  - [x] Per-request disconnect callback (`Request::on_cancel`); both C17 rules stated (`docs/mechanisms/coroutine-handlers.md`) and tested (coroutine_test [C])
  - [x] `reactor::AdmissionGate`, beside the application's own cap (`RequestRejected(503, ..., retry_after)`), each with its own 503 (coroutine_test [D])
  - [x] Buffered-body cap with 413 (coroutine_test [E]); parser fuzz target
  - [x] Shutdown order (D16) and its bound (C14, the grace, reactor test [F], coroutine_test [F]); `Reactor::track` O(1) (reactor test [L])
  - [x] prism (and http-log, which it wraps) migrated; prism builds and its demo serves
  - [ ] ASan+UBSan done (reactor 9/9, http 10/10); TSan; preflight
  - [x] Xapiand HTTP-layer check against these pins: configures; compile errors listed below for X3
- Done: see the log.
- Doing: TSan builds; preflight.
- Next: push the final heads, record CI jobs, hand to the code reviewer.
- Blocked on: nothing.
- Findings folded in: none yet (not reviewed).
- Last verified: macOS x86_64, Apple clang Debug: reactor 9/9, http 11/11 ctest (tls_asio included); Homebrew LLVM 23 ASan+UBSan: reactor 9/9, http 10/10 (tls_asio is not built with a local reactor include); libFuzzer 181 s, 1.9M runs, no crash.

## Proof

Mutants (http, one build and run each; a mutant "dies" when a test fails or the run hangs past 100 s):

| Mutant | Dies in |
|---|---|
| EOF no longer cancels a request in flight | coroutine_test [C] x6 |
| a pipelined byte fires the cancel | [C] pipelined-byte case |
| a cancelled request's response is skipped | [C] half-close x2, [F] stop |
| cancellation left enabled while the handler runs | [F] stop (response from a live frame), [F] stalled write |
| gate refusal removed | [D] cap |
| Retry-After dropped | [D] |
| lease not released after the response | [D] keep-alive holds no lease |
| Content-Length cap off by 2x | [E] default cap |
| chunked cap off | [E] chunked |
| default cap 0 | [E] default cap |
| `force()` a no-op | [F] stalled write (hangs) |
| `abort()` a no-op | [F] stop x3 |
| `stopping()` check removed from the keep-alive loop | [F] stop (hangs) |
| BodyStream unbounded | [B] bounded |
| BodyStream drops the failure | [B] failed body |
| late `on_cancel` not run | [B] |

Reactor mutants (reactor test): gate off by one ([G] x5), `abort_all` inline instead of on the loop ([O]),
`force_all` skipped (hang), linear scan in `add` (exceeds the 60 s timeout), no cursor fix in `remove` ([L]).
A mutant removing the drain-time cancel in `ConnectionInput::drain` survived (exit 0): the next `co_await` of a
cancelled coroutine throws, so the code was redundant and was removed.

Revert checks: each new test is new, so "fails without the fix" is the mutant above that undoes that fix.

## What X3 must change (Xapiand `d71cf7a97` with pins reactor 67e22a6, http c05b4d3, http-log c7772ab)

CMake configures. Compiling `src/server/search_service.o`, `search_views.o` and `manager.o` stops at:

1. `search_service.h:50`: `handle` becomes `asio::awaitable<void> handle(http::Request&, http::ResponseWriter&)`;
   each view's task is wrapped in `task::as_awaitable`. `SearchService` is abstract until then (`manager.cc:185`).
2. `search_service.h:66`, `search_service.cc:28`: delete `should_offload`; the OPTIONS/QUIT/OPEN/CLOSE exemption goes.
3. `search_views.cc:694, :2849, :2853, :2978`: `Request::body_reader` is `Request::body_stream`, read with
   `task::await_asio<void(asio::error_code)>` (`eof` is the end); RESTORE's `objects_mtx` goes (D3).
4. `manager.cc:814, :821`: `HttpAsioService(handler, reactors, workers, 1000, ...)` is `(handler, reactors, 1000, ...)`;
   `--http-servers`/`workers` are D15.
5. `manager.cc:157`: `reactor(1, 0)` is `reactor()`; `:465`, `:880`: `control_offload` and `join()` use `Reactor::pool()` (D13).
6. `engine/release.h:44-48`: `release_on_pool` uses `Reactor::pool()` (D14: awaited closes).
7. Not compile errors, but required: register a `task::cancel_source` with `request.on_cancel(...)` in `handle` (D3);
   enforce the demoted cap in `create_extension` with `RequestRejected(503, ..., "Retry-After")` (R3); the 16 MiB
   buffered cap is the http default (`set_max_body`), so a `stored` upload above it must use `wants_body_stream` (D9, D12).
   Streamed and sunk bodies are not capped; all other bodies above 16 MiB get 413.
8. The shutdown order in `manager.cc` (D16) can call `HttpAsioService::stop()`, which now runs abort, grace
   (`set_shutdown_grace`, 5 s), force, and join; handlers must honor `Request::cancelled()`.

Other consumers of reactor or http still on their own pins (not touched): Detent (http and reactor), consensus
(reactor). cluster only constructs `reactor::UdpServer(handler)`, which is source compatible.

## Log
- 2026-10-30: reactor `67e22a6`: pool removed, `AdmissionGate`, intrusive abort list, `abort_all`/`force_all` on the
  loop, shutdown grace. http `bda9b5a`: coroutine handlers, `BodyStream`, cancel/watch, admission, cap, tests.
