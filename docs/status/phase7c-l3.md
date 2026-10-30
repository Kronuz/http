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
- State: review conditions fixed (L3-B1, L3-B2, L3-N1); waiting for the lead to rerun the review probes and land
- Done criteria: the lead's list was the task message; they are the checkboxes below.
  - [x] Offload removed: worker pool, `should_offload`, body hand-off, `ChannelBodyReader`, `Reactor::pool`
  - [x] `HttpHandler::handle` is a coroutine; `Router` routes are coroutines
  - [x] `BodyStream` read with `co_await` on the loop (asio_test [K] one thread; coroutine_test [B])
  - [x] Per-request disconnect callback (`Request::on_cancel`); both C17 rules stated (`docs/mechanisms/coroutine-handlers.md`) and tested (coroutine_test [C])
  - [x] `reactor::AdmissionGate`, beside the application's own cap (`RequestRejected(503, ..., retry_after)`), each with its own 503 (coroutine_test [D])
  - [x] Buffered-body cap with 413 (coroutine_test [E]); parser fuzz target
  - [x] Shutdown order (D16) and its bound (C14: the grace, reactor test [F], coroutine_test [F]); `Reactor::track` O(1) (reactor test [L])
  - [x] prism (and http-log, which it wraps) migrated; prism builds and its demo serves (smoke script: 8 routes, SIGINT stop)
  - [x] ASan+UBSan and TSan clean at the final heads (reactor 9/9, http 10/10 each, 0 reports)
  - [x] preflight (`cpp-libraries/tools/preflight.sh`): reactor mac-hardened, mac-asan-ubsan, linux, freebsd ok; http mac-hardened, linux, freebsd ok;
        http mac-asan-ubsan fails to build `compressors` under Homebrew LLVM (zlib's SDK `-isystem` shadows libc++'s headers;
        the same 33 errors are in the older preflight log of http `main`), so ASan+UBSan was run by hand with `-stdlib++-isystem` and a zlib include override
  - [x] Xapiand HTTP-layer check against these pins: configures; the compile errors are listed below for X3
- Done: see the log.
- Doing: nothing running.
- Next: the code review; then the lead merges in order reactor, http, http-log, prism and moves their pins to `main`.
- Blocked on: nothing.
- Findings folded in: see "Review fixes" below. Earlier: Self-found and fixed: a request tracked after the force walk was never forced (reactor d442d87);
  `abort_all` waited on heap state (`std::promise`, then a `shared_ptr`) while the accept-pressure test stops the server out of descriptors, where clang's UBSan vptr check cannot look up a new type (linux-asan-ubsan CI); the wait now uses reactor-owned members (reactor 7dcf73c).
- Heads: reactor 76c32b4, http fce24c6 (plus this ledger's docs-only commit), http-log d2315b8, prism bdaf3e6.
- CI at the review heads (reactor 7dcf73c, http 0553e53, http-log 86661b3, prism 51e21bf): all green. At the fix heads above: pushed, CI not watched.
- Last verified: macOS x86_64, Apple clang Debug: reactor 9/9, http 11/11, http-log 2/2; Homebrew LLVM 23 ASan+UBSan and TSan as above; libFuzzer 181 s,
  1.9M runs, no crash; TSan reactor_test 518 runs, no hang (one abort in the older `free_port` test: bind on a port another process took).
- Known cost for the lead to measure (not measured here): after a keep-alive request the one-byte watch read has consumed the first byte of
  the next request, so the parser gets it from `ConnectionInput::read` and the rest takes a second read.

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
| BodyStream drops the failure | [B] failed body; [H] x4 (a real socket cut mid-body) |
| late `on_cancel` not run | [B] |

Reactor mutants (reactor test): gate off by one ([G] x5), `abort_all` inline instead of on the loop ([O]),
`force_all` skipped (hang), linear scan in `add` (exceeds the 60 s timeout), no cursor fix in `remove` ([L]),
no cancellation emitted for idle sessions (hang), `track` after `force_all` not forced ([O]).
A mutant removing the drain-time cancel in `ConnectionInput::drain` survived (exit 0): the next `co_await` of a
cancelled coroutine throws, so the code was redundant and was removed.

Revert checks: each new test is new, so "fails without the fix" is the mutant above that undoes that fix.

## What X3 must change (Xapiand `d71cf7a97` with pins reactor 7dcf73c, http 0553e53, http-log 86661b3)

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

## Review fixes (`docs/reviews/phase7c-l3-code.md`)

| Finding | Fix | Test | Revert / mutant evidence |
|---|---|---|---|
| L3-B1 | `feed()` maps EOF (and a zero-byte read) before the body is complete to `connection_reset`, and cancels the request before failing the stream, so `cancelled()` is true when the reader's read returns | coroutine_test [H]: Content-Length FIN, RST, half-close and chunked cut; the reader's end is not `eof` and `cancelled()` is true; a complete body still ends with `eof` | no eof mapping: [H] fails x3 (FIN, half-close, chunked); cancel left to the later call: [H] fails x4 |
| L3-B2 | `Request::cancel` and a late `on_cancel` drop a throwing callback; the streamed path holds any throw between the `co_spawn` and `handler_done->wait()` in one `try` and rethrows after the wait | coroutine_test [I]: streamed (the request stays admitted while the handler runs, which the unwinding connection frame released early), buffered (survives), late registration | callback catch removed: the process terminates in [I]; guard removed with a throw injected after the cancel: ASan heap-use-after-free in the handler, passes with the guard (injection harness in `.scratch/inject.sh`, not committed) |
| L3-N1 (reactor `76c32b4`) | per-connection accept errors (ECONNABORTED, ECONNRESET, EPROTO, the network errors, EINVAL on macOS) skip the connection; 64 in a row back off; a real listener error ends the loop and is logged on stderr and through `L_EXC` | reactor_test [N]: 1500 reset pending connections against both accept strategies, then the listener serves | per-connection branch removed: [N] fails x2 |
| L3-N5 (partly) | http `CMakeLists.txt` header, http-log `AGENTS.md` (`body_stream`), reactor `test/test.cc` header | | |
| L3-N11 | note on `set_headers_callback` | | |

Follow-ups left open (not one-line changes):

1. L3-N2: a second concurrent `Reactor::abort_all` returns before the first caller's walk has run.
2. L3-N3: D16 lists the acceptors first; `TcpServer::stop` aborts first (safe). Update D16's wording in Xapiand.
3. L3-N4: `Reactor::track` is loop-thread only, checked by `assert`; add a release-mode check or a post, and say so in the Detent/consensus migration notes.
4. L3-N5 (rest): http `docs/http.d2` and `docs/http.svg` still draw the pool and `should_offload`.
5. L3-N6: `Reactor` member order (`io_` should be declared after `gate_` and the mutexes, or documented).
6. L3-N7, N10: what bounds the sum of buffered bodies, and a bound for handlers that never return (architect, X6).
7. L3-N8, N9: documented cancellation limits; the keep-alive extra read is for the lead to measure.

Proof of the fixes (macOS x86_64, Homebrew LLVM 23): ASan+UBSan and TSan clean on reactor (9/9 each) and http (10/10 each, coroutine_test with [H] and [I]);
the review's probes against the fixed code: `probe_truncated` 0 of 4 clean eof, `probe_throwcb` streamed and buffered survive under ASan,
`probe_acceptrst` 2000 reset connects, listener alive. Preflight: reactor mac-hardened, mac-asan-ubsan, linux, freebsd ok; http mac-hardened, linux, freebsd ok;
http-log and prism mac-hardened ok; every mac-asan-ubsan run for http and its dependents fails in the same libc++ header search-path error as before this change.
