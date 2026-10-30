> Status: CURRENT (2026-10-30) code review of Phase 7c L3 at reactor 7dcf73c, http 0553e53 (b71120b is ledger only), http-log 86661b3, prism 51e21bf.

# Phase 7c L3 code review

Reviewer: independent code reviewer. Scope: reactor `phase7c/l3-http` (4 commits), http (8), http-log (the
`AccessLog` coroutine commit), prism `phase7c/l3-prism`. Design: Xapiand `docs/design/phase7c.md` D2, D3, D16;
`docs/reviews/phase7c-design.md` R3, R6, C14, C17. I did not redo the developer's proof (`coroutine_test`, the 23
mutants, CI). Everything below comes from probes in `.scratch/review-l3/` of this worktree.

## Verdict: APPROVE WITH CONDITIONS

The design is implemented faithfully and most of the risk areas hold under hard probing: pipelining and the
watch-byte path, admission accounting, shutdown with a grace, the intrusive abort list, accept races. Two defects are
real and small. L3-B1 is a wrong answer at the API's main contract (a truncated upload reads as a clean end).
L3-B2 is a memory-safety hole behind a throwing callback. Neither needs a redesign. Land after both are fixed with a
real-socket test each, re-checked by a rechecker (not a new review).

Blockers:

1. L3-B1: a client that closes mid-body reaches the `BodyStream` reader as `asio::error::eof`, a clean end of body.
2. L3-B2: a throwing `Request::on_cancel` callback destroys the connection frame under a running streamed handler
   (heap-use-after-free) and terminates the process on the buffered path.

## Blockers

### L3-B1. A truncated upload is delivered to the reader as a clean `eof`

`http_asio.h:735-736`:

```cpp
catch (const asio::system_error& e) { failure = e.code(); }
if (failure || n == 0) { stream->fail(failure ? failure : asio::error::connection_reset); ...
```

When the peer closes mid-body, `ConnectionInput::read` throws `system_error(asio::error::eof)`, so `failure` is
`eof` and `fail(eof)` is called. `BodyStream::ReadOp` (`http_body_stream.h`) then completes the pending read with
`eof`, which the API documents as "the body ended" (`async_read`'s contract, `coroutine-handlers.md` "Streamed
bodies"). The documented `connection_reset` ("when the client closed mid-body") only occurs on an RST. The ordinary
FIN, a `shutdown(SHUT_WR)` before the last byte, and a chunked body cut after a chunk all look like a complete
body.

The reader cannot disambiguate: the probe records `request.cancelled()` at the moment the read returns and it is
`false`. The reader's wake is posted before the connection coroutine reaches `request.cancel(Disconnect)`
(`http_asio.h:~752`). Writes take no token (D3), so a RESTORE-style consumer in X3 that treats `eof` as the end would
commit a truncated stream and answer 200 to nobody. The old `BodyReader::read` was also ambiguous, but the point of
the new typed errors is to remove that ambiguity, and the docs promise it.

Probe: `.scratch/review-l3/probe_truncated.cc` (ASan and TSan, same result). Four cases against a `/t` streamed route:

```
Content-Length 100000, 1000 sent, close()      -> End of file  [EOF = looks like a clean end]
Content-Length 100000, 1000 sent, RST          -> Connection reset by peer  [failure]
chunked, one chunk sent, close()               -> End of file  [EOF]
Content-Length 100000, 1000 sent, shutdown(WR) -> End of file  [EOF]
```

Also `probe_streamcancel.cc` ("mid-body close"): the handler's `read_err` stays 0.

Why the tests missed it: `coroutine_test` [B] calls `BodyStream::fail(connection_reset)` directly
(`test/coroutine_test.cc:247`); no test closes a real socket mid-body on a streamed route. The ledger's tick for
"BodyStream drops the failure" is killed by that unit test only.

Fix: in `feed()`, map `eof` (and any other error that means "the peer ended before the parser completed") to a
non-eof error, for example `asio::error::connection_reset` or a named `http::error::body_truncated`; keep `timed_out`
and parse errors as they are. Test: a real-socket truncated upload (Content-Length and chunked, FIN and half-close)
where the handler's last read is not `eof` and is not `operation_aborted`; revert the map and it must fail.
`probe_truncated` is that test, ready to adopt.

### L3-B2. A throwing `Request::on_cancel` callback breaks the lifetime guarantee

`Request::cancel` (`http_message.h`, `cancel()`) calls the user callback with no guard. It is called from four
places: the watch (`ConnectionInput::completed -> fire`), `RequestAbort::abort` (inside `Reactor::abort_all`'s
walk), `Request::on_cancel` itself (a late registration), and the connection coroutine directly
(`http_asio.h:~752` `request.cancel(...)` after a failed feed).

1. Streamed path: the direct call at `~752` runs after the handler was `co_spawn`ed and before
   `co_await handler_done->wait()`. An exception unwinds `serve_connection_body`, destroying `request`, `writer`,
   `stream` and `tracked` while the spawned handler is still suspended and holds references to them. ASan:
   heap-use-after-free in the handler's resume (`probe_throwcb`, no argument). That is exactly the invariant D3 R6.3
   states ("frames end only by completing").
2. Buffered path (`probe_throwcb buffered`): the callback runs from an Asio completion handler on the reactor
   thread, the exception leaves `io_context::run()`, and the thread lambda in `TcpServer::start` has no `catch`:
   `libc++abi: terminating due to uncaught exception of type std::runtime_error`. The same holds for a throw inside
   `abort_all`'s walk.

A callback that throws is a programmer error, but this is the transport's memory-safety boundary and the callback is
supplied by a layer (X3's `task::cancel_source`) that this review has not seen.

Fix (small): `Request::cancel` runs the callback in `try { ... } catch (...) {}` (and `on_cancel`'s late call the
same), and the streamed path guarantees the `handler_done->wait()` on every exit between the `co_spawn` and the wait
(a scope guard or one outer `try`), so no later throw, for example a `bad_alloc` in `watch -> arm`, can unwind the
frame under the handler. Test: `probe_throwcb` in both modes must pass under ASan.

## Conditions for landing

1. L3-B1 and L3-B2 fixed, each with its real-socket test added to `coroutine_test`, and each test shown to fail with
   the fix reverted. A rechecker pass is enough.
2. Pins moved to the fixed heads in this order: reactor (unchanged), http, http-log, prism, as the ledger plans.
3. The ledger's "BodyStream drops the failure" row is re-pointed at the new test.

## What held (probes run, all clean)

All under ASan+UBSan, and under TSan where noted (`build.sh NAME asan|tsan`; sources in `.scratch/review-l3/`).

1. Risk 2, pipelining and the watch byte. `probe_pipeline`: 120 random rounds, 2 to 6 requests each (GET with
   suspended handler, buffered POST, streamed POST up to 200 KB, chunked buffered, chunked streamed), split at random
   byte boundaries including 1 to 3 byte pieces with random pauses, on 2 reactors, with and without a trailing
   `shutdown(SHUT_WR)`, and with the idle/header/inactivity/request timeouts armed. Every response arrived in order
   with the right body, and the gates ended at zero. TSan 40 rounds each, clean. `probe_timeouts`: the watch byte does
   not lose or delay the idle timeout (402 ms for 400), the header timeout after a single stolen byte (301 ms for 300),
   and a request begun 200 ms into the idle window parses correctly; a pipelined `GE` during a handler plus `T /fast...`
   later is one request. EOF cancels reads and still answers (`probe_admission` "half-close", `probe_streamcancel`
   cases 1 to 2).
2. Risk 4, admission. `probe_admission`: the gate returns to zero after a handler throw, RST and FIN mid
   buffered body, a stalled body (request timeout), 413 by Content-Length and by chunked, client close and RST during a
   handler, half-close, a streamed handler returning without reading 3 MiB, RST during a streamed upload, FIN mid
   streamed body, `create_extension` throwing `RequestRejected`, headers that never complete, pipelined then rejected,
   cap fill and release (cap 6 per reactor respected, 503 carries `Retry-After: 1`), an 8-thread storm of 480 random
   connections, and `stop()` with 8 requests in flight. After each the next 20 requests were admitted, so the cap does
   not leak. TSan: same, clean.
3. Risk 3, shutdown (`probe_shutdown`, ASan and TSan). Idle keep-alive with a pending watch read: 300 connections stop
   in 13 ms. Stalled streamed uploads with handlers blocked in body reads: 6 handlers released in 0 ms (C14's "cancel
   unfinished body reads" holds). 60 cancelable requests: all 60 answered `cancelled`, callbacks ran, frames intact.
   A client that never reads a 64 MiB response: `stop()` returns at the grace (405 ms with 400 ms; 5012 ms with the
   default). 25 rounds of `stop()` under a six-thread connect/request storm on 3 reactors (this is `abort_all` racing
   accepts): bounded, no handler frame outlives `stop()`, gates drained. `abort_all` from 3 threads at once, `stop()`
   twice, and deferred replies with a producer completing after the writer is gone (`probe_deferred_stop`, four modes,
   ASan and TSan): clean.
4. Risk 1, the intrusive list. `probe_abortlist`: 20,000 randomized walks where callbacks destroy the current node,
   the next node, random others, and add nodes mid-walk: every surviving start node visited exactly once, destruction
   unlinks, no UAF. Reactor-level: `track` racing `abort_all`/`force_all` from four threads with the loop churning
   2,000 nodes: late-tracked nodes were aborted and forced at `track()` (TSan clean).
5. The cap (`probe_cap`, `probe_cl`): the boundary is exact (== cap passes, cap+1 is 413, for Content-Length, chunked,
   1-byte chunks and one big chunk); 2^63, 2^32 and 1001 give 413; 2^64-1 and 2^64-2 are dropped by http-parser with no
   response (parser behavior, unchanged); a 20 MiB upload past a 1000-byte cap got its 413 in 20 of 20 tries.
6. A request's frame: the request, writer and `BodyStream` are still valid after a client close, RST, half-close and
   shutdown for a suspended handler that touches them after the cancel (`App::/cancelable`, `frame_bad == 0`).
7. The wrong-loop risk: in http and reactor nothing completes off the loop except the deferred reply's `notify`, which
   posts to the loop; `BodyStream`'s `async_compose` completes on the channel's executor (the connection's loop).
   I found no resumption on a foreign loop. The shared-accept path posts the new session to its target reactor.

## Non-blocking notes (follow-ups, no second review)

- L3-N1. The accept loop dies on one RST'd pending connection on macOS: `accept` returns `EINVAL` (22), which is not in
  `retryable_accept_error` (`reactor.h`, `retryable_accept_error`), the loop throws, `AcceptControl::finish` swallows
  the error, and the listener silently disappears (`connect` then fails with ECONNREFUSED). Reproduced on reactor main
  and on the branch (`probe_acceptrst`, `probe_poststorm`; one `SO_LINGER{1,0}` connect per round, dead within 100
  connects). Pre-existing, not introduced by L3, dev-host only as far as I can see (Linux returns ECONNABORTED, which
  is retried), but a client can kill a macOS server. Follow-up in reactor: treat `EINVAL` on Darwin as retryable and
  log a non-retryable accept error instead of swallowing it.
- L3-N2. `Reactor::abort_all` is documented to "return when they have all run" but a second concurrent caller returns
  at `if (aborting_.exchange(true)) { return; }` before the first caller's walk has run. Harmless for
  `TcpServer::stop` (single caller); matters if X6 calls `abort_all` from two places and then relies on it.
- L3-N3. `TcpServer::stop` runs `abort_all` before closing the acceptors; D16 lists the acceptors first. It is safe:
  a session accepted after the walk is aborted at `track()` (`walked_`) and a session queued before is walked
  (`probe_shutdown` race). Update D16's wording or reorder.
- L3-N4. `Reactor::track` changed from mutex-guarded (any thread) to "loop thread only" and the check is an `assert`
  (`reactor.h` `track`), compiled out in release builds. A consumer that tracks from a foreign thread now races
  silently. In-tree consumers are fine (`defer()`/`stream()` run in the handler on the loop). Say so in the
  Detent/consensus migration notes and consider a release-mode check or a post.
- L3-N5. Stale references: http `docs/http.d2` and `docs/http.svg` still draw the pool and `should_offload`;
  http `CMakeLists.txt:9` ("inline or offloaded"); http-log `AGENTS.md:47` (`Request::body_reader`); reactor
  `test/test.cc:2` header ("offload to the per-reactor pool"). No shims were found: the old names fail to compile
  (every old `HttpAsioService`/`TcpServer`/`UdpServer`/`Reactor` constructor arity no longer matches), which is the
  right way to migrate. prism is migrated; Xapiand is the known X3 list in the ledger; cluster's only use
  (`UdpServer(handler)`) is source compatible.
- L3-N6. `Reactor` member order (`reactor.h` private section): `gate_`, the session mutex and the sync mutex are
  declared after `io_`, so they are destroyed before it. A session frame destroyed inside `~io_context` (only on a
  reactor destroyed without `stop()` and with live sessions) would call `Lease::reset` on a destroyed gate. Not
  reachable through `TcpServer` (it waits for the sessions). Declare `io_` last-but-one or document it.
- L3-N7. Limits (for the architect): the 16 MiB body cap is per request. With the in-flight cap of 1,000 per reactor
  the aggregate buffered memory is not bounded (16 GiB per reactor in the worst case). It is far better than before
  (unbounded), but D2 and D12 should say what bounds the sum. Streamed and sunk bodies are uncapped by design (D12).
- L3-N8. The watch ends on any byte, so a stray CRLF after a POST body (some old clients), or a client that pipelines,
  silences disconnect detection for that request, as C17 states. A pipelined client that half-closes (`printf ... | nc
  -N`) has its second and later requests cancelled at once, because a watch armed after EOF fires immediately. Both are
  documented and agreed; flagging only so X3 does not take cancellation as always delivered.
- L3-N9. Known cost, not measured by me (no benchmarks): every keep-alive request now takes an extra one-byte read and
  a second read for the rest, plus re-arming the watch. The ledger leaves it to the lead.
- L3-N10. A handler that never returns holds `stop()` for as long as it runs (the grace bounds I/O only). Documented
  (`coroutine-handlers.md` "Shutdown"), consistent with D3, but X6 needs a bound on chores and turns that a handler can
  wait on, or `stop()` has no hard limit (`probe_shutdown ignore`: 2.3 s for handlers that sleep 2.5 s and ignore
  cancel).
- L3-N11. The new admission 503 is thrown from the headers-complete hook, which `http_parser` invokes from C frames.
  The mechanism is old (`create_extension` already threw `RequestRejected` there) and works on every platform CI ran,
  but it relies on unwinding through C frames; a one-line note in `http_request_parser.h` would keep a future
  `-fno-exceptions`-style C build from breaking it.

## Probes (all in `.scratch/review-l3/` of the http worktree, kept)

| File | Checks |
|---|---|
| `probe_common.h` | Raw client, configurable `App` (suspended, cancelable, streamed, throwing, huge, reject routes) |
| `probe_pipeline.cc` | Random split/pipelined/keep-alive streams; FIN variant; timeouts variant |
| `probe_timeouts.cc` | Idle, header, inactivity timeouts with the persistent watch read |
| `probe_admission.cc` | Gate returns to zero on every exit path; cap fill; storm; stop |
| `probe_shutdown.cc` | Ignore-cancel, stalled writer (grace 400 ms and 5 s), stalled stream, idle, cancelable, accept race x25, double stop, concurrent `abort_all` |
| `probe_deferred_stop.cc` | Deferred replies vs disconnect, shutdown, late producer |
| `probe_cap.cc`, `probe_cl.cc` | Cap boundaries; absurd Content-Length; past-the-cap upload |
| `probe_abortlist.cc` | `AbortList` randomized model; `Reactor` abort/force/track races |
| `probe_streamcancel.cc` | Streamed request: FIN/close after body, mid-body close, stop |
| **`probe_truncated.cc`** | **L3-B1** (3 of 4 truncation cases read as `eof`) |
| **`probe_throwcb.cc`** | **L3-B2** (`probe_throwcb` streamed: ASan UAF; `probe_throwcb buffered`: terminate) |
| `probe_acceptrst.cc`, `probe_poststorm.cc` | L3-N1 on reactor main and on the branch |
| `build.sh NAME asan|tsan`, `build_rx.sh` | Build against the worktree reactor (or `RX=dir` for an instrumented copy), Homebrew LLVM, reusing the prebuilt `build-asan`/`build-tsan` deps |

## Re-check (to append)

Re-check starts from `probe_truncated` (must report 0 clean-eof cases), `probe_throwcb` (both modes pass) and
`probe_pipeline`/`probe_admission` (still clean).

## Not run

Linux, FreeBSD and arm64 (CI only; N1 is macOS-specific). TLS (`tls_asio_test` was not exercised by my probes; the
watch and the one-byte read ride on the SSL stream there). The developer's mutants, libFuzzer and `coroutine_test`
(not redone, per the brief). The Xapiand build and the X3 consumers. fd exhaustion plus a stalled peer together (the
`force_all` path's first-time handler type under UBSan on Linux, the CI failure the last reactor commit fixed). Owned
stream replies under shutdown beyond the deferred reply probe. Benchmarks, per the rules.
