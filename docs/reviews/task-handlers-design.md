> Status: CURRENT (2026-11-02) preliminary design review of W2a, revision 1 at http:b110118.

# http review: W2a, http handlers on task (preliminary design)

- Date: 2026-11-02.
- Reviewed: `docs/design/task-handlers.md` and `docs/status/task-handlers.md` at http:b110118
  (branch `design/task-handlers`); http `origin/main` http:679c0f8 (`http_asio.h`, `http_handler.h`,
  `http_body_stream.h`, `http_message.h`, `http_router.h`, `http_asio_tls.h`, `bench/run.sh`,
  `CHANGELOG.md`); task:8cad661 (`task.hh`, `task_asio.hh`, `ARCHITECTURE.md`, README performance
  table); reactor:ade618d (`reactor.h`, `reactor_udp.h`), the pin http:679c0f8 declares; Xapiand
  Xapiand:81c311799 (`src/engine/host_asio.h`); Detent `src/detentd/main.cc`,
  `src/detent_relay/host.cc`; cluster `bus.h`.
- Checked against: Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "One coroutine type: http handlers on
  task (November 2)", items 1-4; the owner's rulings relayed on 2026-11-02 (no mixed shape: the
  connection loop and factories move to task; D8's asio loop only as a case to the owner if a
  considerable gap survives optimizing task); the L3 review `docs/reviews/phase7c-l3-code.md` and
  its probes (`~/code/Kronuz/http-wt/l3-http/.scratch/review-l3/`, read only); the Xapiand lead's
  notes `docs/reviews/task-handlers-xapiand.md` (untracked in this worktree);
  Detent `docs/reviews/calibration.md` (both checks applied: configured bounds reach consumers, and
  holders exporting member addresses).
- Checked by: probes in `.scratch/arch-review/` (`probe_loop.cc`, `build.sh`, `src/` holds the
  task:8cad661 headers and the http:679c0f8 sources read), built under `quiet light` with Apple
  clang `-O2` (`rel`), Homebrew clang 23.1.2 ASan+UBSan (`asan`) and TSan (`tsan`), asio 1.36.0, on
  the i9-9980HK; each of the four modes run in all three builds, no sanitizer report. Results in
  `.scratch/arch-review/results.txt`.
  - Not run: http's suite, the L3 probes on any API, T6 and T7 (no benchmarks), g++ builds,
    fuzzing, the M4.

## Verdict

**REJECT: revision 1 is the mixed shape the owner ruled out, and the all-task loop it must become
needs a cancellation carrier task does not have. S1 (baseline instrumentation) and S2 (the task
pin) may start now; S3 onward waits for revision 2.**

What holds, and reproduces:

- The handler API in D1 (`HttpHandler::handle`, `Router::Route`, `BodyStream::read` returning
  `task::task`) is the right public shape, and `read()` returning the error code keeps L3-B1's
  eof-versus-failure contract (`http_body_stream.h:53-59`).
- The writer side needs no coroutine type: `ResponseWriter`, `DeferredReply` and
  `OwnedStreamReply` are synchronous on the loop or callback submits (`http_handler.h:88-130`).
- Budget fairness against I/O holds by construction: `asio_scheduler` drains one demoted handle
  per executor pass and reposts behind ordinary posts (`task_asio.hh:65-72`, `:95-108`), so every
  pass also polls the reactor.
- The cost of the all-task loop's known overhead is small against the request: one extra
  `asio::post` per socket completion (probe `posts`, TH-R4), which task's own table times at 42 ns
  (M4) to 148 ns (i9) per post (task README, "Yield and resume"), against 10-22 us of server CPU
  per request (`CHANGELOG.md:9-23`).

What blocks:

- TH-R1: the connection factories, `BodyStream::push` and the TLS seam stay `asio::awaitable` in
  public headers; the owner's ruling moves them, and the loop, to task.
- TH-R2: the loop's shutdown, timeouts and deferred writes are carried by asio's per-coroutine
  cancellation; task has none, so a task loop loses idle shutdown and the `||` timeouts unless the
  design adds a carrier.
- TH-R3: D3's driver and channel and D4's shared block solve the bridged loop's problem; in the
  task loop the handler is a child task and the real lifetime hazard moves to teardown.
- TH-R8: T6 cannot see the regressions it gates: req/s is capped by wrk on the M4 and the i9 A/A
  spread is 18%; "considerable" needs a threshold on server CPU per request.

## Findings

### TH-R1. A second public coroutine type remains (blocking)

D1 keeps four seams on `asio::awaitable` because `reactor::Session` is one (design lines 68-71):

1. `HttpAsioService::ConnectionFactory` and `TimedConnectionFactory` (`http_asio.h:1010-1013`) are
   public typedefs. Detent names the type (`src/detentd/main.cc:244`) and stores
   `http::tls_connection_factory_timed(...)` in it (`:261`, and `src/detent_relay/host.cc:244`);
   any consumer that writes its own factory writes an `asio::awaitable` coroutine.
2. `BodyStream::push` (`http_body_stream.h:70`) is a public member in a header every streamed
   handler includes.
3. `detail::serve_tls_connection` (`http_asio_tls.h:52-53`) and `detail::serve_connection`
   (`http_asio.h:990`) are what the factories return.
4. `reactor::Session` and `TimedSession` (reactor:ade618d `reactor.h:280-282`) are reactor's public
   API. Its direct users are http and the L3 probes (`probe_acceptrst.cc:19-21`); Detent and
   Xapiand reach it only through http's factories (no direct use in `Detent/src`). cluster
   uses `reactor::UdpServer`, whose `receive_loop` (`reactor_udp.h:205`) is a private awaitable.

D8's bridged loop is private, but under the ruling it goes too. Q2's `cancelled()` beside
`cancel_token()` is the same duplication at the API (TH-R6).

Fix: every one of these becomes `task::task`: the factories return `task::task<void>`, `push`
returns `task::task<bool>`, `serve_connection` and `serve_tls_connection` are tasks, and
`reactor::Session` is a `task::task<void>` factory. The reactor change is not "the next step"
after http (the Xapiand notes, line 20-25): it carries the cancellation of TH-R2 and the scheduler
of Q1, so it is the first implementation step (TH-R13). Detent's factory use stays source
compatible: `main.cc:244` names the typedef, not the coroutine type, and both sites take the
factory from `tls_connection_factory_timed`.

### TH-R2. The all-task loop has no cancellation carrier (blocking)

The connection loop cancels through asio's per-coroutine cancellation state, at twelve sites:

| Site | Use |
| --- | --- |
| reactor `reactor.h:296-301`, `:318-320` | shutdown: `SessionControl::abort` emits `cancellation_type::all` into the session's `co_spawn` slot |
| `http_asio.h:356`, `:507`, `:890`; `http_asio_tls.h:96` | `||` races: write before deadline, read before deadline, feed versus handler done, TLS handshake deadline; the loser is cancelled through its slot |
| `http_asio.h:605-607`, `:642` | deferred and owned writes bound to `write_cancellation()` |
| `http_asio.h:127`, `:581-582`, `:670` | `this_coro::cancellation_state` reads after a wake |
| `http_asio.h:897`, `:922`, `:953`, `:967`, `:996` | `reset_cancellation_state`: disabled from handler start to written response (the L3 rule), terminal-only between requests, disabled for the final drain |

task has none of this. `await_asio` binds no cancellation slot (`task_asio.hh:163-178`);
`cancel_token` is a polled `shared_ptr<atomic<bool>>` with no callbacks (`task.hh:621-641`);
there is no `when_any`. Probe `cancel` puts a socket read under a reactor-style slot emit:
the `asio::awaitable` read ends 0.0-0.1 ms after the emit; the same read in a task behind
`as_awaitable` is not reached, and ends only when the 500 ms fallback closes the socket
(489-491 ms after the emit, in all three builds). In the service that turns L3's idle keep-alive
shutdown (300 connections in 13 ms, `phase7c-l3-code.md:121-122`) into the force grace (default
5 s), and leaves a `||` timeout whose loser never stops.

Fix: the design names one cancellation mechanism and where it lives. Recommended, because it also
answers Q2 (TH-R6) and keeps one implementation:

1. task's `cancel_source` gains a reason and callback registration (the shape of
   `std::stop_callback`, registered and deregistered by an RAII object in the waiting frame), with
   a throwing callback contained in task (L3-B2's rule moves into the library that runs the
   callbacks). `checkpoint()` stays a relaxed load (`task.hh:661`).
2. `task_asio.hh` gains `await_asio(init, token)`: it binds an `asio::cancellation_signal` slot to
   the operation's handler and registers a callback that emits it, so a requested token cancels
   the pending asio operation, which completes with `operation_aborted` and resumes normally.
3. The four `||` sites become "arm a timer whose callback cancels the operation", the pattern
   `expire_read` already uses (`http_asio.h:546-552`), or a task `when_any` if the designer
   prefers one primitive; either way the loser must complete before the frame goes on (the
   comment at `:600-602`).
4. reactor's `SessionControl::abort` requests the session's `cancel_source` instead of emitting a
   slot, and the loop's "disabled while the handler runs" becomes structural: the loop passes the
   session token to its own reads and writes, never to the handler, which sees only
   `Request::cancel`.

Gate: probe `cancel` rerun on the task carrier ends the read within one loop pass, and
`probe_shutdown` keeps its numbers (TH-R10).

### TH-R3. D3 and D4 answer the bridged loop; the task loop has a different hazard (blocking)

In a task loop the connection does `co_await handler->handle(request, writer)`. task's awaiter
starts the child by symmetric transfer (`task.hh:441-446`) and the child's final suspend
transfers straight back when the scheduler is the same (`task.hh:378`), so a handler that
finishes during its first run costs no post, no driver frame and no channel. D3's driver,
`running_on` scope and channel signal, and D4's per-connection `shared_ptr` block, exist only
because the handler frame sat outside the connection's asio stack. They go. `Request`, the writer
and the `BodyStream` live in the connection frame as today, and the L3 rule "frames end only by
completing" holds as long as no task frame is destroyed while suspended.

That last condition is where the risk moves. Probe `teardown` destroys an `io_context` (no
`stop()`, D4's own scenario) with the server coroutine suspended in a socket read: the
`asio::awaitable` frame is destroyed with it (1 of 1); the task frame is not (0 of 1, in all three
builds), because `await_asio`'s handler holds a raw awaiter pointer (`task_asio.hh:173-178`) and nothing
destroys the task when asio destroys the handler unrun. That is a leak, not a use-after-free, but
it leaks the connection's `Request`, socket and admission lease, and `asio_scheduler`'s destructor
asserts on a demoted handle left behind (`task_asio.hh:54`).

Two cases still need concurrency in the task loop: the streamed path runs the handler beside the
feed (`http_asio.h:849-850`), and the deferred and owned writes run beside a deadline. The design
names the primitive (a `spawn` onto the reactor's scheduler plus a join on a `task::event`, or a
task-side start-now-join-later) and its cost (one post per streamed request).

Fix: revision 2 replaces D3 and D4 with "the handler is awaited as a child task" and a teardown
rule: the reactor stops and drains its loop before its `io_context` and scheduler are destroyed,
and a test shows zero live task frames after `stop()` (a frame counter on the pool, or LSan on the
Linux ASan job; LSan does not run on macOS). T4 (d) changes to that test.

### TH-R4. An inline `await_asio` resume must open a scheduler scope (condition on the task change)

The Xapiand notes make "resume inline when the completion already runs on the scheduler's thread"
the first optimization. It removes the extra post the task loop pays per socket completion
(probe `posts`: 10,000 socket round trips cost 40,002 io_context handlers with `asio::awaitable`
and 60,003 with task plus `await_asio`; the client's two callbacks per round trip are in both, so
the server's two completions cost 2 handlers against 4, one extra post each; operator new is 9
against 5 in total, 0.001 per round trip, so frames are not the cost). It is safe only inside
`running_on(scheduler)`:

- `budget::checkpoint()` is ready whenever `current()` is empty (`task.hh:661`). Probe
  `nocurrent` resumes a task from a raw asio handler, as an inline resume without a scope would,
  then runs a 200 us budget loop for 5 ms: none of its checkpoints yields and the loop runs no
  other handler during the 5 ms. Resumed through `asio_scheduler`, the same loop yields at 19-20
  checkpoints and the loop runs 24-25 other handlers. A handler resumed that way monopolizes its
  reactor and never lets the disconnect watch run, so T4 (f) would hang.
- `continuation_scheduler`, event and lock waiters record `current()` at suspend
  (`task.hh:444`, `:724`, `:882`). A resume without the scope records an empty scheduler, and the
  continuation then resumes on whatever thread completes it: the wrong-loop resume L3 found absent
  (`phase7c-l3-code.md:138-140`).
- The thread test cannot be `current() == sched_`: a raw asio completion has no `current()`. It
  needs the executor's own check (`io_context::executor_type::running_in_this_thread()`, reached
  through `any_io_executor::target`) or a thread recorded by the scheduler.

Condition: the task change ships with a test that a budget loop after an inline-resumed socket
read still demotes, and a TSan test that a continuation awaited after an inline resume returns
to its loop.

### TH-R5. Q1: the reactor owns its loop's scheduler (agree with the Xapiand lead)

D2 creates two paths: the service's per-reactor scheduler bound to its thread by a post, and a
scheduler the connection owns "for its life" on loops the service did not start (design lines
75-81). Two owners for one thing is the duplication the decision forbids, and a connection-owned
scheduler dies with a connection whose demoted or posted handles may still be queued
(`task_asio.hh:54`). With `reactor::Reactor` owning one `task::asio_scheduler` over its
`io_context`, every loop has exactly one, tests that build a `Reactor` get one, the thread binding
goes, and Detent's non-HTTP owner loops get the same scheduler without http. Since TH-R2 puts the
reactor step first, no interim http-owned version is needed. `HttpAsioService::scheduler(i)`
becomes `reactor(i).scheduler()`, as the Xapiand notes say.

### TH-R6. Q2: one mechanism, carried by the token (agree, with two refinements)

`cancelled()` is `cancel_token().requested()` read on the loop (`http_message.h:141-142` against
D5); keeping both is a duplicate API. `on_cancel` is the woken view, which the polled token cannot
express today (`task.hh:621-641`). With TH-R2's carrier the token has the reason and the callbacks,
so `Request` exposes `cancel_token()` (engaging the watch at the call, on the loop) and
`cancel(reason)` for the transport, and `cancelled()`, `cancel_reason()` and `on_cancel()` go.

Refinements:

1. The reason in task is generic (an integer the token carries, `CancelReason` mapped in http),
   since task has no HTTP vocabulary.
2. The source is created on the first `cancel_token()` call, so a handler that never asks pays no
   `make_shared` (`task.hh:634`); a handler that asks pays one allocation per request, and T7 must
   include such a kind (TH-R9). Xapiand asks on every request (`search_views.cc:3078-3084`),
   and its callback also fails the `BodyStream` so a waiting read ends; with `read()` taking the
   token through `await_asio`, that part of the callback goes too.

Callbacks run on the thread that calls `request()`. For `Request::cancel` that is the loop
(`http_message.h:137-138`); the documentation says so.

### TH-R7. D7's 503 after a started response (non-blocking)

The 503-and-close applies only when the response is unanswered: `!writer.ended() &&
!writer.is_deferred()` (`http_asio.h:650-657`). A buffered reply is not on the wire until the
handler returns, so "started" means ended, deferred or an owned stream. Ended and deferred are
already handled by `map_handler_error`. An owned stream whose head has gone out must close
without its terminal `0\r\n\r\n`, or the client reads a truncated body as complete, the response
side of L3-B1. D7 also needs to say how it tells "after `Request::cancel`" from "the handler's own
source": by the request's cancel state when the exception reaches the loop. Add a T4 case: an
owned stream started, then `task::cancelled` escapes; the client sees no terminal chunk and the
gate returns to 0.

### TH-R8. T6 cannot see what it gates; "considerable" needs a number (blocking)

1. On the M4, wrk caps every server near 37,000 req/s (`CHANGELOG.md:21`), so req/s cannot
   separate A from B there.
2. On the i9, the A/A spread of req/s is 0.84 to 1.02 (`CHANGELOG.md:20`): "within the A/A band"
   passes the 15-17% loss the same table records for `e70ba34`.
3. Server CPU per request is the row that moved on both hosts (M4 A/A 1.01; the 4% gain from
   22.5 to 21.7 us was visible), and `run.sh` already prints it (`bench/run.sh:38-39`). T6
   reports it but does not gate on it.
4. The owner now wants before and after for the task optimization, so T6 needs three binaries
   beyond A/A: A (head, asio), B0 (task loop on task:8cad661), B1 (task loop on the optimized task
   pin).

Proposed gate, against the A/A band from `ab_compare.py` (median of 7 rotated rounds, paired):

| Row | Same shape | Considerable (the case goes to the owner) |
| --- | --- | --- |
| Server CPU per request, 8 and 64 connections, `http` and `httpr`, i9 and M4 | B1/A within the A/A band, or worse by at most 3% | B1 worse than A by more than 5% beyond the A/A band edge on both hosts after the task optimizations, with every paired ratio worse than every A/A ratio |
| p99 latency, same cells | within the A/A band, or worse by at most 5% | worse by more than 10% beyond the band edge on both hosts |
| req/s | reported, not gated on the M4 (wrk-bound); on the i9, within the band | as CPU per request |
| Allocations per request (T7) and io handlers per request | B1 ≤ A, exact | none: a count over A is a defect to fix, never a case |

Between the two columns the task loop lands and the gap stays in the ledger as an open
optimization target, not a case. Without the inline resume, B0's io handlers per request exceed
A's by one per socket completion (probe `posts`); B1 must close that count.

Why 5%: one extra post per socket completion costs 42-148 ns (task README) and a keep-alive
request has two completions, about 0.1-0.3 us against 10-22 us per request (0.5-3%). A gap past
5% (1.1 us per request on the M4) is several times what the known mechanism explains, so it has a
structural cause worth the owner's decision. In capacity terms, 5% of the i9's 10 us per request
is 0.5 us, or 0.05 cores at 100,000 req/s. A row whose own A/A band is wider than ±3% is
not decisive: rerun it with more rounds or longer runs before ruling.

### TH-R9. T7 measures the wrong kinds for the costs this design adds (condition)

T7 counts allocations for `http` and `httpr` only. The new per-request costs are elsewhere: the
cancel source (`make_shared`, TH-R6) for a handler that asks, `BodyStream::read` frames on a
streamed body, the streamed handler's spawn, and frames over 2 KiB (`task.hh:255`; the connection
frame holds `char tmp[64 * 1024]`, `http_asio.h:765`, so it is heap-allocated once per connection
in both A and B). Add kinds: one that takes `cancel_token()` and runs a budget, one streamed POST,
and `close` mode (one request per connection) for the per-connection frame. Add the io handlers per
request from `io_context::run()` beside the allocation count: it is deterministic and is what
TH-R4's optimization changes.

### TH-R10. T3 omits three probes and states no numbers (condition)

1. `probe_abortlist`, `probe_acceptrst` and `probe_poststorm` are not in T3. The last two drive
   `reactor::Session` (`probe_acceptrst.cc:19-21`) and move with the reactor step.
2. `build.sh` hard-codes `H` (`review-l3/build.sh:4`); the copy needs editing, not an environment
   variable, and the include list needs task's directory.
3. "As in the review" is not a target. State the review's numbers as targets: idle keep-alive
   shutdown of 300 connections within 50 ms (review: 13 ms), stalled streamed uploads released at
   once (0 ms), `stop()` at the grace with a client that never reads (405 ms for 400 ms),
   `probe_pipeline` 120 rounds in order with gates at 0, `probe_truncated` 0 of 4 eof. The
   cancellation carrier (TH-R2) is what can regress the first one.
4. The probes assert `req.cancelled()` (`probe_common.h:109-116`, `probe_streamcancel.cc:11-14`);
   with Q2 they move to the token. That is an assertion change, not a scenario change.

### TH-R11. T1's grep misses the bridges that remain (condition)

Xapiand's bridge is `engine::Run::await` (`host_asio.h:131-145`), a copy of `task::as_awaitable`
(`task_asio.hh:118-133`) under another name; `grep as_awaitable` does not find it. Once nothing
uses it, `task::as_awaitable` is itself a shim in task's public header. T1 becomes: no
`asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http's, http-log's and
prism's headers and sources, nor in reactor's after its step; no `async_initiate` over a task in
Xapiand; and `as_awaitable` removed from task in the wave, its last user gone.

### TH-R12. Affinity across schedulers needs one rule and a check (condition)

A handler that awaits a child which moves to Detent's owner loop (`co_await task::schedule_on`
inside the child) comes home: the child's final suspend posts to the scheduler recorded at the
await (`task.hh:378-381`). A handler that writes `co_await task::schedule_on{owner}` in its own
body stays on the owner thread and then touches `Request` and the writer there, which are
loop-only (`http_message.h:137-138`). Nothing detects it. Add a debug assertion that
`task::current()` is the connection's scheduler in `ResponseWriter` calls and `BodyStream::read`,
and a T4 case for both shapes (the child form passes, the body form asserts in debug).

### TH-R13. Step order and consumers (condition)

Revision 2's order: (1) S1 baseline, S2 pin (start now); (2) task: the cancellation carrier
(TH-R2) and the inline resume (TH-R4) on task's own bench and tests, then a pin; (3) reactor:
`Session` on task, the scheduler it owns (TH-R5), `SessionControl` on the carrier, the accept and
UDP loops on task; (4) http: handler API, loop, factories, TLS; (5) http-log, prism; (6) Detent
(both factory sites), cluster (reactor pin; `bus.h` API unchanged), Xapiand's note. T8 adds
cluster. D9's pin moves to the task commit of step 2.

## Answers to the open questions

- **Q1.** The reactor owns one `task::asio_scheduler` per loop; http exposes it as
  `reactor(i).scheduler()`; Xapiand's `AsioHost` wraps it (TH-R5). No interim http-owned scheduler,
  because the reactor step comes first.
- **Q2.** One mechanism: the task token carries the reason and the callbacks; `Request` keeps
  `cancel_token()` and `cancel(reason)`; `cancelled()`, `cancel_reason()` and `on_cancel()` go
  (TH-R6). The churn is mechanical; the L3 probes change assertions only.
- **Q3.** Under the ruling, the task loop is built in any case and Q3's options reorder: optimize
  task first (the inline `await_asio` resume, TH-R4; an inline-start primitive is not needed for
  the buffered path, since `co_await` of a child already starts inline, and is worth it only if the
  streamed path's one post shows in T7's streamed kind), measure B0 and B1, and take D8's asio loop
  to the owner only if TH-R8's "considerable" threshold is crossed after that work. Option (c),
  accepting a cost silently, is out.

## Probes (kept in `.scratch/arch-review/`)

| Probe | Finding | Result (rel, ASan+UBSan, TSan) |
| --- | --- | --- |
| `probe_loop posts` | TH-R4, TH-R8, TH-R9 | 40,002 against 60,003 io handlers per 10,000 round trips; 9 against 5 `operator new` |
| `probe_loop teardown` | TH-R3 | frames destroyed with the `io_context`: awaitable 1 of 1, task 0 of 1 |
| `probe_loop nocurrent` | TH-R4 | yielding checkpoints 19-20 through the scheduler, 0 without a scope; other handlers 24-25 against 0 |
| `probe_loop cancel` | TH-R2 | slot emit ends an awaitable read in 0.0-0.1 ms; a task read behind `as_awaitable` ends at the 500 ms socket close |

Rebuild: `.scratch/arch-review/build.sh rel|asan|tsan` (under `quiet light`), then
`bin/probe_loop-KIND MODE`. `src/` holds the task:8cad661 headers the probe compiles against.

## Blocking items / Required changes

- TH-L1 (TH-R1): revision 2 puts every seam on task: factories, `BodyStream::push`,
  `serve_connection`, `serve_tls_connection`, `reactor::Session`; D8 states the task loop as the
  design, and the asio loop only as the owner's case under TH-R8.
- TH-L2 (TH-R2): a decision naming the cancellation carrier (task token with reason and callbacks,
  `await_asio` with a token, the race pattern for the four `||` sites, `SessionControl` on the
  carrier), with a test per site in the table.
- TH-L3 (TH-R3): D3 and D4 replaced by "the handler is a child task", the teardown rule, the streamed
  path's concurrency primitive, and T4 (d) rewritten as a zero-live-frames test after `stop()`.
- TH-L4 (TH-R8): T6 gates on server CPU per request and p99 with the thresholds above (or the owner's
  own), with A, AA, B0 and B1 binaries.

## Conditions

- C1 (TH-R4): the inline resume runs inside `running_on`, with the two tests named.
- C2 (TH-R9): T7 adds the token-and-budget kind, a streamed kind, `close` mode, and io handlers per
  request.
- C3 (TH-R10): T3 lists all thirteen probes with numeric targets.
- C4 (TH-R11): T1's grep as above; `task::as_awaitable` removed in the wave.
- C5 (TH-R12): the affinity assertion and its T4 case.
- C6 (TH-R7): the owned-stream cancel case in T4.
- C7 (TH-R13): the step order and T8's consumer list, cluster included.

The revision that folds these in adds one new mechanism (the cancellation carrier, TH-L2); that
section gets a scoped re-check, the rest the lead's confirmation.

Not run: http's suite, the L3 probes, T6 and T7 benchmarks, g++ and the M4, fuzzing.
