> Status: DRAFT (2026-11-02) Revision 1, designing (ready for preliminary review). Ledger:
> `docs/status/task-handlers.md`. Workstream W2a of Detent `docs/PLAN.md`.

# http handlers on task

Reads http `origin/main` 679c0f8; task 8cad661 (`task.hh`, `task_asio.hh`, README, ARCHITECTURE);
http-log 431b878 and prism da169a9 (their `origin/main`); Xapiand 81c311799 (read only); the
Phase 7c L3 ledger and review (`docs/status/phase7c-l3.md`, `docs/reviews/phase7c-l3-code.md`) and
their probes in `http-wt/l3-http/.scratch/review-l3/`. Line numbers below are at these commits.

## Summary

`task::task<T>` (Kronuz/task) is the family's one coroutine type (Detent
`docs/SHARED_LIBRARY_DECISIONS.md`, "One coroutine type", November 2). This design makes http's
handler-facing API task-based and keeps everything below it on Asio:

1. `HttpHandler::handle`, `Router` routes and `BodyStream` reads return `task::task` (D1). Nothing
   a handler calls on the writer side is awaited, so the writer API does not change.
2. Each reactor gets one `task::asio_scheduler`; a handler starts inline on it, so a handler that
   never suspends costs no extra queue trip, and budget checkpoints demote onto that reactor's
   queue (D2, D3, D6).
3. Request cancellation stays one mechanism: the transport detects (CancelWatch), `Request::cancel`
   signals, and the handler observes through a `task::cancel_token` the request hands out (D5).
4. The connection loop stays `asio::awaitable` privately unless the throughput or allocation gate
   (T6, T7) fails with it; then it moves to task (D8).

The public type is task either way. No alias, shim or second handler signature remains, and
Xapiand's bridge (`engine::Run::await`, `src/engine/host_asio.h:131`) is deleted.

## What this depends on and changes

| Document | Relation |
| --- | --- |
| Detent `SHARED_LIBRARY_DECISIONS.md`, November 2, items 1-4 | Implements it; the L3 constraints (item 3) are T3 and T4, the gate (item 4) is T6 and T7 |
| Detent `docs/PLAN.md` W2 | W2 writes Detent's handlers against D1 instead of `asio::awaitable` |
| Xapiand `docs/design/phase7c.md` D2, D3 | Changes how D3's cancel reaches a budget (D5) and where the reactor's scheduler lives (D2, Q1) |
| http `docs/mechanisms/coroutine-handlers.md` | Rewritten at landing (S5); its rules (watch, monitor, shutdown) are unchanged |

## Decisions

### D1. Public signatures

```cpp
// http_handler.h (includes "task.hh", no longer <asio/awaitable.hpp>)
virtual task::task<void> handle(Request& request, ResponseWriter& response) = 0;
// http_router.h
using Route = std::function<task::task<void>(Request&, ResponseWriter&, const Params&)>;
task::task<void> Router::handle(Request&, ResponseWriter&) override;
// http_body_stream.h: replaces the async_read(chunk, token) template
task::task<asio::error_code> BodyStream::read(std::string& chunk);
// http_message.h: Request gains
task::cancel_token cancel_token();   // D5
```

These replace `http_handler.h:183`, `http_router.h:120` and `:128`, and `http_body_stream.h:60-64`.
`read()` keeps the completion values of today's `async_read` (`http_body_stream.h:53-59`): no error
with a non-empty chunk, `asio::error::eof` only for a complete body (sticky), any other error for a
failed body. It returns the code rather than throwing, because eof versus failure is the contract
L3-B1 fixed. It is `await_asio` over the channel's `async_receive`, the form Xapiand already uses
(`search_views.cc:2944`).

`ResponseWriter` (`http_handler.h:88-130`), `BufferedResponse`, `DeferredReply` and
`OwnedStreamReply` do not change: their calls are synchronous on the loop or thread-safe submits
with callbacks (`http_deferred.h:270`, `http_owned_stream.h:301-313`), and a disconnect cancels
reads, not writes. A task producer that waits for owned-stream room adapts `on_writable` to a
`task::event` itself.

The transport seams stay `asio::awaitable` because `reactor::Session` is, and reactor is out of
scope: `HttpAsioService::ConnectionFactory` and `TimedConnectionFactory` (`http_asio.h:1010-1013`),
`detail::serve_connection`, `serve_tls` (`http_asio_tls.h:52`) and `BodyStream::push`
(`http_body_stream.h:70`). Detent's TLS factory (`src/detentd/main.cc:244`) is source compatible.

### D2. One scheduler per reactor

`HttpAsioService` owns one `task::asio_scheduler` per reactor over that reactor's executor, declared
before `server_` so it outlives the loops (the order rule at `http_asio.h:1095-1098`). The service
binds each to its loop thread with a post queued before the loop runs (FIFO puts it ahead of every
accept). The connection reads the binding; a connection served on a loop the service did not start
(a test driving `serve_connection` on its own `io_context`) owns a scheduler for its life. The
factory signatures do not change. `task::asio_scheduler& HttpAsioService::scheduler(std::size_t i)`
exposes it next to `reactor(i)`, so an application can count `demoted()` for admission (Q1).

### D3. How a handler runs

A private driver per request (a `task` frame from task's pool) awaits the handler task and records
done-or-exception. The connection starts it inline, inside
`task::running_on scope(task::scheduler_ref::of(scheduler))`, so `task::current()` is the reactor's
scheduler and the slice starts now (`task.hh:162`, `:661`).

```
connection (asio)          driver (task)                    handler (task)
 start inline ---------->  co_await handle(req, w) ------>  runs to first suspend
 done? -> respond          (synchronous: no asio op, no post)
 else co_await done ...    resumed by the scheduler  <-----  completes on the loop
      <---- channel try_send (posted to the connection's executor)
```

A handler that finishes during its first resume is the common case (a buffered `send()`), and it
costs no asio operation. A suspended handler always resumes through the scheduler, so the driver's
completion runs on the loop after the connection has started its wait; it signals a per-connection
`asio::experimental::channel<void(asio::error_code)>` of capacity 1 (the pattern at
`http_asio.h:558`), so the connection never resumes inside a task trampoline or `running_on` scope.
The streamed path (`http_asio.h:849-850`) starts the same driver instead of `asio::co_spawn`; its
completion calls `HandlerDone::finish()` and `BodyStream::abandon()` as today, and the join at
`:889-911` is unchanged.

### D4. Lifetimes: request, writer and BodyStream outlive the connection frame

Today the handler frame is a child of the connection's awaitable, so Asio destroys it before the
request it references. A task frame is outside that stack: if the connection frame were destroyed
(an `io_context` torn down without `stop()`, or a throw like L3-B2's between start and wait), a
suspended handler resumed later would touch a dead `Request`. So the `Request`, the
`AsioResponseWriter` and the executor it points at move into a per-connection block held by
`std::shared_ptr`, reused across keep-alive requests (one allocation per connection, none per
request); the driver holds a reference while the handler runs. `Request::body_stream` is already
shared (`http_asio.h:842`). The admission lease stays in the connection frame
(`http_asio.h:747`): any exit releases it once. The connection's cancellation stays disabled from
handler start to written response (`:897`, `:953`), as L3 requires.

### D5. Cancellation: one mechanism, three roles

| Role | Mechanism | Change |
| --- | --- | --- |
| Detect a hang-up | `CancelWatch` (`http_message.h:56-61`), engaged lazily (`http_asio.h:455-466`) | none |
| Signal | `Request::cancel(reason)`, first call wins (`http_message.h:149-153`) | also requests the request's `task::cancel_source`, if one was handed out |
| Observe, polled | `Request::cancel_token()` | new: creates the source on first call (no allocation for a handler that never asks) and engages the watch, like `cancelled()` |
| Observe, woken | `Request::on_cancel(callback)` | none; a throwing callback stays contained (`http_message.h:193-196`) |

A handler writes `task::budget slice(200us, request.cancel_token())`; checkpoints then throw
`task::cancelled` after a disconnect or shutdown. `cancel_source::request()` is a relaxed atomic
store (`task.hh:632-640`), so a token can travel to work on other threads. The watch rules do not
change: a disconnect cancels reads, not writes, and a pipelined byte ends the watch for that
request. `BodyStream` failure stays the transport's (`http_asio.h:854-857`), so a truncated body
still reads `connection_reset`, never eof or `operation_aborted`.

### D6. Budgets

`checkpoint()` demotes with `current().post_demoted(h)` (`task.hh:661-663`) onto the reactor's
scheduler from D2; `asio_scheduler` drains one demoted handle per executor pass, so socket
completions and fresh requests run first (`task_asio.hh:65-72`, `:95-108`). Each resume starts a
fresh slice. A demoted handler is a running handler: it keeps its admission lease, is tracked for
shutdown, and `stop()` waits for it as for any handler, so no demoted handle remains when the
scheduler is destroyed (its destructor asserts that, `task_asio.hh:54`).

### D7. Exceptions

| Escapes `handle()` | Result |
| --- | --- |
| any exception | stored in the task, rethrown at the driver, mapped by `map_handler_error` (`http_asio.h:650-657`): `on_error`, then a generic 500 |
| `task::cancelled` after `Request::cancel` | `on_error` first; if unanswered, 503 with `Connection: close` (Xapiand's mapping at `search_views.cc:3148`, moved into http) |
| `task::cancelled` from the handler's own source | as any exception |
| from the driver or a callback | impossible: the driver catches everything; `on_cancel` callbacks are contained |

### D8. The connection loop: decided by the gate

The loop stays `asio::awaitable` privately (the bridged loop) if T6 and T7 pass with it. If either
fails, the developer moves it to task (the task loop): socket operations through `await_asio`, and
the `||` timeouts (`http_asio.h:356`, `:507`) rebuilt without awaitable operators; it must pass
the same rows. The task loop is the larger change and carries a known cost: `await_asio` resumes
through `scheduler.post` (`task_asio.hh:175-178`, `task.hh:682-686`), one more queue trip per
socket completion. If neither passes, the step stops for Q3.

### D9. Dependency

http declares `FetchContent_Declare(task GIT_REPOSITORY https://github.com/Kronuz/task.git GIT_TAG
8cad661e0dd3bd1c74b964cdc0ee42066f568838)` after Asio (`CMakeLists.txt:50-55`), so task's
`ASIO_INCLUDE_DIR` check finds http's Asio and fetches no second copy, and links `task::asio`
into `http`. Xapiand already pins task at that commit (`CMakeLists.txt:734`); Detent adds it to
`cmake/dependencies.json` in W3.

## Gate rows

Build directories live in the worktree; scratch in its `.scratch/`; builds under
`quiet light ... -- nice -n 19 cmake --build ... -j4`.

| Row | Target | Procedure |
| --- | --- | --- |
| T1 API | no `asio::awaitable` in the handler API; no shim | `git grep -n 'asio::awaitable\|as_awaitable\|async_read(' -- http_handler.h http_router.h http_body_stream.h` prints only `push`; the same grep for `as_awaitable` is empty in http, http-log, prism |
| T2 suite | every ctest at head passes; none removed | `cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j4 && ctest --test-dir build`; same names as head's `ctest -N` |
| T3 L3 probes | the review's results hold | copy `review-l3/` to `.scratch/review-l3-task/` (originals read only), port `probe_common.h`'s `App` (`nap` over `task::await_asio`, reads with `read()`), set `H` to this worktree and `RX` to `build-asan/_deps/reactor-src`, add task's include dir; `build.sh P asan` and `tsan` for each: `probe_truncated` 0 of 4 clean eof; `probe_throwcb` streamed and buffered survive; `probe_pipeline` 120 rounds in order, gates 0; `probe_admission` gate back to 0 on every path; `probe_shutdown`, `probe_streamcancel`, `probe_deferred_stop`, `probe_timeouts`, `probe_cap`, `probe_cl` as in the review |
| T4 new tests (`coroutine_test`) | each passes, and fails with its fix reverted | (a) synchronous handler: no asio op (an executor-counting test); (b) a budget loop demotes at least once (`scheduler(0).demoted() > 0` seen), answers, and `demoted()` is 0 after `stop()`; (c) a handler resumed from a `std::thread` finishes on the loop thread; (d) a suspended handler whose connection frame is destroyed touches request and writer afterwards, ASan clean; (e) escaped `task::cancelled` after a disconnect gives 503 + close, gate back to 0; (f) a client hang-up during a budget loop throws `task::cancelled` within one slice; a handler that never asks still costs no watch read |
| T5 sanitizers | 0 reports | separate `build-asan` (`-fsanitize=address,undefined -fno-sanitize-recover=undefined`) and `build-tsan` with Homebrew LLVM (`-stdlib++-isystem` workaround from the L3 ledger); T2 and T3 in each; g++ Debug ASan for T2 too, since g++ takes task's trampoline path |
| T6 throughput | B within the A/A band for req/s and p99 at 8 and 64 connections, kinds `http` and `httpr` | the lead, in one `quiet exclusive` hold: A = S1's `http_server_compare` (head's library, `git diff 679c0f8 S1 -- '*.h'` empty), AA = a second copy of A, B = the branch; Release, `-DHTTP_BUILD_BENCH=ON`; 7 rounds, order rotated per round: `BIN=$s LABEL=$s TMPDIR=.scratch sh bench/run.sh $kind 2 $c 4 >> .scratch/bench/$s/$r.txt`; then `ab_compare.py compare --a .scratch/bench/a --b .scratch/bench/b --aa .scratch/bench/aa --higher-better --regex '^(?P<kind>\S+) .* C=(?P<conns>\d+) +\S+ +req/s +(?P<value>[\d.]+)'`, and the p99 column with `--regex '^(?P<kind>\S+) .* C=(?P<conns>\d+) .*p99 +(?P<value>[\d.]+)(?P<unit>us|ms|s)'`; also report server CPU per request (on the M4, wrk caps req/s near 37,000, CHANGELOG 2026-11-01) |
| T7 frame allocations | B ≤ A allocations per request, kinds `http` and `httpr`, 1, 8 and 64 connections, Apple clang and g++ | S1's `-DHTTP_BENCH_COUNT_ALLOCS=ON` build of A and B; `run.sh` prints `allocs/req` from the `ALLOCS` lines; pinned as a ctest that serves 10,000 keep-alive requests after 1,000 warm-up and asserts the per-request ceiling A measured |
| T8 consumers | build and pass at the branch head | http-log `ctest`; prism builds and its demo serves the 8 routes and stops on SIGINT; Detent configures and builds against the branch; Xapiand builds (the Xapiand lead, after its note) |

## Open questions for the owner

**Q1. Who owns a reactor's scheduler for Xapiand.** Options: (a) http owns it (D2) and Xapiand's
`AsioHost` wraps `service.scheduler(i)` instead of its own (`host_asio.h:49`, `manager.cc:955`);
(b) the service accepts an injected `scheduler_ref` per reactor before `start()`; (c) each
application switches schedulers itself with `task::schedule_on`, two posts per request.
Recommendation: (a). One demoted queue per loop keeps Xapiand's admission count
(`search_views.cc:3029-3030`) true, costs no post, and adds one accessor, not a configuration
path. The Xapiand lead decides with the owner.

**Q2. Whether `Request::cancelled()` stays beside `cancel_token()`.** Options: (a) keep
`cancelled()`, `cancel_reason()` and `on_cancel()` and add `cancel_token()`; (b) remove
`cancelled()` in favor of `cancel_token().requested()`. Recommendation: (a). `cancelled()` is the
loop-thread check that also has a reason beside it; the token is the cross-thread, reasonless view
a budget needs. Removing it churns every consumer and the L3 probes for no new capability.

**Q3. If the bridged loop misses T6 or T7.** Options: (a) ask task for an inline-start primitive
that drops the driver frame, then re-measure; (b) the task loop (D8); (c) accept the measured cost.
Recommendation: (a) first, since it is small and in one library; (b) if it still misses.

## Steps

Each is sized for one developer. http's steps share one implementation branch the lead names;
http-log and prism get their own.

1. **S1 Baseline instrumentation (asio API).** `server_compare.cc`: `HTTP_BENCH_COUNT_ALLOCS`
   (replaced global `operator new` with a relaxed counter, printed beside `RUSAGE`), and a `httpr`
   kind (a `Router` route that yields once with `asio::post`); `run.sh` prints `allocs/req`. No
   library header changes. This is T6's and T7's A.
2. **S2 Dependency (D9).** The task pin and link; CI green on every job; nothing uses it yet.
3. **S3 Handler and router on task (D1-D4, D6, D7).** Signatures, per-reactor scheduler, driver,
   per-connection block, exception mapping; port every test, example and bench handler
   (`test/*.cc`, `examples/*.cc`, `bench/server_compare.cc`; Router tests drive with
   `task::sync_wait`); T4 (a)-(e).
4. **S4 BodyStream and cancellation on task (D1, D5).** `read()`, `cancel_token()`; port
   `coroutine_test` [B] and [H]; T4 (f); T2 and T5.
5. **S5 Probes (T3)** in `.scratch/review-l3-task/`, ASan and TSan.
6. **S6 Docs.** README, ARCHITECTURE, AGENTS, `docs/TRANSPORT.md`, `coroutine-handlers.md`, and a
   CHANGELOG entry with the upgrade note (old signature, new signature, `cancel_token`, removed
   `async_read`).
7. **S7 The lead measures T6 and T7** and rules D8; the task loop, if needed, is its own step.
8. **S8 http-log.** `AccessLog::handle` (`http_log.h:371`) returns `task::task<void>` and awaits
   `inner_.handle` directly; tests' `drive()` (`test/test.cc:35`, `test/completion_test.cc:11`,
   `examples/demo.cc:5`) uses `task::sync_wait`; pin http.
9. **S9 prism.** Its routes and `handle` (`src/main.cc:186-234`) return `task::task<void>`; pin
   http and http-log; T8.
10. **S10 Detent.** W2 writes its handlers on D1; task joins `cmake/dependencies.json`.

## What Xapiand changes (for the Xapiand lead)

1. `search_service.h:54`, `search_views.cc:3069`: `handle` returns `task::task<void>`.
2. `search_views.cc:3146-3147`: `co_await request.decode_large_body()` and
   `co_await request.view(request)` directly; delete `Run::await` (`host_asio.h:131-145`). A debug
   build that keeps a per-request scheduler switches with `task::schedule_on`.
3. `search_views.cc:2944`: `co_await stream.read(chunk)`.
4. `search_views.cc:3079-3085`: the budget takes `hreq.cancel_token()`; `Request::cancel`
   (`search_views.h:195`) goes unless another path needs it. The stream `fail` in the callback can
   stay.
5. `search_views.cc:3148`: the `task::cancelled` catch can go if http's 503 (D7) suits; keep it
   for Xapiand's own body.
6. `manager.cc:955`: with Q1 (a), `AsioHost` wraps `http_service->scheduler(i)`.

## Limits

1. A handler frame over 2 KiB is allocated with `operator new` on every request (`task.hh:255`),
   silently. T7 covers the bench handlers only; SearchService's frame size is not measured.
2. `task::sync_wait` on a reactor thread deadlocks the loop (task README). Nothing detects it.
3. A task producer on another thread resumes the handler by posting to the reactor's scheduler; it
   must not outlive the service (pre-existing for `as_awaitable`). TcpServer waits for sessions,
   so only a reactor destroyed without `stop()` reaches it.
4. D4's block keeps a handler memory-safe after its connection frame dies, but such a handler's
   response goes nowhere and its lease is already released.
5. Nothing here has been built or measured. Every cost statement above is from reading the code.

## Status and what to review

Designed, not built. Review asks:

1. D3 and D4: is the driver plus per-connection block enough to keep the L3 lifetime rule now that
   the handler frame is outside Asio's stack?
2. D5: is "three roles, one signal" the single mechanism the decision asks for?
3. T6 and T7: are A, the kinds and the regexes the right gate for D8?
