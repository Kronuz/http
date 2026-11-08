> Status: DRAFT (2026-11-02) Revision 2, designing (ready for preliminary review). Supersedes
> revision 1 (http:b110118), which the architect rejected (`docs/reviews/task-handlers-design.md`).
> Ledger: `docs/status/task-handlers.md`. Workstream W2a of Detent `docs/PLAN.md`.

# http on task: reactor sessions, the connection loop and handlers

Reads http http:679c0f8 (`http_asio.h`, `http_asio_tls.h`, `http_body_stream.h`, `http_message.h`,
`http_handler.h`, `http_router.h`, `bench/run.sh`), reactor reactor:ade618d (`reactor.h`,
`reactor_udp.h`), task task:8cad661, http-log http-log:431b878, prism prism:da169a9, cluster
cluster:428d57d, consensus consensus:23b1e55, Detent Detent:6702e07, Xapiand Xapiand:81c311799 (read
only). Line numbers are at these commits. The task half is task `docs/design/cancellation.md`
(branch `design/cancellation`, task:6d361ca); its decisions are cited as task D1-D11.

## Summary

The owner ruled one shape everywhere: `task::task<T>` is the only coroutine type inside the
libraries and at their API, asio stays the reactor, every consumer migrates in the same wave, and
no alias, shim or second API remains (Detent `docs/SHARED_LIBRARY_DECISIONS.md`, November 2, the
last three sections). Revision 1 kept the connection loop and four public seams on
`asio::awaitable` and was rejected for it. Revision 2 moves all of it:

1. **reactor**, the first step: `Session` returns `task::task<void>` and receives a cancel token
   in its context; each `Reactor` owns one `task::asio_scheduler`; shutdown requests the
   session's token; the accept and UDP loops are tasks (R1-R4).
2. **http**: the connection loop, its factories, `serve_connection`, `serve_tls_connection`,
   handlers, routes and `BodyStream` (read and push) are tasks. A handler is awaited as a child
   task: no post, no driver, no shared block (D1, D2). Timeouts and deferred writes are
   token-cancelled operations (D3, D4, D7). `Request` keeps `cancel_token()` and
   `cancel(reason)`; `cancelled()`, `cancel_reason()` and `on_cancel()` go (D5).
3. **task** supplies the carrier: tokens with reasons and callbacks, deadlines, `await_asio`
   that cancels and resumes inline, and teardown ownership.

The gate is server CPU per request and p99 latency, not req/s, over head, its A/A copy, the task
loop on task without the inline resume (B0) and with it (B1), on a Linux x86-64 host with one
macOS check; allocations and io handlers per request never exceed head's (T6, T7). An asio loop
returns only as a case to the owner if a considerable gap survives task's optimizations; the
threshold is the owner's (Q1).

## What this depends on and changes

| Document | Relation |
| --- | --- |
| Detent `SHARED_LIBRARY_DECISIONS.md`, November 2 (three sections) | Implements them; item 7 of the last is T6 and T7 |
| task `docs/design/cancellation.md` | Depends on task D1-D10; its steps C1-C3 come first |
| `docs/reviews/phase7c-l3-code.md` and its probes (`http-wt/l3-http/.scratch/review-l3/`) | Its constraints are restated against the new mechanism ("The L3 constraints") and its 13 probes are T3 |
| `docs/mechanisms/coroutine-handlers.md`, `docs/TRANSPORT.md` | Rewritten in step H2; the watch, monitor and shutdown rules keep their meaning |
| Detent `docs/PLAN.md` W2 (`:51`) | W2 writes Detent's handlers as `task::task<void>`, not `asio::awaitable<void>`; the lead edits the row |
| Xapiand `docs/design/phase7c.md` D2, D3 | D2's per-loop scheduler becomes the reactor's; D3's cancel arrives as a token |

## Decisions: reactor (step R)

### R1. One `Session` type, on task, with the session's token

```cpp
struct SessionContext { std::chrono::steady_clock::time_point accepted_at; task::cancel_token stop; };
using Session = std::function<task::task<void>(asio::ip::tcp::socket, Reactor&, SessionContext)>;
TcpServer(std::size_t reactors, int admission_limit, Session session, std::size_t maximum_connections = 0);
```

This replaces `Session`, `TimedSession` and the two `TcpServer` constructors (`reactor.h:280-282`,
`:493-505`). The untimed form only drops `accepted_at`; with every signature changing anyway, two
forms are one obvious way too many. A session has no implicit cancellation state any more:
`stop` is the shutdown signal, and the session chooses which operations take it.

### R2. Each reactor owns its loop's scheduler

`Reactor` gains `task::asio_scheduler scheduler_{io_.get_executor()}` and
`task::asio_scheduler& scheduler()`, declared last so it is destroyed first: destroying its roots
(task D8) runs `SessionControl::finish` (`reactor.h:303-305`), which still finds `aborts_`, the
admission gate and the session counter alive, and `io_` (`:263`) outlives it. Every loop has
exactly one scheduler, tests that build a `Reactor` get it, and Detent's non-HTTP loops get it
without http. `HttpAsioService` adds nothing: `service.reactor(i).scheduler()`.

### R3. Shutdown requests the session token

`SessionControl` (`reactor.h:287-306`) holds a `task::cancel_source` instead of an
`asio::cancellation_signal`; `abort()` requests it with `task::cancel_reason::shutdown`.
`abort()` runs on the loop on every path (`abort_all` walks there through `on_loop_sync`,
`:218-221`; a late `track` aborts there, `:211`), so the request and the callbacks it runs
(task D5's emit) stay on the loop, and the posted lambda with its `finished` check (`:298-301`)
goes. `launch_session` (`:307-324`) spawns one task on the target reactor's scheduler that tracks
the control, checks `aborted` and awaits the session as a child, with `control->finish()` as the
spawn's completion: one post per connection where today there are two (the post and `co_spawn`'s).
Expected, not measured.

### R4. The accept loops and the UDP receive loop are tasks

`accept_loop`, `shared_accept_loop` and `AcceptControl::retry` (`reactor.h:361-370`, `:412-482`)
await `async_accept` and the retry timer through `await_asio`; `stop_on_owner` closes the
acceptor as today, which ends the pending accept. `UdpServer::receive_loop`
(`reactor_udp.h:205-226`) is spawned on the reactor's scheduler; after `stop()` its suspended frame
is destroyed by the scheduler (task D8), where today the stopped io_context does it (`:223-224`).
reactor declares task (`FetchContent`, at the commit of task step C3) and links `task::asio`.

## Decisions: http (steps H1, H2)

### D1. Public signatures

```cpp
// http_handler.h, http_router.h
virtual task::task<void> handle(Request& request, ResponseWriter& response) = 0;
using Route = std::function<task::task<void>(Request&, ResponseWriter&, const Params&)>;
// http_body_stream.h
task::task<asio::error_code> read(std::string& chunk, task::cancel_token token = {});
task::task<bool> push(std::string chunk, task::cancel_token token);   // the transport's side
// http_message.h (D5)
task::cancel_token cancel_token() const;
void cancel(task::cancel_reason reason);
// http_asio.h, http_asio_tls.h
using ConnectionFactory = std::function<task::task<void>(asio::ip::tcp::socket, HttpHandler*,
    reactor::Reactor*, const AsioResponseOptions*, reactor::SessionContext)>;
HttpAsioService(HttpHandler&, std::size_t reactors, std::size_t in_flight_limit,
    ConnectionFactory connection = {}, std::size_t maximum_connections = 0);
ConnectionFactory tls_connection_factory(std::shared_ptr<asio::ssl::context>,
    std::chrono::milliseconds handshake_deadline = 5s, std::size_t maximum_handshakes = 64,
    std::size_t maximum_connections = 1024, TlsFailureHook failed = {});
```

These replace `http_handler.h:183`, `http_router.h:120` and `:128`, `BodyStream::async_read` and
`push` (`http_body_stream.h:60-75`), and, merged as R1 merges reactor's, the two factory types and
two constructors (`http_asio.h:1010-1032`) and the two TLS factories (`http_asio_tls.h:107-138`).
`detail::serve_connection` and `detail::serve_tls_connection` are tasks. `read()` keeps today's
completion values (`http_body_stream.h:53-59`) and returns the code rather than throwing, the
eof-versus-failure contract L3-B1 fixed. Its token is the handler's own (a deadline, its source);
an empty token adds none, since the transport already fails the stream on disconnect and shutdown
(D6). The writer side does not change: `ResponseWriter`, `DeferredReply` and `OwnedStreamReply`
are synchronous on the loop or thread-safe submits (`http_handler.h:88-130`).

### D2. The handler is a child task

The connection awaits `co_await handler->handle(request, writer)`. task starts the child by
symmetric transfer (`task.hh:441-446`) and the child's final suspend transfers back on the same
scheduler (`:378`), so a handler that finishes in its first run costs no post. Revision 1's
driver, channel and per-connection `shared_ptr` block go. `Request`, the writer and the
`BodyStream` stay in the connection frame, declared before the awaited handler, so a teardown
destroys the handler frame first; otherwise frames end only by completing.

### D3. Which operation takes which token

The session token (R1) reaches only the waits that shutdown should end, and the handler never
sees it. This makes structural what asio's per-coroutine state did at twelve sites:

| Site | New |
| --- | --- |
| `reactor.h:296-301`, `:318-320`: slot emit at shutdown | R3: the session source is requested |
| `http_asio.h:507` read before deadline; `:546-555` `expire_read` | D4: the read takes a deadline linked to the session token; a token callback cancels the pending monitor read, as `expire_read` does |
| `http_asio.h:356` write before deadline | D4: the write takes a deadline with no session link (writes finish) |
| `http_asio_tls.h:96` handshake deadline | D4: a deadline linked to the session token |
| `http_asio.h:890` feed versus handler done | D6: `when_all`; handler completion requests the feed's source |
| `http_asio.h:605-607`, `:642` writes bound to `write_cancellation()` | D7: the reply control's source, linked into the write's deadline |
| `http_asio.h:127`, `:581-582`, `:670` cancellation-state reads after a wake | removed: shutdown reaches replies through `AsioDeferredControl::abort` (`:117`) closing the exchange; `HandlerDone` goes (D6) |
| `http_asio.h:897`, `:953` cancellation disabled while the handler runs | structural: the handler-phase reads and writes take no session token |
| `http_asio.h:922`, `:967` terminal-only between requests | structural: the next request's reads take the session token |
| `http_asio.h:996` disabled for the final drain | structural: `drain()` takes no token |

### D4. Timeouts are deadlines

The idle, header, request and inactivity bounds (`http_asio.h:779-784`, `:816-833`, `:869`,
`:931`) become `task::deadline` objects (task D4) on the reactor's scheduler, linked to the
session token for reads and the TLS handshake, and passed to `await_asio`.
`ConnectionInput::read(output, token)` replaces `read_before`, `read_completed` and `expire_read`
(`:505-555`): it registers a `task::cancel_callback` that cancels the socket's pending monitor
read (`stream_.lowest_layer().cancel()`), then drains as today. A timed-out read ends with
`operation_aborted` and `reason() == deadline`, which the loop maps to `timed_out` where it needs
the distinction.

### D5. Request cancellation: one mechanism, the token

| Role | Mechanism |
| --- | --- |
| Detect a hang-up | `CancelWatch`, engaged lazily (`http_message.h:56-61`, `http_asio.h:455-467`): unchanged |
| Signal | `Request::cancel(reason)` requests the request's source, first call wins, on the loop |
| Observe, polled or woken | `Request::cancel_token()` creates the source on first call (pooled, task D1) and engages the watch; woken observers register a `task::cancel_callback` on it |

`cancelled()`, `cancel_reason()`, `on_cancel()` and `CancelReason` (`http_message.h:50`,
`:141-153`) go. Shutdown is `task::cancel_reason::shutdown`; http defines `http::disconnected =
task::cancel_reason::user`. task contains a throwing callback (task D2), which keeps L3-B2's
rule. The watch rules do not change: a disconnect cancels reads, not writes, and a pipelined byte
ends the watch for that request. A handler writes `task::budget slice(200us, request.cancel_token())`.

### D6. The streamed path joins with `when_all`

```cpp
task::cancel_source feed_stop{context.stop};   // linked: shutdown also stops the feed
co_await task::when_all(feed(feed_stop.token()), run_handler(handler, request, writer, feed_stop));
```

`run_handler` awaits the handler, maps its exception (D8), then requests `feed_stop`, so a handler
that answers early stops the feed's read and push. `feed` fails the stream on a failed body
(`http_asio.h:854-857`, still `connection_reset` for a truncated one) and, after a complete body
with the handler still running, sets the disconnect watch (`:905`). Both are children of the
connection frame, started inline by `when_all` (`task.hh:1036-1044`), which rethrows after both
finish (`:1060`): no spawn, no post, no `HandlerDone`, and with task D9 no allocation.

### D7. Deferred and owned writes

`AsioDeferredControl` (`http_asio.h:109-151`) holds a `task::cancel_source` instead of the
`asio::cancellation_signal`; `notify()` requests it when the exchange is closing (`:143`) and
cancels the timer as today. `wait()` awaits its timer through `await_asio`. The writes in
`write_response` and `write_owned_response` take a deadline at `exchange->deadline()` linked to
the control's token, replacing `co_spawn(write_before_deadline, bind_cancellation_slot(...))`.

### D8. Exceptions

| Escapes `handle()` | Result |
| --- | --- |
| any exception, response unanswered | `map_handler_error` (`http_asio.h:650-659`): `on_error`, then 500 |
| `task::cancelled` while the request's source is requested, unanswered | `on_error` first; if still unanswered, 503 with `Connection: close` (Xapiand's mapping, `search_views.cc:3146-3150`, moved into http) |
| `task::cancelled` from the handler's own source | as any exception |
| any, deferred reply | abandoned, as today |
| any, owned stream whose head is on the wire | the connection closes without the terminal `0\r\n\r\n`, so the client reads a truncated body, never a complete one |

"Started" means ended, deferred or an owned stream: a buffered reply is not on the wire until the
handler returns.

### D9. Affinity

`ResponseWriter` calls and `BodyStream::read` assert in Debug that `task::current()` is the
connection's scheduler. A child that moves with `task::schedule_on` comes home at its final
suspend (`task.hh:378-381`); a handler that moves its own body to another scheduler and then
touches the writer fails the assertion.

### D10. Dependencies

http declares task after Asio (`CMakeLists.txt:50-55`) at the commit of task step C3, moves
reactor (`:65`) to step R's commit, and links `task::asio`. Step S2 (in progress) plumbs the task
dependency at task:8cad661; H1 moves the pin.

### D11. No asio loop

The task loop is the design. Revision 1's bridged loop is not built. An asio loop returns only as
a case to the owner, with before and after numbers, if B1 crosses the "considerable" threshold
(Q1) after task's optimizations (task D6, D9, and whatever T6 and T7 point at next).

## The L3 constraints on the new mechanism

| Constraint (Detent decision, "One coroutine type", item 3) | How it holds | Check |
| --- | --- | --- |
| Request, writer and `BodyStream` outlive a destroyed frame | the handler is a child declared after them (D2); teardown destroys children first; frames otherwise end by completing | T4 (d), `probe_shutdown`, `probe_throwcb` |
| A disconnect cancels reads, not writes | the watch requests the request's source; writes take no request or session token (D3) | `probe_streamcancel`, `probe_admission` half-close |
| A pipelined byte ends monitoring | `ConnectionInput` unchanged | `probe_pipeline`, `probe_timeouts` |
| The `AdmissionGate` count is exact on every exit | the lease stays in the connection frame (`http_asio.h:747`), released by completion or root destruction | `probe_admission` |
| A throwing cancel callback is contained | task D2 | `probe_throwcb` |
| A truncated body is an error, not eof | `fail_body` unchanged; a cancelled `read()` returns `operation_aborted`, never eof | `probe_truncated` |

## Gate rows

Builds in the step's worktree: `build` (Debug), `build-asan` and `build-tsan` (Homebrew LLVM with
the L3 ledger's `-stdlib++-isystem` workaround), and g++ Debug ASan; each
`quiet light w2a-build 20 "<dir>" -- nice -n 19 cmake --build <dir> -j4`; delete them after.

| Row | Target | Procedure |
| --- | --- | --- |
| T1 One shape | no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http, http-log or prism headers and sources (the benches' asio comparators excepted), nor in reactor's after step R; no `async_initiate` over a task in Xapiand; `as_awaitable` gone from task | `git grep -nE 'asio::awaitable\|use_awaitable\|awaitable_operators\|co_spawn' -- '*.h' '*.cc' ':!bench'` in each repo; `git -C Xapiand grep -n async_initiate` shows no task; task T7 |
| T2 Suites | every ctest at head passes; none removed | `ctest -N` names equal head's for http (`CMakeLists.txt:125-195`) and reactor; `ctest --test-dir <dir> --output-on-failure` in each build |
| T3 L3 probes | `probe_truncated` 0 of 4 clean eof; `probe_throwcb` streamed and buffered survive; `probe_pipeline` 120 rounds in order, gates 0 (TSan 40); `probe_timeouts` idle ≤ 410 ms for 400, header ≤ 310 ms for 300; `probe_admission` gate back to 0 on every path the review lists, the next 20 admitted; `probe_shutdown` 300 idle keep-alive connections stop within 50 ms (review: 13 ms), stalled streamed uploads released at 0 ms, `stop()` at the grace with a client that never reads (review: 405 ms for 400), no handler frame outlives `stop()` in 25 storm rounds; `probe_streamcancel` `read_err` 0; `probe_deferred_stop` four modes clean; `probe_cap`, `probe_cl` boundary exact; `probe_abortlist` 20,000 walks clean; `probe_acceptrst`, `probe_poststorm` listener survives (macOS) | copy `http-wt/l3-http/.scratch/review-l3/` to the worktree's `.scratch/review-l3-task/` (originals read only); edit `build.sh`'s hard-coded `H` (`:4`) and add task's include directory; port `probe_common.h`'s `App` to task handlers, the `req.cancelled()` assertions (`probe_common.h:109-116`, `probe_streamcancel.cc:11-14`) to `cancel_token().requested()`, and the two reactor probes to `Session`; build each under `quiet light` for `asan` and `tsan`; record every result in the ledger |
| T4 New tests (`coroutine_test`, reactor `test.cc`) | each passes, and fails with its fix reverted | (a) a handler that never suspends adds 0 io_context handlers (an executor-counting test); (b) a budget loop demotes (`reactor(0).scheduler().demoted() > 0` seen), answers, and `demoted()` is 0 after `stop()`; (c) a handler resumed from a `std::thread` finishes on the loop thread; (d) `stop()` with handlers suspended in a read, an event and a checkpoint, then the service destroyed: `task::detail::live_frames()` at its baseline, ASan clean; (e) `task::cancelled` escaping after a disconnect gives 503 + close, gate back to 0; (f) a hang-up during a budget loop throws `task::cancelled` within one slice, and a handler that never asks costs no watch read; (g) an owned stream started, then `task::cancelled` escapes: no terminal chunk, gate 0; (h) a child that `schedule_on`s away passes, a body that does asserts (Debug); (i) reactor: an idle session's read ends within one loop pass of `abort_all` (the architect's `cancel` probe as a test) |
| T5 Sanitizers | 0 reports | T2, T3 and T4 in `build-asan` and `build-tsan`; T2 in g++ Debug ASan; the Linux CI ASan job (LSan) |
| T6 Server cost | B1 against A on server CPU per request and p99, 8 and 64 connections, kinds `http` and `httpr`; verdict by Q1's threshold (pending) | the lead. Binaries, Release, `-DHTTP_BUILD_BENCH=ON`: A = S1's `http_server_compare` (head's library; `git diff 679c0f8 <S1> -- '*.h'` empty); A' = a second build of A; B0 = the branch with `-DFETCHCONTENT_SOURCE_DIR_TASK=<task at C2's last commit>`; B1 = the branch at C3's. Linux x86-64 (the gate host): the lead's qemu VM, each round `QUIET_VM=<qemu pattern> quiet strict w2a-t6 90 "W2a T6" -- ssh <vm> 'BIN=$s LABEL=$s sh bench/run.sh $kind 2 $c 4'`, 7 rounds, order rotated, output in `.scratch/bench/linux/$s/$r.txt`. macOS check: one 7-round set on the i9 under `quiet exclusive`. Compare with `cpp-libraries/tools/ab_compare.py compare --a A --b B1 --aa A'` on `--regex '^(?P<kind>\S+) .* C=(?P<conns>\d+) .*server CPU (?P<value>[\d.]+) us/req'` (lower is better, `bench/run.sh:38-39` prints it) and on the p99 column; req/s reported, not gated; B0 reported to show what C3 bought |
| T7 Allocations and io handlers per request | B1 ≤ A exactly, for `operator new` and io_context handlers per request; kinds `http`, `httpr`, a `cancel_token()` plus budget kind, a streamed POST kind, and `close` mode (one request per connection; the connection frame holds `char tmp[64 * 1024]`, `http_asio.h:765`, so it is heap-allocated per connection in A and B alike); 1, 8 and 64 connections; Apple clang and g++ | S1's counting build of A, B0 and B1; pinned as a ctest that serves 10,000 keep-alive requests after 1,000 of warm-up and asserts A's per-request counts. A count over A is a defect, never a case |
| T8 Consumers | build and pass at the branch heads | http-log `ctest`; prism builds, serves its 8 routes and stops on SIGINT; cluster and consensus build and `ctest` at the reactor pin; Detent configures, builds and runs its suite; Xapiand by the Xapiand lead (docs E2E, 641 assertions; `stall_load.py`; the Lua flood) |

The "considerable" threshold (Q1) is pending with the owner. The architect's proposal, cited and
not adopted: a gap goes to the owner when B1 is worse than A by more than 5% on server CPU per
request, or 10% on p99, beyond the A/A band edge on both hosts after task's optimizations; a 3-5%
CPU gap lands as an open optimization target; allocations or handlers per request over A are
defects. Its arithmetic: one post per socket completion costs 42-148 ns (task README) and a
keep-alive request has two completions, 0.5-3% of the 10-22 us of server CPU per request
(`CHANGELOG.md:9-23`).

## Review response

| ID | Finding | Answer |
| --- | --- | --- |
| TH-L1, TH-R1 | Factories, `push`, `serve_connection`, `serve_tls_connection`, `reactor::Session` stay `asio::awaitable` | All tasks (R1, D1); an asio loop only as Q1's case (D11) |
| TH-L2, TH-R2 | No cancellation carrier | task D1-D5 (token, callbacks, linking, deadlines, `await_asio` with a token); the twelve sites mapped (D3); reactor on the token (R3); a test per site: T4 (i), T3 |
| TH-L3, TH-R3 | Driver and block solve the bridged loop; the hazard moves to teardown | Handler as a child (D2); the teardown rule (task D8); the streamed primitive is `when_all`, no post (D6); T4 (d) is the zero-live-frames test |
| TH-L4, TH-R8 | T6 cannot see what it gates | T6 gates server CPU per request and p99 over A, A', B0, B1; threshold Q1, the proposal cited as pending |
| TH-R4, C1 | The inline resume needs a scope | task D6, with both tests in task T1 |
| TH-R5 | The reactor owns the scheduler | R2 |
| TH-R6 | One mechanism; a generic reason; a lazy source | D5; `task::cancel_reason`, http adds `disconnected`; the source is lazy and pooled (task D1) |
| TH-R7, C6 | 503 after a started response; the owned stream | D8, T4 (g) |
| TH-R9, C2 | T7 measures the wrong kinds | T7 adds the token-and-budget kind, a streamed kind, `close` mode and io handlers per request |
| TH-R10, C3 | Thirteen probes with numeric targets | T3 |
| TH-R11, C4 | T1 misses the bridges | T1 as proposed; `as_awaitable` removed (task D10) |
| TH-R12, C5 | Affinity | D9, T4 (h) |
| TH-R13, C7 | Order and consumers | Steps; T8 adds cluster and consensus |
| Xapiand notes, D8 | The task loop by default; optimize task first | D11; the inline resume is task step C3, measured as B0 against B1 |
| Xapiand notes, seams | reactor's `Session` is a step, not out of scope | R1-R4, the first step after task |
| Xapiand notes, Q1 | The reactor owns the scheduler; `AsioHost` wraps it | R2; "What Xapiand changes", item 6 |
| Xapiand notes, Q2 | One way to ask about cancellation | D5 |
| Xapiand notes, Q3 | Optimize first; an asio loop only with numbers | D11, Q1 |
| Xapiand notes, migration | Six items, plus `Run::await` and `as_awaitable` | "What Xapiand changes" |
| Xapiand notes, L3 | Rerun every probe and record each result | T3 |

## Open questions for the owner

**Q1. The "considerable" threshold.** Pending; the Xapiand lead puts it to the owner (decision
item 7). Options: (a) the architect's proposal above; (b) a tighter bound; (c) none, any gap is a
case. Recommendation: (a). 5% of the i9's 10 us per request is 0.5 us, or 0.05 cores at 100,000
req/s, several times what the known mechanism explains. It blocks only the verdict after T6.

**Q2. Detent's own `asio::awaitable` code.** Outside http, 11 Detent files use `asio::awaitable`
(`detent_delivery` client, coordinator and completion scheduler, `https_client.hh`,
`managed_host`, `authority/service.cc`; `git grep -c` at Detent:6702e07). Options: (a) a separate
Detent workstream after W2a, beside M1b's coroutine drivers; (b) inside this wave; (c) exempt.
Recommendation: (a). The rule covers it, but it is Detent's delivery protocol, not a consumer of
http's seams, and folding it in doubles the wave. Not blocking.

task's questions (`lock_until`, loop-local tokens, `inline_scheduler` roots) are in task
`docs/design/cancellation.md`.

## Steps

Dependency order; each step is one developer on its own branch and ledger. Landing order: task,
reactor, http, http-log, prism, then the consumers.

1. **S1 baseline instrumentation, S2 task dependency** (in progress on `phase/task-handlers-s1`,
   `http-wt/task-handlers-s1`): S1 is T6's and T7's A, and adds T7's new kinds (C2).
2. **C1, C2, C3 task** (task design, "Steps"): tokens and deadlines; `await_asio` with a token and
   teardown (B0's pin); the inline resume (B1's pin). The Xapiand lead reviews the task diff once.
3. **R reactor** (R1-R4): `Session`, the scheduler, `SessionControl`, the accept and UDP loops;
   reactor's tests and examples ported; T4 (i); `probe_abortlist`, `probe_acceptrst` and
   `probe_poststorm` (T3) on reactor; pin task.
4. **H1 http, buffered path** (D1-D5, D7-D10): handler and router API, the loop, factories and
   TLS, deadlines, deferred and owned writes, the `Request` token API, exceptions, affinity; every
   test, example and bench handler ported; T4 (a)-(h) without the streamed cases.
5. **H2 http, streamed path and proof** (D1's `BodyStream`, D6): `read` and `push`, the
   `when_all` join; T3 (all 13 probes), T5; docs (README, ARCHITECTURE, AGENTS, `TRANSPORT.md`,
   `coroutine-handlers.md`) and a CHANGELOG upgrade note (old and new signatures; removed
   `cancelled()`, `cancel_reason()`, `on_cancel()`, `CancelReason`, `async_read`; the merged
   factories).
6. **M the lead measures** T6 and T7 (A, A', B0, B1); a case goes to the owner only under Q1.
7. **L http-log**: `AccessLog::handle` (`http_log.h:371`) returns `task::task<void>` and awaits
   `inner_.handle`; the tests' `drive()` (`test/test.cc:35`, `test/completion_test.cc:11`,
   `examples/demo.cc:5`) uses `task::sync_wait`; pin http.
8. **P prism**: routes and `handle` (`src/main.cc:186-234`) return `task::task<void>`;
   `tls_connection_factory` at `:324`; pin http and http-log; T8.
9. **D Detent**: pins http (from http:6a07f99, whose handlers are synchronous), reactor, task and
   http-log; W2's three handlers as `task::task<void>`; `ConnectionFactory` and
   `tls_connection_factory` at `src/detentd/main.cc:244`, `:261` and `src/detent_relay/host.cc:244`.
10. **K cluster and consensus**: the reactor pin; `bus.h` (`UdpServer`) and `reactor_tls.h` keep
    their APIs; build and `ctest`.
11. **X Xapiand**: the Xapiand lead, below.

## What Xapiand changes (for the Xapiand lead)

1. `search_service.h:54`, `search_views.cc:3069`: `handle` returns `task::task<void>`.
2. `search_views.cc:3146-3147`: `co_await request.decode_large_body()` and
   `co_await request.view(request)` directly; delete `Run::await` (`host_asio.h:131-145`). A debug
   build that keeps a per-request scheduler switches with `task::schedule_on`; `Run`'s posted
   `Resume` wrappers are not roots, so a stopped loop leaks them in debug builds.
3. `search_views.cc:2944`: `co_await stream.read(chunk)`.
4. `search_views.cc:3079-3085`: `request.cancel` becomes `task::cancel_source{hreq.cancel_token()}`
   (linked, task D3), or the budget takes `hreq.cancel_token()` directly; the `stream->fail` goes,
   since the transport fails the stream on disconnect and shutdown (D6).
5. `search_views.cc:3148`: the `task::cancelled` catch can go if http's 503 (D8) suits.
6. `manager.cc:950-956`: `AsioHost` wraps `http_service->reactor(i).scheduler()` by reference
   instead of owning one (`host_asio.h:98`); tasks that must die with the loop are spawned on that
   scheduler, since a `scheduler_ref` over `AsioHost` carries no root list (task D7, D8).
7. Teardown order: WorkGroups cancelled and chores drained (`pool.cc`, `ChoreAwaiter`) before the
   service stops and destroys its reactors (task D8.3).
8. `manager.cc:473`, `search_views.cc:2944`: `await_asio` now resumes inline (task D6); check that
   nothing relied on the queue trip.
9. One commit on master moving the task, reactor, http and http-log pins; rerun the docs E2E (641
   assertions), `stall_load.py` and the Lua flood.

## Limits

1. A handler frame over 2 KiB is allocated with `operator new` on every request (`task.hh:255`),
   silently; T7 covers the bench kinds, not SearchService.
2. `task::sync_wait` on a reactor thread deadlocks the loop; nothing detects it.
3. Teardown is correct only in the owner's order (task D8.3); a root suspended on a loop that still
   runs is a use after free, silent in release builds.
4. A session that ignores `SessionContext::stop` does not end at shutdown until `force()` closes its
   socket after the grace: what asio's implicit slot did is now each factory's duty. http's
   factories honor it; a consumer's own factory must, silently otherwise.
5. Nothing here is built or measured. Costs are from reading the code; the probe numbers are the
   architect's (task:8cad661) and the L3 review's.

## What to review

1. D3: does the token map cover every site where asio's per-coroutine cancellation acted?
2. R1-R3: the merged `Session` with the token in its context as reactor's public shape.
3. D6: `when_all` as the streamed join, against the L3 rule that a handler and what it references
   end only by completing.
