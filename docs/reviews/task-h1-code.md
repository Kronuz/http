> Status: CURRENT (2026-11-02) Code review of http step H1 (buffered path on `task::task`).

# Code review: task-h1 (http)

- Branch `phase/task-h1` at http:908f152 (code at http:a54a21d), base http:679c0f8 plus S1 (http:d6f8468).
- Design `docs/design/task-handlers.md` revision 2 (branch `design/task-handlers`): D1-D5, D7-D11, the gate
  rows, step 4; the lead's ruling on the four teardown phases (`docs/status/task-handlers.md:43`).
- Ledger `docs/status/task-h1.md`, including the lead's rulings of 2026-11-03.
- Pins: task task:6558c2e (C3), reactor reactor:4f5a0b5 (step R before its fix round).

## Verdict: APPROVE WITH CONDITIONS

The buffered loop, the handler as a child task, the token map, the deferred and owned write
cancel, D8 and the teardown phases do what the design says. The suite passes 13/13 under ASan+UBSan
and TSan on Linux. Eleven probe scenarios pass under both builds, with zero live task frames after
each one. Counts reproduce the ledger exactly. Two things hold the branch back:

1. Shutdown is unbounded when a handler does not end. This depends on reactor's fix round (CR1).
2. D9's Debug assert aborts correct code that touches the writer from a cancel callback on the loop
   thread (CR2).

The lead's two rulings are not built yet (CR3).

## Conditions (one line each)

1. Move the reactor pin to R's fix-round commit, add T4 (d)'s handler that ignores cancellation, and rerun probes `ignore` and `hop-b`: `stop()` ends within the grace plus 100 ms, with zero live frames after the phases and ASan/TSan clean (CR1).
2. Fix CR2 so that a writer call from a cancel callback on the loop thread passes in Debug, with a test for the hang-up and shutdown cases; T4 (h)'s death test must still fail for a body that moved away.
3. Build the lead's rulings: move `examples/bench_asio.cc` under `bench/` so the T1 grep is empty, and add `Request::scheduler()` with a test (CR3).
4. Correct the ledger's consumer break list against the origin heads below (CR4).

The re-check covers concurrency, so a rechecker reruns this review's probes from `.scratch/review-h1/`.

## Findings

### CR1: `stop()` does not return while a handler never ends (blocking until R's fix round)

- Probe `ignore` sets a 200 ms grace and runs a handler suspended on its own 1 h timer with no
  token. `stop()` had not returned after 5 s (watchdog, rc 3) under both ASan and TSan.
- Probe `hop-b` uses Detent's owner pattern: the handler moves to an owner scheduler with
  `schedule_on` and waits on a `task::event` that a cancel callback sets, through a `task::deadline`
  linked to `request.cancel_token()`. The owner's loop stops first. Shutdown sets the event, but the
  resume is posted to the stopped owner loop, so the session never finishes. `stop()` had not
  returned after 5 s.
- Cause: reactor:4f5a0b5 `TcpServer::stop()` calls `force_all()` and then `wait_sessions()` with no
  bound (`reactor.h:623-624` at that commit). `force()` (`http_asio.h:704`) closes the socket, which
  cannot end a wait that is not on the socket. http's code is not at fault. Design Limit 4 relies on
  R's bounded `stop()`, which the ledger already defers ("Next" 2).
- Control: probe `hop-a` stops the service first, then the owner, and runs each phase on both. All 7
  parked handlers were cancelled and answered within 1 ms, with 0 frames left. Probe `hop-c`
  (`destroy_roots()` alone while the owner runs) took 1 ms and left 0 frames. So the cross-thread
  cancel works (task's `post_call` path), and the order is the only problem.
- For the re-check after R lands: in `hop-b` with a bounded `stop()`, the owner loop still holds the
  posted resume of a frame that http's `destroy_roots()` destroys. The probe must show that nothing
  runs the owner loop again (task Limit 3) and that ASan stays clean.

### CR2: the D9 assert fires on the loop thread when a cancel callback touches the writer (condition)

- Probe `cbwrite`: a handler registers `task::cancel_callback(req.cancel_token(), [&w] {
  w.set_header(...); })` and waits for cancellation. The client half-closes. TSan Debug build:
  `http_asio.h:279: AsioResponseWriter::affine(): Assertion ... failed`, rc 134.
- Probe `cbwrite-stop` does the same with the cancellation coming from `svc.stop()`, and aborts the
  same way.
- Cause: `Request::cancel` runs callbacks on the requesting thread. Here that is the loop thread, but
  inside a raw Asio handler, where `task::current()` is empty:
  - the watch's read completion, `ConnectionInput::completed`/`fire` (`http_asio.h:576-591`);
  - reactor's abort walk, `on_loop_sync`'s posted lambda (`reactor.h:261-272` at 4f5a0b5), which
    reaches `RequestAbort::abort` (`http_asio.h:703`).

  The writer is used on its loop, which is what D9 means, yet the assert compares
  `task::current()` and so rejects it. Release builds are unaffected; every consumer's Debug build
  aborts.
- Two fixes are possible. One is local: `affine()` also accepts no current task while
  `reactor_->on_loop()` holds. The other is wider: the transport and reactor request tokens inside
  the scheduler's scope. That touches reactor's abort walk and is cross-library, so it goes to the
  architect only if the lead prefers it. The same rule will apply to `BodyStream::read` in H2.

### CR3: the lead's rulings are not built (condition)

- `git grep -nE 'asio::awaitable|use_awaitable|awaitable_operators|co_spawn' -- '*.h' '*.cc' ':!bench'`
  still hits `examples/bench_asio.cc:17-70`. K1 stays unticked.
- `Request::scheduler()` does not exist. Tests and examples still find their loop with
  `this_loop(service)` (`test/test_loop.h:11`).

### CR4: the consumer break list was grepped at stale or local heads (condition: ledger only)

Fetched 2026-11-02, and grepped for `asio::awaitable|use_awaitable|co_spawn|tls_connection_factory_timed|CancelReason|on_cancel(|cancelled()|async_read(`:

- **http-log**: origin/main http-log:431b878 is 8 commits ahead of the local http-log:d2315b8 that the
  ledger used, and pins http:679c0f8, not http:fce24c6. The sites are unchanged: `http_log.h:371`;
  `examples/demo.cc:5,8,14`; `test/completion_test.cc:11,14,21`; `test/test.cc:35,38,44,55,244`.
- **prism**: origin/main prism:da169a9 is 5 commits ahead of local and pins http:d76c5ef. List the
  route lambdas by line, `src/main.cc:186,197,207,217,224,229`, plus `:234` and `:324`.
- **Detent**: github/main is Detent:cce3602 (the `origin` remote is a stale local path). The sites
  match the ledger. Also `tests/detent_authority/test_transport_https.cc:136` uses
  `tls_connection_factory` with 4 arguments, which still compiles. Constructors such as
  `HttpAsioService(app, 1, 0, 16, factory, 4)` belong to the earlier-http gap the ledger already
  names.
- **cluster**: origin/main cluster:428d57d is 7 commits ahead of local and pins reactor:ade618d. It
  makes no http use, as the ledger says.
- **Xapiand**: origin/master Xapiand:3fe13a2fb (Oct 28) is 312 commits behind local master
  Xapiand:88892b4cb, which is unpushed.
  - At origin/master the handler is synchronous (`search_service.h:49`) and the factory is at
    `manager.cc:1059`.
  - At local master the ledger's three sites hold. Add `search_views.cc:3075` (the definition),
    `:2951` (`BodyStream::async_read`, which breaks in H2) and `host_asio.h:131-142` (`Run::await`,
    T1's `async_initiate` over a task, design "What Xapiand changes" item 2).
  - Record which head the Xapiand lead migrates from.

### CR5: counts (no finding; K7 reproduced)

`.scratch/review-h1/counts.sh` builds both servers Release with `-DHTTP_BENCH_COUNT_ALLOCS=ON`:

- A is S1 (http:03467b1), from `git archive` into `.scratch/review-h1/a-src`.
- B1 is this branch.

The run was macOS, R=2, 3 s per row. Output is in `.scratch/review-h1/counts-macos.txt`. Ignore the
timing columns: this is a counting build on a loaded host.

| kind | C=1 A / B1 allocs | C=8 | C=64 | handlers/req A = B1 |
|---|---|---|---|---|
| http | 3.00 / 3.00 | 3.00 / 3.00 | 3.00 / 3.00 | 1.00 |
| httpr | 7.00 / 5.00 | 7.29 / 5.03 | 8.24 / 5.49 | 2.00 |
| httpt | 22.00 / 4.00 | 22.29 / 4.00 | 22.91 / 4.00 | 2.00 |

These match the ledger's table to the second decimal (except A httpr C=64, 8.24 here against 8.27),
so B1 ≤ A on every row.

The counting change at http:0085e1b compares like with like. A's `asio::awaitable` frames come from
Asio's recycling allocator, which `ASIO_DISABLE_SMALL_BLOCK_RECYCLING` does not reach. B's task
frames come from task's pool. A miss in either one reaches the same counted `operator new`, and both
builds count handlers with the same `ASIO_CUSTOM_HANDLER_TRACKING` hook.

T7's other kinds (close mode, streamed POST, cancel token with a budget) are step M's, not H1's.

### Non-blocking notes

- `on_error` that throws drops the connection without a response (probe `throw`), and the gate
  returns to 0. This is acceptable. `HttpHandler::on_error`'s comment could say so.
- After R's fix round chains the phases, `HttpAsioService::close()` (`http_asio.h:1124`) no longer
  needs to call `server_.stop()` itself. That is harmless either way.
- The rejected-request write (`http_asio.h:1023`) takes no session token but is bounded at 1 s.
  This is acceptable.

## Probe results (`.scratch/review-h1/`, Linux lease: g++ 13 Debug ASan+UBSan, clang 18 Debug TSan)

The suite passes 13/13 under ASan+UBSan and 13/13 under TSan, with 0 reports.

| Scenario | What it does | ASan+UBSan | TSan |
|---|---|---|---|
| `throw` | throw `std::exception`, throw `int`, throw after suspending (500 on a kept-alive connection); `on_error` throws; write after `end()` | pass, 0 frames | pass |
| `late` | 20 handlers ignore the hang-up for 150 ms after a client RST, then write 1 MiB; gate back to 0 | pass | pass |
| `deferred` | 16 deferred replies, 8 clients reset, producers complete from a thread during `stop()` and after the service is destroyed | pass, `stop()` 9 ms | pass |
| `pipeline` | 200 pipelined requests with random suspends, half of them engaging the token: in order, no token requested; then 2,000 keep-alive requests; gate 0 | pass | pass |
| `token` | escaped tokens outlive their requests; a throwing cancel callback is contained and the next callback runs | pass | pass |
| `tls` | 20 handshakes pending (deadline 10 s) at `stop()` | pass, 2 ms | pass |
| `hop-a` | 8 handlers parked on an owner scheduler; one hang-up crosses threads; service stopped first, each phase on both | pass, 1 ms, 0 frames | pass |
| `hop-c` | `destroy_roots()` alone with handlers parked on a running owner | pass, 0 frames | pass |
| `hop-b` | the owner's loop stops before the service | HANG over 5 s (CR1) | HANG |
| `ignore` | a handler with no token, grace 200 ms | HANG over 5 s (CR1) | HANG |
| `cbwrite`, `cbwrite-stop` | writer touched from a cancel callback | not run (stale ASan binary) | assert, rc 134 (CR2) |

Files:

- `probe_h1.cc`: the scenarios, run as `probe_h1 <scenario>`.
- `CMakeLists.txt`: adds the worktree as a subdirectory, with `TASK_COUNT_FRAMES=1`.
- `run-review.sh asan|tsan`: suite and probes, run via `~/.scratch/detent-lease/remote.sh`.
- `counts.sh` and `a-src/`: the counting builds of A and B1.

Build directories are deleted.

## Not run

- The ledger's 11 mutants were not rerun (the ledger records them).
- macOS sanitizer builds and the FreeBSD and arm64 matrix were left to CI (run 41585840133).
- T3's L3 probes are H2's.
- No timing, and no T6.
- The CR2 scenarios under ASan: the binary was stale. The TSan Debug build shows the assert.
