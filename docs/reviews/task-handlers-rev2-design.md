> Status: CURRENT (2026-11-02) preliminary design review of W2a revision 2: http
> `docs/design/task-handlers.md` at http:209ceef with the done criteria at http:b2245de, and task
> `docs/design/cancellation.md` at task:6d361ca. Re-check of `docs/reviews/task-handlers-design.md`
> (revision 1, REJECT).

# W2a review: revision 2, task cancellation and http on task (preliminary design)

- Date: 2026-11-02.
- Reviewed: http `docs/design/task-handlers.md` at http:209ceef and `docs/status/task-handlers.md`
  at http:b2245de (branch `design/task-handlers`); task `docs/design/cancellation.md` at
  task:6d361ca (branch `design/cancellation`); the code they cite at task:8cad661 (`task.hh`,
  `task_asio.hh`, `test/asio_test.cc`), reactor:ade618d (`reactor.h`, `reactor_udp.h`), http:679c0f8
  (`http_asio.h`, `http_body_stream.h`, `bench/run.sh`, `bench/server_compare.cc`), Xapiand:81c311799
  (`src/engine/host.h`, `src/engine/host_asio.h`, `src/database/pool.cc`), Detent:6702e07, consensus
  and cluster at their heads; asio 1.36.0 (`detail/reactive_socket_service_base.hpp`,
  `detail/deadline_timer_service.hpp`, `experimental/detail/channel_service.hpp`, `impl/write.hpp`,
  `ssl/detail/io.hpp`).
- Checked against: Detent `docs/SHARED_LIBRARY_DECISIONS.md`, the last three sections ("Coroutine
  drivers", "One coroutine type" with the owner's one-shape ruling and its refinement, "task
  cancellation, inline resume and reactor sessions", items 1-7); the revision-1 review
  (`docs/reviews/task-handlers-design.md`, TH-L1 to TH-L4, TH-R1 to TH-R13, C1 to C7) and its
  probes (`.scratch/arch-review/`); the Xapiand lead's notes on both designs
  (`docs/reviews/task-handlers-xapiand.md`; task `docs/reviews/cancellation-xapiand.md`, folded in
  below as XN1-XN3); the L3 probes (`http-wt/l3-http/.scratch/review-l3/`, read only).
- Checked by: probes in `.scratch/arch-review-rev2/` (`probe_rev2.cc`, `build.sh`; `src/` holds the
  task:8cad661 headers, identical to `git show 8cad661:task.hh`), built under `quiet light` with
  Apple clang `-O2` (`rel`), Homebrew clang ASan+UBSan (`asan`) and TSan (`tsan`), asio 1.36.0, on
  the i9; results in `.scratch/arch-review-rev2/results.txt`. The T6 regex was checked against a
  line in `bench/run.sh:39`'s format.
  - Not run: nothing in either design is built, so no test, T-row or benchmark was run; the
    revision-1 probes were not rerun (their numbers are the baseline the T3 rows target); no g++
    build; no fuzzing; the L3 probes; the M4.

## Verdict

**task `docs/design/cancellation.md`: REJECT. Two defects in the mechanism itself: D5's order
(initiate, then register) with D6's inline resume runs a coroutine to its end inside its own
`await_suspend`, and D8's teardown contract is per scheduler while frames, guards and I/O objects
cross schedulers. Both have small, exact fixes; the revision that folds them in needs a scoped
re-check of D5, D6 and D8 only.**

**http `docs/design/task-handlers.md` revision 2: APPROVE WITH CONDITIONS. It closes TH-L1, TH-L2
and TH-L4; TH-L3 stays open on one item, the cross-reactor teardown order (TH2-L1), which is the
reactor half of task's CN-L2. Step R may start now against task C2's interface; H1 may start
against C2 and R's interfaces; nothing lands before task's re-check clears.**

What holds, and reproduces:

- One shape. reactor's `Session` (R1), the factories, `serve_connection`, `serve_tls_connection`,
  `BodyStream::read` and `push` (D1) are tasks, and D11 makes the asio loop a case to the owner,
  not a design option. T1's grep covers http, http-log, prism and reactor, plus Xapiand's
  `async_initiate` bridge and task's `as_awaitable` (closes TH-L1, TH-R1, C4).
- The cancellation carrier. One mechanism: the token gains a reason and `std::stop_callback`
  lifetime (task D1, D2), deadlines are linked token sources (D3, D4), `await_asio` takes a token
  (D5), `SessionControl` requests the session source (R3), and the twelve revision-1 sites are each
  mapped (http D3; checked against `http_asio.h:740-1000` at http:679c0f8). Polling stays one
  relaxed load behind a null check (`task.hh:624` today, D1 the same), and an empty token
  allocates nothing and binds no slot (D5.1).
- Slot mapping per operation. Every operation the loop awaits honors `terminal | partial |
  total` (asio 1.36.0 `reactive_socket_service_base.hpp:719-721`, `deadline_timer_service.hpp:318-320`,
  `channel_service.hpp:273-275`); the composed write and the TLS operations filter to terminal
  (`impl/write.hpp:372`, `ssl/detail/io.hpp:271`, `:286`), which `cancellation_type::all` includes.
  Emitting `all` (D5.3, reactor's choice today, `reactor.h:300`) is right for each.
- A cancel that loses the race is harmless. Probe `race`: a read whose completion is already
  queued when the slot is emitted completes with success and its 4 bytes, and a second emit after
  the handler ran is a no-op, in all three builds. D5 must say so (CN-R7).
- The handler is a child task (D2): no post, no driver, no shared block; `when_all` for the
  streamed join (D6) matches today's semantics, including the feed taking the shutdown token
  (`http_asio.h:891`, enabled until the reset at `:897`).
- T6 gates server CPU per request and p99 over A, A', B0 and B1 on the Linux gate host with a
  macOS check (closes TH-L4, TH-R8); the CPU regex parses `bench/run.sh:39`'s line into kind,
  connections and value. T7 adds the kinds and the io-handler count TH-R9 asked for (C2).
- Q1 (`lock(token)`), Q2 (loop-local tokens) and Q3 (no roots on `inline_scheduler`): (a) each,
  as the designer and the Xapiand lead recommend ("Rulings").

What blocks:

- CN-L1 (CN-R1): D5.2 registers the cancel callback after initiating, and D6 resumes inline from
  the handler; a completion that runs inside initiation then ends the frame before
  `await_suspend` returns (probe `dispatch_inline`).
- CN-L2 (CN-R2, CN-R3, XN1): D8's contract orders one scheduler's teardown, not several. A root
  suspended on another loop destroys I/O objects of a destroyed `io_context` (probe `crossroot`), a
  dying guard holder posts to a destroyed scheduler (probe `holder`), and a waiter granted but not
  yet resumed leaves the mutex locked forever (probe `grant_destroyed`).
- TH2-L1 (TH2-R1): reactor's R2 inherits CN-L2: "declared last, destroyed first" orders one
  `Reactor`; `TcpServer` destroys its reactors one by one.

## Findings: task (`docs/design/cancellation.md`, task:6d361ca)

### CN-R1. Initiate-then-register plus the inline resume ends the frame inside `await_suspend` (blocking)

D5.2 initiates with the bound slot, "then registers the callback". D5.4 has the handler destroy
the registration, then resume, and D6 resumes inline when the handler runs on the loop. The
design itself names completions that run during initiation ("an immediate completion, a
dispatch", D6), and `await_asio`'s contract already allows one (`task_asio.hh:166`). In that
case:

1. The handler runs inside `init_`, finds no registration to destroy, and resumes the coroutine
   inline (depth 1 < 4). The coroutine runs to its end; its root ends and frees the frame.
2. `init_` returns into `await_suspend`, which registers the callback on an awaiter that no longer
   exists.

Probe `dispatch_inline` builds exactly that order (a prototype awaiter, the `running_on` scope
and depth bound of D6, an `asio::dispatch` on the loop as the initiation): ASan reports
use-after-poison in `one_op` (`.resume`): the write lands in a frame the task pool has already taken back and poisoned (`task.hh:279`); the `rel` and TSan builds run on silently. Without the inline resume the same order still breaks D5.3's invariant:
the late registration can post an emit step after the handler has decided to resume, so the frame
can end under a posted emit.

Fix, all in D5 and D6:

1. Register before initiating. A token already requested completes the await at once with
   `operation_aborted` and initiates nothing (`await_ready` or `await_suspend` returning false).
2. The handler records its result and a park/wake handshake, the shape `lock_waiter` already has
   (`task.hh:767-774`): a handler that runs before `await_suspend` has returned only stores its
   result, and `await_suspend` then returns false and continues in place. Only a handler that runs
   after the park resumes, inline (D6) or posted.
3. The depth bound then bounds what remains: a handler invoked from inside another inline resume
   (a dispatch from a resumed coroutine).
4. T1 gains: an `Init` that completes through `asio::dispatch` on the loop and one that calls the
   handler directly, each with and without a token, each ending its root in the continuation,
   under ASan; the D6 "chain of 100 nested completions" test is driven by dispatches from resumed
   coroutines, since initiation no longer nests.

### CN-R2. The teardown contract is per scheduler; frames and I/O objects cross schedulers (blocking)

D8.2 destroys a scheduler's roots in its destructor; D8.3 asks the owner to stop every scheduler a
root may be suspended on "before destroying any". Stopping is not enough, because destruction is
still ordered scheduler by scheduler:

- A root on loop A whose chain is suspended on loop B (a `schedule_on`, a lock or event wait
  there, a timer of B's) outlives B when B is destroyed first. Destroying the root then destroys
  B's I/O objects after B's `io_context`. Probe `crossroot` (a root on A, `schedule_on(B)`, a
  timer on B; B destroyed, then the root): ASan heap-use-after-free in the timer's `~io_object_impl` (B's timer service, freed with B's `io_context`); the `rel` build dies with SIGSEGV (exit 139).
- A dying frame's RAII posts to other schedulers: a guard's `unlock` wakes the next waiter through
  the waiter's scheduler (`task.hh:866-868`, `:770`), an `event::set` through each waiter's
  (`:712`). Probe `holder` (the holder's root and the waiter's root on A, the waiter suspended on
  B; B destroyed, then A's roots): ASan heap-use-after-free in `asio::post` called from `asio_scheduler::post`; the `rel` build aborts with `asio::execution::bad_executor` (exit 134).

XN1 (the Xapiand lead's first finding) is right, refined. Their scenario (holder on A, waiter
rooted on B) has no use after free once D8.4 abandons B's waiter: destroying B first destroys the
waiter first, and destroying A first posts the grant to a live B, which then destroys a granted
waiter and leaves the turn locked (CN-R3). The use after free needs a frame that outlives its
scheduler, which the probes show is one `schedule_on` away. Their two-phase proposal is the fix, with the phases stated as the owner's
contract and a hook for each:

1. Stop and join every loop and pool any root may be suspended on (as D8.3).
2. Every scheduler closes: `post` and `post_demoted` drop the handle (its frame belongs to a
   root, destroyed next), and `spawn` onto a closed scheduler destroys the task at once instead
   of linking a new root while roots are being destroyed.
3. Destroy the roots of every scheduler, with every `io_context` still alive.
4. Destroy the schedulers, then the `io_context`s.

Phase 3's order inside one loop is sound: probe `composed` destroys the cancellation signals of a
pending token-bound `async_write` and `async_read`, then their sockets, then the `io_context`,
and ASan is clean. TSan does not see the two use-after-free shapes above (both run clean there),
so these tests belong in the ASan build.

T1 gains the probes' two shapes as tests (a root suspended on another loop; holder and waiter on
two loops, destroyed in both orders), an `event` set by a dying frame with a waiter on another
loop, and a destructor that spawns during phase 3, all with `live_frames()` back to baseline under
ASan.

### CN-R3. A waiter granted but not yet resumed leaves the mutex locked when its frame dies (blocking)

D8.4 settles a waiting `lock_waiter` as abandoned. It does not cover a waiter that `unlock`
already granted (`task.hh:858-868`) whose posted resume never runs: its frame is destroyed, the
awaiter only releases its reference (`:877-879`), and the lock is never handed on. Probe
`grant_destroyed`: one resume queued after the grant, the waiter never ran, and `try_lock()` fails afterwards: the mutex is locked forever, in all three builds. Under CN-R2's closing state every grant made during
teardown is this case, and in Xapiand the mutex is an index's turn, which outlives the http
service.

Fix: `lock_awaiter`, `timed_awaiter` and the token form (Q1) unlock in their destructor when the
waiter's state is `granted` and `await_resume` has not run. T1: grant, destroy the frame unrun,
`try_lock()` succeeds.

### CN-R4. Destroying a timer does not cancel an expiry already queued (condition)

D4 says destroying a deadline cancels its timer and "destroys the callback unrun", citing
`asio_scheduler::start_timer` (`task_asio.hh:81-90`, whose comment says the same). asio runs a
handler whose expiry is already queued with success even when the timer is gone. Probe
`late_fire` (two timers with one deadline; the first handler destroys the second):
the destroyed timer's handler runs with success (1 of 1, 0 aborted), in all three builds. `lock_until` survives this because `lock_timeout` holds a reference
(`task.hh:777-794`); a deadline whose callback reaches the deadline object would not, and one
that reaches only the shared state requests a token after its owner destroyed the deadline, which
breaks D4's "destroying it first never requests".

Fix: the timer callback holds a reference to the source state and checks a disarmed flag that
`~deadline` sets (both on the loop; Xapiand's `AsioHost::Armed` does the same,
`host_asio.h:68`, `:87`); correct the `start_timer` comment. T1 (D4) adds the probe's shape:
destroy the deadline after its expiry is queued, and the token stays unrequested.

### CN-R5. Roots and the inline resume do not survive a wrapper scheduler (condition)

D7 detects `roots` and `runs_here` on the scheduler type, and http's "What Xapiand changes"
item 6 says tasks that must die with the loop are spawned on the reactor's scheduler. Xapiand
spawns through its `Host` (`host.h:90`, `:241`, `:246`: `task::spawn(task::scheduler_ref::of(*this),
...)`) and waits for its turn with `lock_until(*host, ...)` (`pool.cc:350`), so its tasks are not
roots and decision item 4's premise ("Xapiand relies on frames being destroyed to release shard
uses and turn leases") fails. The turn waiter also records the `Host`'s reference as its
scheduler (`task.hh:914`), so a grant during teardown posts through the wrapper. Fix: D7 states
that a wrapper forwards both hooks (a `roots()` and a `runs_here()` member, detected as
`post_demoted` is today, `task.hh:132-136`), Xapiand's `Host` declares them (detection sees the
type `scheduler_ref::of(*this)` names, the base) and `AsioHost` forwards them to the reactor's
scheduler (its `post` drops while that scheduler is closing, CN-R2), and a T1 test spawns
through a forwarding wrapper and sees the root destroyed at teardown.

### CN-R6. `live_frames()` must count the frames over 2 KiB (condition)

D8.5 counts "pooled frames". The frame pool hands frames over 2 KiB to `operator new`
(`task.hh:255`), and http's connection frame is one (`char tmp[64 * 1024]`, `http_asio.h:765`),
the frame the zero-live-frames tests most need to see. Count in `pooled_frame::operator
new`/`delete` (`task.hh:300-303`), both paths, and say that cancel state blocks (D1) count too.

### CN-R7. Smaller corrections (conditions)

1. D5.3's emit step is "a pooled frame" posted to the scheduler. A frame destroyed at teardown
   under a pending emit (the stopped loop never runs it) leaks that frame, and it is not a root.
   Post a plain function object instead (the io_context destroys it unrun) or own the step from
   the awaiter; T1 (D8) adds "frame destroyed with a cross-thread emit pending".
2. D5 says nothing about a cancel that loses the race: the operation returns its real result
   (probe `race`), so a caller that must stop checks `token.requested()` after the await. State it;
   http D4's "a timed-out read ends with `operation_aborted`" holds only when the cancel wins.
3. D2 calls registration lazy, but D3 registers a linked child on its parent at construction:
   every per-read deadline linked to the session token takes the session state's lock twice per
   read. Say so in D2 and D3; T6 and T7 measure it only with TH2-R2's timeout kind.
4. Limit 3 (`sync_wait` frames are not roots) and CN-R2's closing state interact: a dropped post
   for an unrooted frame is a leak `live_frames()` will show. Name the unrooted frames (the
   `sync_wait` driver, the D5.3 step if kept, Xapiand's debug `Run::Resume`) in D8.

### Xapiand's notes on the task design (XN1-XN3)

- XN1 (D8.4, the holder side): agreed, refined in CN-R2; the waiter side needs CN-R3 as well.
- XN2 (reentrancy): agreed as a rule, and it reduces to CN-R1. asio never runs a handler inside
  its initiation: probe `channel` shows a waiting `channel` receive is not run inside
  `async_send` or `try_send` ("no" for both, in all three builds; the handlers run later from the loop), and `async_compose` completes through the
  immediate executor. The hand-written operations on the path are http's `ConnectionInput`
  (`arm`/`completed`, `try_send` on `done_`, `http_asio.h:549-561`) and `BodyStream`
  (`async_compose` over a channel, `http_body_stream.h:60-64`), both posted. With CN-R1's park
  handshake, a handler that does run inside its initiation continues in place and never resumes
  the coroutine inside its object's call. Add the rule to D6 ("a completion handler never resumes
  the awaiting frame inside its own initiation") and Xapiand's streamed test to http T4 (TH2-R5).
- XN3 (Q1's premise): agreed. `ShardEndpoint::take_turn` calls `turn.lock_until(*host, deadline)`
  (`pool.cc:350`, Xapiand:81c311799). Q1's text says "nothing calls them yet"; correct it and
  name the call site, which Xapiand migrates in the wave.

## Findings: http and reactor (`docs/design/task-handlers.md`, revision 2)

### TH2-R1. R2's destruction order covers one reactor (blocking)

R2 declares `scheduler_` last so it is destroyed before `aborts_`, the gate and `io_`; that is
right inside one `Reactor`. `TcpServer` owns its reactors in a `std::vector<std::unique_ptr<Reactor>>`
(`reactor.h:613`) and destroys them one at a time after `stop()` joins the threads (`:596-600`), so a
root on reactor 0 meets reactor 1 already destroyed: CN-R2's probes in reactor's shape. Today's
sessions are all finished by then (`wait_sessions`, `:595`), but roots that are not sessions are
on these schedulers after R2 (Detent's owner-loop tasks, Xapiand's tasks once `AsioHost` wraps the
reactor's scheduler, item 6), and a session's handler may `schedule_on` another reactor's scheduler
(D9 allows it).

Fix: R2 adds the phases of CN-R2 to `TcpServer::stop()` (after the join at `:598`: close every
reactor's scheduler, then destroy every reactor's roots) and to `~UdpServer`; reactor's T4 (i)
gains a two-reactor teardown test with a root on reactor 0 suspended on reactor 1. A process
with several servers or pools closes and destroys roots across all of them before destroying any,
so `TcpServer` exposes the phases as calls as well, and "What Xapiand changes" item 7 states the
order across Xapiand's services, loops and WorkPool. R2's sentence
that destroying roots "runs `SessionControl::finish`" holds through `~SessionControl`
(`reactor.h:290`) when the spawn's completion callback held the last reference, not because the
callback runs: a destroyed root never calls `done` (`task.hh:482-494`); say which.

### TH2-R2. T6 and T7 omit the timeout kind (condition)

D4 changes most per request where timeouts are set: a deadline per read linked to the session
token (two lock pairs on the session state, CN-R7.3), a timer through `start_timer`, and a token
callback on the monitor read, in place of today's `||` (`http_asio.h:505-555`). The bench already
has that kind: `httpt` sets all four bounds (`server_compare.cc:118-123`). T6 runs `http` and
`httpr`, T7 its own list; neither runs `httpt`, so the one mechanism this revision adds per request
is never measured. Add `httpt` to T6's kinds and to T7.

### TH2-R3. T3's port list misses `on_cancel` and the reactor tree (condition)

T3 ports the `req.cancelled()` assertions. Two probes also call `req.on_cancel`, which D5 removes:
`probe_throwcb.cc:8` (the throwing callback the row is about) and `probe_streamcancel.cc:8`. Name
their port (a `task::cancel_callback` on `req.cancel_token()`, held in the handler frame) and
restate `probe_throwcb`'s target for it: task drops the exception (task D2), the connection
survives, the gate returns to 0. `build.sh` also takes reactor from `RX` (default
`reactor-wt/l3-http`, `build.sh:4`); the procedure sets `RX` to step R's tree, and
`probe_acceptrst.cc:19-21` moves to the one-`Session` form.

### TH2-R4. T6's p99 needs its pattern (condition)

"On the p99 column" is not a procedure: wrk prints p99 with a unit that changes between rows
(`us` or `ms`, `bench/run.sh:35`, `:39`). Give the pattern, for example
`'^(?P<kind>\S+) .* C=(?P<conns>\d+) .* p99 +(?P<value>[\d.]+)(?P<unit>us|ms|s) '`; `ab_compare.py`
scales by the unit group (`ab_compare.py:59`, `:236-238`).

### TH2-R5. Xapiand's streamed-body test (condition)

XN2's test belongs where `push` and `read` live: H2's T4 gains (j) a handler that reads 10,000
pushed chunks back to back, each resumed inline (the C3 pin), under ASan and TSan.

### TH2-R6. The ledger contradicts its own criteria (condition)

`docs/status/task-handlers.md:34-37` says the criteria "were set for revision 1 (K4 (a)-(f), K6
req/s, K9 D8): the lead updates them", but K4, K6 and K9 above it already name T4 (a)-(i), CPU per
request and p99, and zero live frames. Remove the stale sentence. When CN-L1, CN-L2 and TH2-L1 are
folded in, K4 also names the new task T1 cases and T4 (j)-(k) (TH2-R1, TH2-R5).

### TH2-R7. Notes, non-blocking

1. Step R needs task C2 (tokens, `await_asio` with a token, roots), not C3; R4 pins C3. R can start
   now on its branch against C2's interface and move its pin to C3 at landing; gates order
   landing, not starting.
2. R3's "abort() runs on the loop on every path" has one exception: `abort_all` runs on the caller
   when no loop is claimed (`reactor.h:249`), as in tests and after `UdpServer`'s loop ends. D5.3's
   off-loop path covers it; state it.
3. consensus's public transport (`include/consensus/transport/peers.h`, `session.h`) is
   `asio::awaitable`; it is outside W2a and inside the "Coroutine drivers" decision's M1b. T8's
   consensus row builds at the reactor pin only. The lead names its owner in the system view.

## The revision-1 findings

| ID | Status | Where |
| --- | --- | --- |
| TH-L1 / TH-R1 | closed | R1, D1, D11; T1 |
| TH-L2 / TH-R2 | closed in the design; its task mechanism is CN-L1 | task D1-D5; http D3 (twelve sites), D4, D7; R3; T4 (i), T3 |
| TH-L3 / TH-R3 | partially closed: handler as child (D2), `when_all` (D6), T4 (d); open on the cross-reactor teardown | TH2-L1, CN-L2 |
| TH-L4 / TH-R8 | closed; the threshold is the owner's (Q1) | T6 |
| TH-R4 / C1 | closed in the design; the scope is right, the entry order is CN-L1 | task D6, T1 (D6) |
| TH-R5 | closed | R2 |
| TH-R6 | closed | D5 |
| TH-R7 / C6 | closed | D8, T4 (g) |
| TH-R9 / C2 | closed; `httpt` added by TH2-R2 | T7 |
| TH-R10 / C3 | partially closed: numbers stated; the `on_cancel` port and `RX` missing | T3, TH2-R3 |
| TH-R11 / C4 | closed | T1, task D10 |
| TH-R12 / C5 | closed | D9, T4 (h) |
| TH-R13 / C7 | closed | Steps, T8 |

## Rulings

- **task Q1 (`lock_until`/`lock_for` beside deadline tokens): (a).** `lock(cancel_token)` with
  `deadline` replaces both; one timeout mechanism under the one-shape rule. The premise is
  corrected (XN3): Xapiand's `take_turn` is the one caller (`pool.cc:350`) and migrates in the
  wave, with its turn wait linked to the request's token. C1 removes them only with Xapiand's
  migration in the same wave.
- **task Q2 (token-bound `await_asio` only on its own loop): (a).** Every planned use is
  loop-local (reactor, http; Xapiand's `signal_loop` and `read_chunk`, per XN). Keep the Debug
  assertion.
- **task Q3 (roots on `inline_scheduler`): (a).** Only `asio_scheduler` owns roots. strata's
  `inline_executor` drains in its destructor (`executor.hh:63`), and tests declare what their tasks
  touch after the scheduler.
- **http Q2 (Detent's own `asio::awaitable` code): inside Detent's W2, as the lead holds, not a
  separate workstream and not W2a.** The one-shape rule covers it; a separate workstream leaves
  Detent on two coroutine types for its duration, which is the mixed shape the owner ruled out;
  and W2 already rewrites `detent_authority/service.cc`, which has its own `asio::awaitable`
  runtime (`service.cc:1137`, `:1161`), so splitting touches that file twice. The mechanisms are
  W2a's, nothing new. Condition: W2's design lists the 11 files as its own landable steps after the
  handlers, and W2's done criteria carry T1's grep over Detent `src` and `include` (benchmarks
  named, `benchmarks/fleet_load.cc`).
- **http Q1 ("considerable")**: not ruled; the owner's.

## Probes (kept in `.scratch/arch-review-rev2/`)

| Probe | Finding | Result (rel, ASan+UBSan, TSan) |
| --- | --- | --- |
| `late_fire` | CN-R4 | a timer destroyed after its expiry was queued still runs its handler with success: 1 of 1, all builds |
| `grant_destroyed` | CN-R3 | waiter granted, resume queued, frame destroyed: the waiter ran 0 times and the mutex stays locked, all builds |
| `crossroot` | CN-R2, TH2-R1 | root on A suspended on B, B destroyed first: ASan heap-use-after-free in the timer's destructor; `rel` SIGSEGV; TSan silent |
| `holder` | CN-R2, XN1 | holder's root on A, waiter suspended on B, B destroyed first: ASan heap-use-after-free in `asio_scheduler::post`; `rel` aborts with `bad_executor`; TSan silent |
| `dispatch_inline` | CN-R1 | D5.2's order with D6's inline resume and a dispatch completion: ASan use-after-poison in the coroutine's resume; `rel`, TSan silent |
| `channel` | XN2 | a waiting channel receive does not run inside `async_send` or `try_send`, all builds |
| `race` | CN-R7, "What holds" | emit after the read completed, before its handler: success, 4 bytes; a second emit after the handler: no-op, all builds |
| `composed` | CN-R2 (the order inside one loop holds) | token-bound `async_write` and `async_read` pending; signals destroyed, then sockets, then the `io_context`: ASan clean, all builds |

Rebuild: `.scratch/arch-review-rev2/build.sh rel|asan|tsan` under `quiet light`, then
`bin/probe_rev2-KIND MODE`. The revision-1 probes stay in `.scratch/arch-review/` for T3.

## Blocking items / Required changes

- CN-L1 (CN-R1): `await_asio` registers before initiating, completes a pre-requested token
  without initiating, and uses a park/wake handshake so a handler that runs inside initiation
  never resumes the frame; D6's rule and the T1 cases as listed.
- CN-L2 (CN-R2, CN-R3, XN1): D8 states the four-phase teardown (stop all; close all; destroy all
  roots; destroy schedulers, then `io_context`s), `spawn` onto a closed scheduler destroys the
  task, a granted-but-unresumed waiter unlocks in its destructor, and T1 covers both orders, the
  cross-loop root and the dying `event` setter.
- TH2-L1 (TH2-R1): reactor's R2 applies CN-L2 across reactors in `TcpServer::stop()` and
  `~UdpServer`, with a two-reactor teardown test.

## Conditions

- C8 (CN-R4): the deadline's callback holds the state and checks a disarmed flag; T1 destroys a
  deadline after its expiry is queued.
- C9 (CN-R5): wrappers forward `roots` and `runs_here`; Xapiand's `Host` does; a T1 test.
- C10 (CN-R6): `live_frames()` counts both allocation paths.
- C11 (CN-R7): the emit step is a plain handler or owned; the lost race and D3's eager
  registration are stated; unrooted frames are named.
- C12 (TH2-R2): `httpt` in T6 and T7.
- C13 (TH2-R3): T3 names the `on_cancel` ports, `probe_throwcb`'s new target, `RX`, and the
  `Session` port of `probe_acceptrst`.
- C14 (TH2-R4): T6's p99 pattern with its unit group.
- C15 (TH2-R5): T4 (j), the streamed back-to-back read test.
- C16 (TH2-R6): the stale ledger sentence removed; K4 extended.
- C17 (http Q2): W2's design lists Detent's 11 `asio::awaitable` files as steps, with the grep in
  its criteria.

The task revision that folds in CN-L1 and CN-L2 adds no new mechanism beyond them; it gets a
scoped re-check of D5, D6 and D8 (rechecker, with these probes), and the http revision the lead's
confirmation of TH2-L1 and C12-C16.

Not run: no T-row (nothing is built); no benchmark; the revision-1 probes and the L3 probes; g++;
the M4; fuzzing.
