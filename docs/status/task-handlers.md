> Status: CURRENT (2026-11-02) the ledger for W2a, http handlers on task (Detent `docs/PLAN.md` W2a).

# task-handlers (http, http-log, prism)

- Design: `docs/design/task-handlers.md` (DRAFT revision 1); reviews: none yet.
- Branch / worktree: design `design/task-handlers` at `~/code/Kronuz/http-wt/task-handlers`, from http
  `origin/main` 679c0f8. Build steps S1 and S2: `phase/task-handlers-s1` at
  `~/code/Kronuz/http-wt/task-handlers-s1`, from `design/task-handlers`.
- State: S1 and S2 built, in review (S3 onward waits for design revision 2)
- Done criteria (set by the lead; contributors tick, never edit):
  - [ ] K1 `HttpHandler::handle`, `Router::Route`/`Router::handle` and `BodyStream::read` are
    `task::task`; no `asio::awaitable`, alias or shim in the handler API (design T1)
  - [ ] K2 every ctest at head passes under the new API, none removed (T2)
  - [ ] K3 the L3 probes rerun on the new API with the review's results, ASan and TSan (T3)
  - [ ] K4 the new tests T4 (a)-(f) pass, each failing with its fix reverted
  - [ ] K5 ASan+UBSan and TSan separate builds clean; g++ Debug ASan suite clean (T5)
  - [ ] K6 req/s and p99 at 8 and 64 connections within the A/A band of head (T6, the lead)
  - [ ] K7 coroutine frame allocations per request no worse than head, pinned as a ctest (T7)
  - [ ] K8 http-log and prism migrated and pinned; Detent and Xapiand build against it (T8)
  - [ ] K9 D8 ruled from K6 and K7; docs and CHANGELOG upgrade note updated
- Done: design draft and this ledger (see Log); S1 and S2 (below).
- Doing: nothing; stopped at S1 and S2 in review. S3 onward waits for design revision 2.
- Next:
  1. The lead's review and CI of `phase/task-handlers-s1`; the gate runs of A and A' (below).
  2. Owner answers Q1 (scheduler ownership), Q2 (`cancelled()` beside `cancel_token()`), Q3 (if the
     bridged loop misses the gate).
  3. S3 onward after revision 2 (B0 and B1 drop into `bench/ab.sh` through `EXTRA`).
- Blocked on: Q1 blocks S3's scheduler accessor shape and Xapiand's note (owner with the Xapiand
  lead); Q2 blocks S4's `Request` surface (owner). S1 and S2 are not blocked. One open item for
  S3/S7 (not a block): see "Frame counts" below.
- Findings folded in: none yet.
- Last verified: nothing built or run (design only); every file:line in the design read at the
  commits its header names.

## Steps S1 and S2 (`phase/task-handlers-s1`)

Step criteria (set by the lead, TH-L4/TH-R8; the criteria above are the workstream's):
- [x] S-K1 the bench reports CPU/request, p99, allocations/request and handlers/request for A, the counting
  code outside the library
- [x] S-K2 the A/A script runs end to end (smoke)
- [x] S-K3 task pinned at head; configure and the full http suite pass, ASan+UBSan build too
- [x] S-K4 ledger current; commits small

Commits: `http:6b0f824` (S2, the pin), `http:d6f8468` (S1, counters, `httpr`, `bench/ab.sh`).

Evidence (macOS x86_64, Apple clang, Release unless noted; host shared, so no number below is a measurement):
- S-K1: `bench/run.sh` already printed `server CPU us/req` and `p99`; it now also prints `allocs/req` and
  `handlers/req` when the server is the counting build, and a `RESULT key=value` line (p50, p99, CPU in
  microseconds). The counters are `bench/counting.h` (global `operator new`, relaxed atomic; Asio handler
  invocations through `ASIO_CUSTOM_HANDLER_TRACKING`, `bench/counting_tracking.h`), compiled only with
  `-DHTTP_BENCH_COUNT_ALLOCS=ON` into `http_server_compare`. `git diff 679c0f8 -- '*.h'` outside `bench/` is empty.
  Counting build, 2 reactors, 2 s smoke runs: `http` 3.00 allocs and 1.00 handlers per request at 1, 8 and 64
  connections; `httpr` 7.00, 7.14, 8.32 allocs and 2.00 handlers. New kind `httpr` is a `Router` route that
  yields once with `asio::post`. `test/bench_counting_test.cc` (ctest `bench_counting`, only in the counting
  build) checks the handler count equals `io_context::run()`'s count on posts, awaitables and a socket round
  trip, and the allocation count on known `operator new` forms and frames. Mutants: handler counter made a
  no-op, 4 checks fail; aligned `operator new` not counting, 2 checks fail; both restored, test passes.
- S-K2: `sh bench/ab.sh build|verify|counts|run|report` under `quiet strict detent-http-s1 10` (10 minute
  hold), `ROUNDS=3 SECS=2`, kinds `http httpr`, 8 and 64 connections, series a, aa and a third copy bx:
  verify passes for every series (103 identical bytes, both kinds), 36 timed runs, report prints req/s, p99 and
  CPU per request with the A/A control and the counts table. The smoke ratios (all within 0.92-1.03, verdict
  noise) are a harness check only; the lead runs the gate. A' is a byte copy of A's binary.
- S-K3: configure fetches task at `8cad661e0dd3bd1c74b964cdc0ee42066f568838` (equal to `git ls-remote ... HEAD`
  today), one `asio-src` in `_deps` (no second Asio); no source includes task. Release `ctest` 12/12; Homebrew
  LLVM 23 ASan+UBSan Debug (`-fsanitize=address,undefined -fno-sanitize-recover=undefined`, the L3 ledger's
  `-stdlib++-isystem` and zlib workaround) `ctest` 12/12, 0 reports. CI: see Log.

Run the gate (the lead): `OUT=.scratch/ab sh bench/ab.sh build` under `quiet light`, then `sh bench/ab.sh run`
under `quiet exclusive` (defaults: kinds `http httpr`, 8 and 64 connections, 7 rounds of 4 s, 2 reactors, series
rotated by round), `sh bench/ab.sh counts`, `sh bench/ab.sh report`. B0/B1: `EXTRA="b0=/path b1=/path"`.

Frame counts: the counting build turns Asio's small-block recycling off so awaitable frames reach `operator
new` (otherwise they come from Asio's per-thread cache and count 0). `task` has its own per-thread frame pool
(`task.hh:235-306`, `frame_pool`, no off switch), so a task frame also reaches `operator new` only on a pool
miss. For T7 (B <= A allocations per request) B0/B1 will undercount frames unless task gains a switch that
disables `frame_pool` in the counting build, or the count adds the pool's own misses. S3/S7 need that
decision from the task owner; S1 cannot make it. A's counts include the handler operations Asio allocates
(read and post), not only frames.

## Log
- 2026-11-02: worktree and branch created from http `origin/main` 679c0f8; design draft revision 1
  and this ledger written.
- 2026-11-02: S2 (task pin) and S1 (counting build, `httpr`, `bench/ab.sh`) built on `phase/task-handlers-s1`
  from `design/task-handlers`; Release and ASan+UBSan suites 12/12; smoke of the A/A script under a 10 minute
  strict hold.
