> Status: CURRENT (2026-11-03) the ledger for step H2, http's streamed path on task and the proof and docs of H1 and H2 (http `docs/design/task-handlers.md` revision 2, "Steps" 5).

# task-h2 (http)

- Design: `docs/design/task-handlers.md` revision 2 (branch `design/task-handlers`, http:694fb47): D1's `BodyStream` (`read`, `push`), D6, D9 for `read`, gate rows T1, T2, T3 (all 13 probes), T5, T7's streamed kind, step 5 and the docs list; H1's ledger `docs/status/task-h1.md` and review `docs/reviews/task-h1-code.md`.
- Branch / worktree: `phase/task-h2` at `~/code/Kronuz/http-wt/task-h2`, from `origin/main` http:2bede1b (H1 landed; pins task:6558c2e, reactor:2fd30c8, unchanged).
- State: built, proved and documented; CI green on every job; ready for code review.

## Done criteria (from the design's H2 row and K1-K10 as they apply; tick only with evidence)

- [x] D1 `BodyStream` on task: `BodyStream(task::asio_scheduler&, capacity)` (`http_body_stream.h:50`), `task::task<asio::error_code> read(std::string& chunk, task::cancel_token = {})` (`:64`: a chunk, sticky `eof` only for a complete body, `operation_aborted` for its own token with nothing lost, else the body's error), `task::task<bool> push(std::string, task::cancel_token)` (`:81`); `async_read` and its `ReadOp` gone. Tests: `coroutine_test` [B] (token already requested, a pending read cancelled by its token loses no chunk, a `task::deadline` bounds a read, buffered chunks before the error), [H] streamed cases. Mutants D1-eof and D1-token killed.
- [x] D6 the streamed join: the feed and the handler are children of the connection task under `task::when_all` (`http_asio.h:956`); the feed's source is linked to the session token (`:885`), the handler's end requests it (`:952`); a failed body cancels the request before failing the stream (`:890`); the disconnect watch starts at the body's end, lazily as for a buffered body unless the handler already asked (`:913`, `RequestAccess::asked` `:675`). Tests: `coroutine_test` [S] (hang-up after a complete body cancels the handler, asked before and after the body; a handler that answers without reading, or after one chunk, stops the feed and the connection closes), [T] (d) body read at shutdown, `handler_count_test` streamed pair. Mutants D6-cancel, D6-truncated, D6-stop, D6-link, D6-watch, D6-lazy-always, D6-lazy-never killed (table).
- [x] D9 for `read`: `BodyStream::affine()` (`http_body_stream.h:111`) asserts in Debug that the read runs on the stream's scheduler or, outside any task, on its loop thread. Test: T4 (h) death test `/affine-read` (a body that moved away reads) and `/affine-read-child` (a child that moved away and came home reads). Mutant D9-read killed.
- [x] K1 / T1 one shape: `git grep -nE 'asio::awaitable|use_awaitable|awaitable_operators|co_spawn' -- '*.h' '*.cc' ':!bench'` is empty; no `async_read(`, `cancelled()`, `cancel_reason()`, `on_cancel(` or `CancelReason` outside the CHANGELOG's upgrade note.
- [x] K2 / T2 suites: every ctest passes, none removed (13 tests, names as at http:2bede1b): macOS Debug, macOS ASan+UBSan and TSan, Linux g++ ASan+UBSan and clang TSan (Log).
- [x] K3 / T3 all 13 L3 probes, ported to `task::task` handlers (`.scratch/review-l3-task/`), ASan+UBSan and TSan, Linux and macOS: table below. One gate value is wrong in the design (Design problems).
- [x] K4 T4's streamed cases: (d) a body read at shutdown ends `operation_aborted`, the request cancelled, answered, `stop()` 0 ms, 0 frames; (h) `/affine-read`; each killed its mutant.
- [x] K5 / T5 sanitizers, separate builds, 0 reports: suite plus T3 plus T4 under ASan+UBSan and TSan on macOS (Homebrew LLVM) and Linux (lease: g++ 13 ASan+UBSan, clang 18 TSan).
- [x] K7 / T7 streamed POST kind (`httpb`, `bench/server_compare.cc`, `bench/post_body.lua`): allocations and io handlers per request at most A (S1): table below.
- [x] K9 docs: README, ARCHITECTURE, AGENTS, `docs/TRANSPORT.md`, `docs/mechanisms/coroutine-handlers.md` and the CHANGELOG upgrade note (old and new signatures; the removed `cancelled()`, `cancel_reason()`, `on_cancel()`, `CancelReason`, `async_read`; the merged factories and constructors; the four teardown phases).
- [x] K10 CI green on every job: run 41603112360 at http:45ddd7a (linux gcc, clang, arm64, asan-ubsan, tsan; macOS arm64, x86_64; FreeBSD; the headers matrix skipped as on main)
- [x] The `timeouts` flake (run 41589992099): explained with numbers (below), test fixed.

## Mutants (`.scratch/mutants/`; Linux lease, Debug, each built and run, the source restored after)

| Mutant | Change | Result |
|---|---|---|
| D1-eof | `read` returns `eof` for a failed body | killed: [B] failed body, [H] x4, `timeouts` streamed timed_out |
| D1-token | `read` ignores its token | killed: `coroutine_test` hangs (rc 124) |
| D9-read | `read` without its affinity assert | killed: T4 (h) `/affine-read` (survived round 1, when `/affine-read` wrote while away; the handler now comes home before writing) |
| D6-cancel | a failed body does not cancel the request | killed: `coroutine_test` |
| D6-truncated | a short body reads `eof` | killed: [H] x3 |
| D6-stop | the handler's end does not stop the feed | killed: [S] "after the body's first chunk" (survived rounds 1 and 2: the handler answered before the feed's read, so the feed's push already saw the abandoned stream) |
| D6-link | the feed's source not linked to the session token | killed: T4 (d) body read at shutdown, "the feed takes the session's token" |
| D6-watch | no watch after the body | killed: [S] hang-up after a complete body |
| D6-lazy-always | the watch after the body always lazy | killed: [S] hang-up after a complete body, handler that asked during the body (`5000:done`) |
| D6-lazy-never | the watch after the body always armed | killed: `handler_count_test` (f) streamed (9.000 handlers per request for both, against 7.000 and 9.000) |
| none | no change | every test passes |

## T3 (`.scratch/review-l3-task/`; Linux lease g++ 13 ASan+UBSan and clang 18 TSan, macOS Homebrew LLVM ASan+UBSan and TSan)

| Probe | Gate | Result |
|---|---|---|
| truncated | 0 of 4 clean eof | 0 of 4, every platform |
| throwcb | streamed and buffered survive | both survive |
| pipeline | 120 rounds in order, gates 0 (TSan 40) | 120 x 3 variants in order, gates 0; TSan 40 |
| timeouts | idle <= 410 ms for 400, header <= 310 ms for 300 | Linux ASan 400 / 300 ms; macOS ASan 402 / 302 ms |
| admission | gate 0 on every path, next 20 admitted | every path 0, admitted; `stop()` 53 ms |
| shutdown | 300 idle within 50 ms; stalled uploads 0 ms; stalled reply at the grace; 25 storm rounds, no frame outlives `stop()` | 300 idle 4 ms (TSan 7); stalled streamed 1 ms (macOS 0); never-reading client 406 ms for 400 (TSan 478, macOS 411); 25 rounds clean, 60/60 cancelled |
| streamcancel | `read_err` 0 (see Design problems) | mid-body close: `read_err=1`, request cancelled, the probe's own check passes |
| deferred_stop | four modes clean | four modes clean |
| cap | boundaries exact | every boundary but "CL = 2^64-1 -> 413", which fails at http:2bede1b too (the parser's, as L3's review recorded) |
| cl | boundaries exact | as L3's review |
| abortlist | 20,000 walks clean | clean |
| acceptrst | listener survives | survives |
| poststorm | listener survives | 0 of 100 failures |
| probe_h1 (H1 review) | 12 scenarios | pass, 0 frames; `ignore` 401 ms, `hop-b` 602 ms |

0 sanitizer reports in any output (Linux TSan: 28 output files fetched to `linux-out/`; macOS TSan `mac-tsan.log`).

## Counts (Release, `-DHTTP_BENCH_COUNT_ALLOCS=ON`, macOS Apple clang, R=2, 3 s per row; `.scratch/counts/`; counts only, no timing)

A is S1 (http:03467b1, `git archive`) with the same `httpb` kind written for its API; B is this branch. allocs/req and handlers/req, C = 1 / 8 / 64 keep-alive, then `close` at C=8.

| Kind | A allocs | B allocs | A handlers | B handlers |
|---|---|---|---|---|
| http | 3.00 / 3.00 / 3.00 | 3.00 / 3.00 / 3.00 | 1.00 | 1.00 |
| httpr | 7.00 / 7.26 / 8.30 | 5.00 / 5.03 / 5.51 | 2.00 | 2.00 |
| httpt (`cancel_token()` plus budget) | 22.00 / 22.29 / 22.92 | 4.00 / 4.00 / 4.00 | 2.00 | 2.00 |
| httpb (1 KiB streamed POST) | 47.00 / 47.42 / 48.81 | 19.00 / 19.10 / 19.77 | 8.00 | 7.00 |
| http close | 3.00 | 3.00 | 1.00 | 1.00 |
| httpb close | 47.36 | 19.08 | 8.00 | 7.00 |

The `http`, `httpr` and `httpt` rows are from the run before the streamed watch change, which does not touch the buffered path; the `httpb` and `close` rows are after it. B <= A on every row. The first `httpb` run had B at 9.00 handlers against A's 8.00 (and 22 allocs): Asio's handler tracking (`.scratch/counts/trk.sh`, 40 requests on one connection) showed the two extra handlers were the disconnect watch armed at the body's end (its read, then `ConnectionInput::drain`'s channel receive and its resume), paid by a handler that never asks for its token. The watch is now lazy there as on the buffered path (D6 above), which `handler_count_test` pins: streamed 7.000, streamed and asking 9.000 handlers per request (macOS Debug). Counts were taken with Apple clang only; g++ counts are left to step M.

## The `timeouts` flake (run 41589992099, macos-arm64, "a slow but moving body completes")

The case sends a byte every 100 ms against a 300 ms inactivity timeout, so it fails only when the client's own gap between two sends exceeds 300 ms. A sweep of the gap (`.scratch/flake/slow_body_sweep.cc`, macOS, 5 runs each) shows the server exact at its timeout: max gap <= 289.9 ms completed 5/5, 300.0-300.1 ms 4/5, >= 309 ms 0/5. So the CI failure is the runner stalling the client's `sleep` by more than 200 ms, the test's timing, not the server. Fix (http:0fc20b7): the test measures each gap (after-send minus before-previous-send) and voids an attempt whose gap reached 0.3 s, up to 3 attempts. Loops under load (3 test loops beside a `readahead_test` loop, `quiet light`): 120 + 120 + 300 runs, 0 slow-body failures, the largest client gap 0.160 s. Loop 1 showed a different case, "a reply the client reads slowly", failing 2/120: the 1200 ms pause began before the server had built its 48 MiB reply (built 56-161 ms after the GET under load), so the pause did not stall the server's write; the test now waits for the reply's first byte (`MSG_PEEK`) before pausing, and loops 2 and 3 passed 420/420. One `readahead_test` run beside loop 3 aborted (1 of 100, output not kept); 200 more runs under `quiet light` with output kept (`.scratch/flake/ra-loop.sh`) passed 200/200, so its cause is not found.

## Design problems

1. T3's `probe_streamcancel` row expects `read_err` 0. `read_err` 0 was L3-B1's symptom (a body cut mid-way read as complete); the design's own D1 requires a failed read (`connection_reset`) there, so the correct result is `read_err=1`, which the probe's own check accepts. The row should say 1.

## Done
- `BodyStream::read` on task with D9 (http:30ac15c); the flake's test fix (http:0fc20b7); the `httpb` bench kind (http:1ad87d8); the lazy streamed watch, the [S] and handler-count tests, the docs and this ledger (Log).

## Doing
- Nothing running.

## Next
1. Code review of `phase/task-h2`.
2. Step M: the lead measures T6 and T7 (the `httpb` kind is ready).

## Blocked on
- Nothing.

## Log
- 2026-11-03: worktree and branch from http:2bede1b; `BodyStream::read` on task with D9 (http:30ac15c).
- 2026-11-03: the flake's test fix (http:0fc20b7) and the `httpb` kind (http:1ad87d8). T3 on the old watch: Linux g++ ASan+UBSan and clang TSan, macOS ASan+UBSan and TSan, suite 13/13 each, every probe as the table, 0 reports. Mutants round 1 left D9-read, D6-stop, D6-link, D6-watch alive; tests added; round 2 killed all but D6-stop. Counts showed `httpb` B at 9 handlers against A's 8; handler tracking found the eager watch.
- 2026-11-03: the lazy streamed watch, [S] cases and the streamed `handler_count` pair (http:d7e928f); docs (http:65f3359). Rounds 3 and 4 killed D6-stop, D6-watch, D6-lazy-always, D6-lazy-never; `none` passes. After the change: Linux g++ Debug 13/13; Linux ASan+UBSan (g++ 13) and TSan (clang 18) suite 13/13 and every T3 probe as the table (stalled reply 411 / 476 ms, 300 idle 4 / 7 ms), 0 reports; macOS Debug 13/13; `httpb` counts 7 handlers, 19 allocs. Round 4's `none` run of `handler_count` timed out once: a stale binary (round 3's last rebuild postdated the rsync'd fix's mtime, so the fixed test was not recompiled until a mutant touched `http_asio.h`); 190 later runs on the lease, 160 of them 4 at once, never hung.
- 2026-11-03: pushed `phase/task-h2`; CI run 41603112360 at http:45ddd7a green on every job. Build dirs deleted (local and lease).
