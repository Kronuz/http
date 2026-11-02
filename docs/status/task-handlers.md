> Status: CURRENT (2026-11-02) the ledger for W2a, http handlers on task (Detent `docs/PLAN.md` W2a).

# task-handlers (http, http-log, prism)

- Design: `docs/design/task-handlers.md` (DRAFT revision 1); reviews: none yet.
- Branch / worktree: `design/task-handlers` at `~/code/Kronuz/http-wt/task-handlers`, from http
  `origin/main` 679c0f8; head: the commit that adds this ledger.
- State: designing (ready for preliminary review)
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
- Done: design draft and this ledger (see Log).
- Doing: nothing; stopped at ready for preliminary review.
- Next:
  1. The architect's preliminary review of the design; the Xapiand lead's review (decision item 2).
  2. Owner answers Q1 (scheduler ownership), Q2 (`cancelled()` beside `cancel_token()`), Q3 (if the
     bridged loop misses the gate).
  3. Steps S1 to S10 of the design, S1 first (it is T6's and T7's baseline).
- Blocked on: Q1 blocks S3's scheduler accessor shape and Xapiand's note (owner with the Xapiand
  lead); Q2 blocks S4's `Request` surface (owner). S1 and S2 are not blocked.
- Findings folded in: none yet.
- Last verified: nothing built or run (design only); every file:line in the design read at the
  commits its header names.

## Log
- 2026-11-02: worktree and branch created from http `origin/main` 679c0f8; design draft revision 1
  and this ledger written.
