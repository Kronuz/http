> Status: CURRENT (2026-11-02) the ledger for W2a, http handlers on task (Detent `docs/PLAN.md` W2a).

# task-handlers (http, http-log, prism)

- Design: `docs/design/task-handlers.md` (DRAFT revision 2), with task
  `docs/design/cancellation.md` (branch `design/cancellation`, task:6d361ca); reviews: the
  architect's preliminary review of revision 1, REJECT (`docs/reviews/task-handlers-design.md`,
  2026-11-02); the Xapiand lead's notes (`docs/reviews/task-handlers-xapiand.md`).
- Branch / worktree: `design/task-handlers` at `~/code/Kronuz/http-wt/task-handlers`, from http
  `origin/main` 679c0f8; head: the commit that adds revision 2.
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
- Done: design revision 1 (http:b110118); revision 2 of the design (this branch) and task's
  half (`design/cancellation`, task:6d361ca). See Log.
- Doing: nothing; stopped at ready for preliminary review of revision 2.
- Next:
  1. The architect's re-check of revision 2 (the cancellation carrier, TH-L2, gets the scoped
     re-check; the rest the lead's confirmation) and of task `docs/design/cancellation.md`; the
     Xapiand lead's review of both.
  2. The owner: Q1 (the "considerable" threshold, put by the Xapiand lead), Q2 (Detent's own
     `asio::awaitable` code); task's Q1-Q3.
  3. Steps in the design's order: S1/S2 (in progress), task C1-C3, reactor R, http H1-H2, the
     lead's T6/T7, http-log, prism, Detent, cluster and consensus, Xapiand.
- Blocked on: nothing blocks task C1 or S1/S2. The verdict after T6 waits for Q1 (owner); task's
  `async_mutex` change waits for task Q1 (owner with the Xapiand lead). The done criteria K1-K9
  were set for revision 1 (K4 (a)-(f), K6 req/s, K9 D8): the lead updates them for revision 2
  (T4 (a)-(i), T6 on CPU per request and p99, D11).
- Findings folded in: TH-L1 to TH-L4, TH-R1 to TH-R13, C1 to C7 and the Xapiand lead's notes, in
  revision 2's "Review response" table.
- Last verified: nothing built or run (design only); every file:line in both designs read at the
  commits their headers name.

## Log
- 2026-11-02: worktree and branch created from http `origin/main` 679c0f8; design draft revision 1
  and this ledger written.
- 2026-11-02: the architect rejected revision 1 (http:88f1275, with the Xapiand lead's notes).
  Revision 2 written: reactor's `Session` on task first, the all-task loop with a token carrier,
  the handler as a child task, T6 on CPU per request and p99; task's half in
  `design/cancellation`.
