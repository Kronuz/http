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
  - [ ] K1 One shape (T1): no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in http, http-log, prism or reactor sources and headers (benches' asio comparators excepted); `as_awaitable` gone from task; one cancellation query (`cancel_token()`), `cancelled()`/`cancel_reason()`/`on_cancel()` removed
  - [ ] K2 every ctest at head passes in task, reactor and http, none removed (T2)
  - [ ] K3 the L3 probes rerun on the new API with the results T3 names, ASan and TSan
  - [ ] K4 the new tests T4 (a)-(i) and task's design tests pass, each failing with its fix reverted
  - [ ] K5 ASan+UBSan and TSan separate builds clean in task, reactor and http; g++ Debug ASan suite clean (T5)
  - [ ] K6 server CPU per request and p99 of B1 against A with the A/A control, on the Linux x86-64 gate host plus one macOS check, judged by the owner's "considerable" threshold (T6, the lead)
  - [ ] K7 allocations and io handlers per request: B1 at most A, pinned as a ctest (T7)
  - [ ] K8 consumers migrated and pinned in the same wave: http-log, prism, cluster, consensus, Detent build and pass; Xapiand and strata/prospect/residency per the Xapiand lead (T8)
  - [ ] K9 zero live task frames after `stop()` in every library's suite; docs, how-it-works notes and CHANGELOG upgrade notes current in task, reactor and http
  - [ ] K10 CI green on every job in task, reactor, http, http-log, prism (and the consumers) before each lands
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
  `async_mutex` change waits for task Q1 (owner with the Xapiand lead). The done criteria (revised for revision 2 by the lead, 2026-11-02)
  were set for revision 1 (K4 (a)-(f), K6 req/s, K9 D8): the lead updates them for revision 2
  (T4 (a)-(i), T6 on CPU per request and p99, D11).
- Findings folded in: TH-L1 to TH-L4, TH-R1 to TH-R13, C1 to C7 and the Xapiand lead's notes, in
  revision 2's "Review response" table.
- Last verified: nothing built or run (design only); every file:line in both designs read at the
  commits their headers name.

- Lead ruling (2026-11-02, from Detent W2 D15): `HttpAsioService` exposes task's four teardown phases as separate calls (`stop()`, `close()`, `destroy_roots()`, then destruction, the destructor running any phase not yet run), forwarding reactor's `TcpServer` calls of step R, so an application runs each phase across http's reactors and its own schedulers together. H1 carries it with a test and a mutant.

## Log
- 2026-11-02: worktree and branch created from http `origin/main` 679c0f8; design draft revision 1
  and this ledger written.
- 2026-11-02: the architect rejected revision 1 (http:88f1275, with the Xapiand lead's notes).
  Revision 2 written: reactor's `Session` on task first, the all-task loop with a token carrier,
  the handler as a child task, T6 on CPU per request and p99; task's half in
  `design/cancellation`.
