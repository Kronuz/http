> Status: CURRENT (2026-10-30) the L3 ledger for Phase 7c (Xapiand `docs/design/phase7c.md` D2, D3, D12, D16, section 5).

# phase7c-l3 (http, reactor, http-log, prism)

- Design: Xapiand `docs/design/phase7c.md` D2, D3, D12, D16 and section 5's L3; reviews: Xapiand
  `docs/reviews/phase7c-design.md` R3, R6, C14, C17; owner ruling Q3 (strata
  `docs/status/xapiand-on-strata.md`): the offload path is removed from http entirely.
- Branches / worktrees (all `phase7c/l3-http` unless noted):
  - reactor: `~/code/Kronuz/reactor-wt/l3-http`
  - http: `~/code/Kronuz/http-wt/l3-http`
  - http-log: `~/code/Kronuz/http-log-wt/l3-http`
  - prism: `phase7c/l3-prism` at `~/code/Kronuz/prism-wt/l3-prism`
- State: implementing (library work done; proof under way)
- Done criteria: the lead's list was the task message; they are the checkboxes below.
  - [ ] Offload removed: worker pool, `should_offload`, body hand-off, `ChannelBodyReader`, `Reactor::pool`
  - [ ] `HttpHandler::handle` is a coroutine; `Router` routes are coroutines
  - [ ] `BodyStream` read with `co_await` on the loop
  - [ ] Per-request disconnect callback (`Request::on_cancel`), both C17 rules stated and tested
  - [ ] `reactor::AdmissionGate`, beside the (application's) demoted cap, each with its own 503
  - [ ] Buffered-body cap with 413
  - [ ] Shutdown order (D16) and its bound (C14); `Reactor::track` O(1)
  - [ ] prism (and http-log, which it wraps) migrated and serving
  - [ ] Tests for each piece; a mutant per claim; ASan+UBSan and TSan clean; preflight
  - [ ] Xapiand HTTP layer check against this branch (what X3 must change)
- Done: see the log.
- Doing: sanitizer builds queued behind other holds; mutants for http.
- Next: TSan; mutants; http-log and prism builds; Xapiand scratch build; preflight; push; code review.
- Blocked on: nothing.
- Findings folded in: none yet (not reviewed).
- Last verified: macOS x86_64, Apple clang, Debug: reactor 9/9 and http 10/10 (+ fuzz replay) ctest.

## Log
- 2026-10-30: reactor `67e22a6`: pool removed, `AdmissionGate`, intrusive abort list, `abort_all`/`force_all` on the
  loop, shutdown grace. http `bda9b5a`: coroutine handlers, `BodyStream`, cancel/watch, admission, cap, tests.
