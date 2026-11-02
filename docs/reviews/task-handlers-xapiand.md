# task-handlers: notes from the Xapiand lead

> Status: CURRENT (2026-11-02) review of `docs/design/task-handlers.md` at http:b110118 for Xapiand's
> fit and the owner's rulings. Not the architect's review.

## The owner's ruling this design predates

The owner ruled on 2026-11-02, after this draft was written, that there is one shape everywhere,
in the Zen of Python's sense: one obvious way to do each thing, here one coroutine type inside
libraries as well as at their API, and one way to ask about cancellation. The exception is
practicality: a cost gap that is considerable and cannot be narrowed or closed. So if `task` costs
more somewhere, the work is first to make `task` cheaper there; a mixed shape is acceptable only
when that work is done and the remaining gap is still considerable, shown with numbers (or the case
is made for dropping `task` everywhere). Two places in the draft conflict:

1. **D8.** The connection loop moves to `task` by default; T6 and T7 first decide what to optimize,
   and only a considerable gap that survives the optimization keeps the loop on `asio::awaitable`. The cost D8 names (`await_asio` resumes through `scheduler.post`, one more
   queue trip per socket completion) is the first target: when the completion already runs on the
   scheduler's own thread, `await_asio` can resume the awaiting coroutine inline (symmetric
   transfer) instead of posting. That is a change in task, measured on its own bench and then on T6
   and T7.
2. **D1's transport seams.** `ConnectionFactory`, `serve_connection`, `serve_tls` and
   `BodyStream::push` stay `asio::awaitable` because `reactor::Session` is. Reactor is ours (its
   consumers are http, Detent and cluster), so out of scope can only mean "the next step", not
   "stays". Name it as a step of this design: reactor's `Session` becomes `task`-based, and the seams
   follow. Until that step lands, the seams are a known, dated exception in the ledger, not a design
   decision.

## The questions

- **Q1, the scheduler: (a), with one change.** One `task::asio_scheduler` per reactor is right, and
  Xapiand's `AsioHost` wraps it instead of its own (`host_asio.h:49`, `manager.cc:955`), which keeps
  the admission count on one demoted queue per loop. The symmetric owner is the **reactor**, not
  `HttpAsioService`: the reactor owns the loop, so it owns the loop's scheduler, and http exposes it
  through `reactor(i).scheduler()`. That also gives Detent's non-HTTP owner loops the same scheduler
  without depending on http. If that waits for the reactor step, (a) as drafted is fine as the
  interim, with the move noted.
- **Q2, cancellation: one mechanism.** `cancelled()` and `cancel_token().requested()` are two ways to
  ask one question. Make the token carry the reason, register `on_cancel` callbacks on the token, and
  remove `cancelled()` and `cancel_reason()` from `Request`. The churn is a rename in every consumer,
  and the L3 probes change their assertions, not their scenarios.
- **Q3: (a) first, then the task loop.** Under the ruling, (b) is the default; (a)'s inline-start
  primitive and the inline `await_asio` resume above are how the task loop meets T6 and T7. (c) is
  the exception, not a fallback: it applies only if both still leave a considerable gap, and that
  case goes to the owner with the numbers before or after each optimization.

## Xapiand's migration

The six-item note is right and complete for Xapiand. Items 1 to 5 are mechanical; item 6 follows Q1.
Xapiand also drops `task::as_awaitable` in `SearchService` (the bridge this design exists to remove),
and its `Run::await` (`host_asio.h:131-145`) as item 2 says. Xapiand migrates when http lands on
main, as one commit on its local master, with its docs E2E (641 assertions), `stall_load.py` and the
Lua flood harness rerun.

## The L3 constraints

D4, D5 and D7 carry the L3 review's constraints as I listed them. Rerun every probe in
`~/code/Kronuz/http-wt/l3-http/.scratch/review-l3/` on the new API, adapting only the API glue, and
record each probe's result in the ledger.
