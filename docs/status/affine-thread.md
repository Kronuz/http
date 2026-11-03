> Status: CURRENT (2026-11-03) the ledger for the D9 thread-affinity rule (lead's ruling).

# affine-thread (http)

- Branch / worktree: `phase/affine-thread` at `~/code/Kronuz/http-wt/affine`, from http:93dc7bb.
- State: done; CI in the Log.

## Done

- [x] `AsioResponseWriter::affine()` asserts `!reactor_ || reactor_->on_loop()`; `BodyStream::affine()` asserts `scheduler_.runs_here()`: the rule is the loop thread, not the scheduler's identity.
- [x] Test `coroutine_test` [T] (h) `/affine-wrapped`: a handler under a wrapper scheduler that resumes on the loop thread reads a streamed body and writes. With the old asserts: abort, rc 134.
- [x] Death tests `/affine-body`, `/affine-read` (another scheduler on another thread) and `/affine-thread` (raw thread) still die; no rewrite needed.
- [x] Mutant: empty `affine()` in both: the three death tests fail.
- [x] Debug ASan+UBSan and TSan builds: ctest 13/13 each.
- [x] Docs: AGENTS.md, CHANGELOG.md, `docs/mechanisms/coroutine-handlers.md`.

## Next

Lead merges after CI.

## Blocked on

Nothing.

## Log

- 2026-11-03: change, test, proof.
