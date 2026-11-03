# task-h2 code review

> Status: CURRENT (2026-11-03) code review of `phase/task-h2` (http:adc04b5) against main http:2bede1b.

## Verdict: APPROVE TO LAND

No blocking findings. Four non-blocking notes.

## What was run

- Linux lease (g++ 13 ASan+UBSan, Debug): the 13-test suite (13/13), `probe_streamcancel`, `probe_truncated` from `.scratch/review-l3-task/` (pass, `read_err=1`, 0 reports).
- New probe `.scratch/review-h2/probe_h2.cc` (build and run via `run.sh asan|tsan [reps]`): ASan+UBSan 5 runs, clang 18 TSan 3 runs, all rc=0, 0 reports.
- `.scratch/review-h2/ra-loop.sh`: `readahead_test` (ASan build) 240 runs, six wide, beside 28 busy loops on the 32-core lease: 0 failures.
- Not run: the macOS builds, the TSan suite (the builder's and CI's runs stand), mutants (read the ledger's table and the D6-stop / D6-lazy tests instead).

## Risk rulings

1. Feed vs handler lifetimes (http_asio.h:885-956): sound. `when_all` makes both children of the connection frame, so the request, writer, `buffered`, `parser` and `tmp` the feed captures by reference outlive it; the feed returns only by completing. `run_handler` sets `handler_done`, abandons the stream and requests `feed_stop` (linked to the session token, so a shutdown stops it too). Probe cases: handler returns at once while the feed is blocked in `push` with 8 MiB incoming and more bytes arriving afterwards, with FIN and with RST, 20 repetitions (mode0); handler returns while the feed waits on the socket for a slow body, reply is written and the connection closes (mode0b); handler throws after one chunk with the feed blocked (mode1, 500); `stop()` with an ignoring handler and a blocked feed (mode4, `stop()` waits the handler's 1.4 s, the 200 reply arrives, no frame outlives). No sanitizer report, frames intact. The `catch (...)` in `feed` fails the stream so a reader is not left hung and `when_all` rethrows.
2. `BodyStream::read` / `push` (http_body_stream.h:64, :81): one path. `await_asio` binds a per-operation cancellation slot, so a cancelled read does not cancel a concurrent `push` on the same channel. Covered by [B]: already-requested token, pending read cancelled loses no chunk, deadline, sticky eof, buffered chunks before the error, push released by abandon and by token. My probe adds a cancelled read followed by a successful one in a real handler (mode5). `async_read` and `ReadOp` are gone; the only `async_read(` left in `*.h`/`*.cc` are Asio's own in `test/bench_counting_test.cc:35,43` (ledger K1 wording says none; the grep pattern is the awaitable set, so this is a wording nit).
3. Lazy streamed watch (:913, :675): sound. Handler asks after body end and the peer then sends FIN (mode2) or RST (mode2b): the token is already requested on the ask and stays so. A handler that never asks (mode3: FIN after body, answered, connection closed; mode3b: five keep-alive requests) is not left armed: `unwatch` and `set_cancel_watch(nullptr)` at :958-959 clear both, `handler_count_test` (f) pins 7 vs 9 handlers per request.
4. Flake fix: does not hide a server bug. The gap bound is `after_send_i - before_send_{i-1}`, an upper bound on two arrivals' distance, so a void attempt means the client stalled at least 0.3 s; a server that cut a moving body early (timer from body start, say) still fails every attempt whose gap stayed under 0.3 s. The sweep in the ledger shows the server exact at its timeout. `readahead` abort: not reproduced (240 runs under heavy load, ASan, abort_on_error). Cause still unknown; see N2.
5. T3 `probe_streamcancel` row: the builder is right. A body cut mid-way must fail the reader (D1: "the body's error", L3-B1 was a truncated body read as clean eof, which shows as `read_err` 0). `read_err=1` with the request cancelled `disconnected` is the correct result, and `probe_truncated` agrees (0 of 4 clean eof). The design row should be corrected to 1 (architect's text).

## Non-blocking notes

- N1: `fail_body` cancels the request with `disconnected` even when the body failed on the inactivity timeout (`timed_out`) (http_asio.h:890, :896-903); a handler watching `cancel_token().reason()` cannot tell a timeout from a hang-up. Follow-up; the read's own error already says `timed_out`.
- N2: the unexplained `readahead` abort is unresolved. The builder kept no output; the next occurrence should keep it. Not reproducible here.
- N3: the mode0 case shows a handler that answers early leaves `keep_alive` off by design (`fed != Complete`); a client that pipelines after an early answer loses the connection. Intended, worth one line in the docs if absent.
- N4: ledger K1 wording (above) and the design row for `streamcancel` (above).

## Probes kept

`.scratch/review-h2/`: `probe_h2.cc`, `probe_common.h`, `CMakeLists.txt`, `run.sh`, `ra-loop.sh`, `out-asan-*.txt`, `out-tsan-*.txt` (remote copies only for build dirs).

Not run: macOS builds, full TSan suite, Release-mode behaviour, mutants re-run, TLS streamed bodies.
