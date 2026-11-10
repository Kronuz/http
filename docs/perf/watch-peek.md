# The disconnect watch's extra read

What a request costs when its handler asks for its cancel token, which engages the connection's disconnect watch
(`CancelWatch`, `docs/mechanisms/coroutine-handlers.md`), against one whose handler never asks, and what removes the
difference. Xapiand's `SearchService::handle` asks on every request, so every Xapiand request paid it.

## Where it went

Before (http:8262172, the library code of main at 66050ec plus the `httpc` bench kind), an asking request did, after
its handler asked and before its reply:

1. `recv(MSG_PEEK | MSG_DONTWAIT)` of one byte: empty (`EAGAIN`).
2. `async_read_some` into the read-ahead buffer, which Asio tries at once (`kqueue_reactor::start_op` and
   `epoll_reactor::start_op` perform a read operation speculatively when nothing is queued): a second `recv`, empty.
3. After the reply, the next request's read found that read pending and went through `read_held` and `drain()` (two
   task frames) to a one-slot channel that the read's completion posted to.

A request whose handler never asks does one empty `recv` and the one that reads the next request. So asking cost one
more `recv`, two task frames, a channel operation and two more Asio handlers per request.

## The change

1. `engage()` reads without waiting into the input's buffer (`recv` with `MSG_DONTWAIT`): data is read ahead and ends
   the watch, EOF or an error cancels the request before `cancel_token()` returns, as the peek decided. With nothing
   there, the watch on plain TCP waits for readability (`async_wait`, which Asio does not try at once) and then reads
   without waiting. (http:f43dee4)
2. The pending read's completion resumes the coroutine waiting on it in place, by the rule `task::await_asio` uses
   (in a top-level handler on the waiter's loop, inside its scheduler's scope; else posted): no frame, no channel. A
   reader's token still ends the wait by cancelling it on the socket; `drain()` waits the same way. (http:6ec7324)

## Counts

Asio handler invocations per request, counted exactly by `test/handler_count_test.cc` (Asio's custom handler
tracking), Release, macOS:

| route | before (base header, job 261109-145905-5a00d5) | after (http:13933f0, job 261109-145852-514f5a) |
| --- | ---: | ---: |
| raw reactor session (the reference) | 1 | 1 |
| handler that never asks (`/flat`) | 1 | 1 |
| handler that asks (`/asks`) | 3 | 1 |
| streamed body, never asks | 7 | 7 |
| streamed body, asks (`/stream-asks`) | 9 | 7 |

`recv` calls per asking request (from the code paths above): 3 before, 2 after, the same as a request that never asks.

## Timing

`bench/watch_ab.sh run` as an `exclusive` job (261109-150350-f39012, 6 interleaved rounds of 4 s per series, 2
reactors, `wrk` keep-alive; i9-9980HK, macOS, x86-64 Release binaries checked with `file`): after is http:13933f0,
before is http:8262172, and a copy of the after binary is the A/A control. Rates and CPU are medians over the rounds;
the ratio is the median per-round ratio with its range, beside the A/A control's per-round range; a row counts when
its range sits on one side of 1 and its ratio lies outside the A/A range.

| kind | conns | req/s before | req/s after | after vs before | A/A range | CPU us/req before | after | verdict |
| --- | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |
| `httpc` (asks) | 1 | 16,570 | 23,688 | 1.39x faster (1.37-1.98) | 1.00-1.05 | 24.4 | 18.8 | 21% less CPU |
| `httpc` (asks) | 8 | 92,414 | 105,055 | 1.10x faster (1.06-1.18) | 0.95-1.08 | 17.4 | 15.3 | 11% less CPU |
| `httpc` (asks) | 64 | 131,346 | 133,057 | 1.03x (0.82-1.11) | 0.91-1.22 | 13.7 | 13.4 | noise |
| `http` (never asks) | 1 | 23,359 | 24,515 | 1.04x (1.01-1.22) | 0.81-1.00 | 18.9 | 18.1 | noise |
| `http` (never asks) | 8 | 105,463 | 105,522 | 0.98x (0.92-1.05) | 0.97-1.04 | 14.3 | 14.2 | noise |
| `http` (never asks) | 64 | 133,364 | 130,363 | 1.00x (0.89-1.03) | 0.95-1.02 | 12.4 | 12.7 | noise |

What asking costs, `httpc` against `http` in the same binary (medians):

| conns | before: req/s, CPU us/req | after: req/s, CPU us/req |
| ---: | --- | --- |
| 1 | 29% fewer req/s, +5.5 us | 3% fewer req/s (inside the A/A spread), +0.7 us |
| 8 | 12% fewer req/s, +3.1 us | the same req/s, +1.1 us |
| 64 | 2% fewer req/s, +1.3 us | 2% more req/s, +0.7 us |

The change removes most of what asking costs where a connection waits on its reply: 1.39x faster at 1 connection
and 1.10x at 8. At 8 the lowest round (1.06) reaches into the top of the A/A range (1.08), so that win is the smaller
and less certain one. At 64 connections the reactor is saturated and the A/B is noise (its range straddles 1). A
request that never asks is unchanged at every count. A residual of 0.7 to 1.1 us of CPU per asking request remains,
not attributed; the counting build shows the same handlers and allocations per request for both kinds
(`counts-*.txt` in the job's output: 1 handler and 3 allocations, against 3 and 6 before).

E1 alone (http:f43dee4, one `recv` fewer) is noise against before at every row (medians 0.99 to 1.02); E2's removal of
the two task frames and the channel carries the gain. The strict screen (261109-144804-ff5f5b, 3 rounds of 3 s, at
http:90cf235 whose library code is the head's) agrees: `httpc` 1.40x faster at 1 connection and 1.09x at 8.

In cost: 2.1 us of CPU less per asking request at 8 connections is 0.21 of a core per 100,000 asking requests a
second; every Xapiand request asks (`SearchService::handle`).

## Reproduce

```sh
REFS="base=/path/to/checkout-at-8262172 e1=/path/to/checkout-at-f43dee4" sh bench/watch_ab.sh build
OUT=.scratch/watch-ab/confirm ROUNDS=6 SECS=4 REFS="..." sh bench/watch_ab.sh run
sh test/watch_mutants.sh   # each claim's mutant fails readahead_test
```
