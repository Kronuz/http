# Per-request CPU and latency, timeouts off and on

What a keep-alive request costs in `HttpAsioService` against a minimal Asio server that answers the same bytes,
with no timeouts and with the timeouts a production service sets, and what each part of the connection input
contributes.

## Method

`bench/server_compare.cc` builds `http_server_compare` (`-DHTTP_BUILD_BENCH=ON`). Each kind answers a 14-byte
reply (`Hello, World!\n`, `Content-Type: text/plain`) to every request on one reactor:

| kind | what answers |
|---|---|
| `asio` | plain Asio: one acceptor on loop 0 handing sockets round-robin, a read loop that finds the blank line and writes the reply |
| `reactor` | `reactor::TcpServer` with the same session body (the runtime without the http library) |
| `http` | `HttpAsioService` and a handler that writes the reply, no timeouts |
| `httpt` | the same with the request (30 s), idle (60 s), header (10 s) and inactivity (30 s) timeouts |

`bench/run.sh` drives one server with `wrk` for 4 s after a 1 s warm-up, at one connection (`wrk -t1 -c1`) and 64
(`wrk -t2 -c64`), and reads the server's user plus system CPU time from `getrusage` (SIGUSR2) over the timed run:
CPU per request is that time over the requests `wrk` counted. `bench/compare.sh` runs `asio`, `http` and `httpt` at
both connection counts for several rounds, rotating the builds it is given, and `bench/summarize.py` takes medians.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON && cmake --build build --target http_server_compare
sh bench/compare.sh build/http_server_compare > out.txt      # REPS=3 rounds
python3 bench/summarize.py < out.txt
```

To compare revisions, build each into its own directory, copy the binaries to distinct names and pass them all to
`compare.sh`. Run on a quiet machine: the spread over the rounds is in the table.

## Numbers

Median of three rounds; latency is the `wrk` average in microseconds, CPU is per request in microseconds, req/s is
at 64 connections. Release build (`-O3 -DNDEBUG`), Apple clang 17.

| machine | kind | 1 connection: latency / CPU | 64 connections: req/s / CPU |
|---|---|---|---|
| Apple M4 Pro, arm64 | `asio` | 60.9 / 29.1 | 41,785 / 24.5 |
| | `http` | 80.1 / 35.6 | 42,059 / 25.4 |
| | `httpt` | 95.6 / 45.8 | 41,888 / 28.7 |
| Intel Core i9-9980HK, x86_64 | `asio` | 33.2 / 22.9 | 130,595 / 9.6 |
| | `http` | 55.2 / 36.1 | 85,420 / 14.9 |
| | `httpt` | 86.8 / 67.6 | 48,423 / 26.1 |

The M4 Pro rows are `http_server_compare` with `bench/compare.sh` (spread over the rounds 1% to 6%). The i9 rows are
the same server code measured on an idle host (spread 0% to 3%); on that host with other work running (load average 8
to 10) it reads 6% to 25% worse on every row and orders the kinds the same way.

`wrk` saturates at about 43k req/s on the M4 Pro, so there the CPU column separates the servers at 64 connections;
the i9 does not saturate it. The `reactor` kind (`sh bench/run.sh reactor 1 1 4`) runs the Asio session body on
`reactor::TcpServer`, to separate the runtime from the HTTP layer.

## Where the time goes

With the timeouts on, `http` spends 1.6x (M4 Pro) to 3.0x (i9) the CPU of a minimal server on a keep-alive request
at one connection, and most of the gap is the connection's input and its deadlines:

- **The disconnect watch.** While a handler runs, one read stays pending on the socket so a hang-up cancels the
  request. The watch reads up to 4096 bytes ahead into the connection's own buffer, and that pending read is the next
  request's read, so a keep-alive request costs one `recv`. A watch that read one byte cost a second `recv` for the
  rest of the request and a `kevent`.
- **The wake-up.** A completed read wakes whoever waits for it through a capacity-1 `asio::experimental::channel`. A
  timer used as a condition variable costs a pipe `write` and `read` and a `kevent` each time on kqueue when it is the
  earliest timer.
- **A deadline per operation.** Each read and write with a timeout is a timer and an `||` of two coroutines. The reply
  is written in slices of up to 64 KiB of head and body together, so a small reply costs one timer and one send, not
  one per buffer.

An ablation of prototypes of these parts (Apple M4 Pro, one connection, 4 s runs, counts per request from an
interposer on the allocator and the socket calls) shows what each is worth:

| connection input | latency, off | CPU, off | mallocs, `kevent`, `recv` | latency, on | CPU, on |
|---|---|---|---|---|---|
| one-byte watch, timer as condition variable, buffers sliced separately | 91.9 | 41.0 | 12, 5, 3 | 143.8 | 81.5 |
| no watch (a bound, not a design) | 61.4 | 30.9 | 7, 2, 2 | | |
| read-ahead watch | 78.0 | 36.3 | 9, 4, 2 | 123.9 | 71.9 |
| read-ahead watch, channel, one buffer | 78.1 | 34.9 | 11, 3, 2 | 94.8 | 44.9 |
| minimal Asio | 59.5 | 28.2 | 0, 2, 2 | | |

The timer replacement and the single buffer matter only with the timeouts on, where the timer race and the second
`sendmsg` are paid per read and per write (143.8 to 94.8 us). At 64 connections with the timeouts off all variants
cost 25.3 to 25.7 us of CPU per request, and the read-ahead is a latency change there, not a throughput one.

What the watch must keep (`docs/mechanisms/coroutine-handlers.md`): `Request::cancel(Disconnect)` on a hang-up while
a request runs, a pipelined byte or request read by the watch going back to the parser in order, every timeout's
meaning, and that a watched read is cancelled but a write never is. `test/readahead_test.cc` and
`test/timeouts_test.cc` pin each of them.
