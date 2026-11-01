# Server comparison: latency and CPU per request

What a keep-alive request costs in `HttpAsioService`, in the same configuration with the timeouts a production service
sets, and in a minimal Asio server, Boost.Beast and cpp-httplib answering the same bytes, and what each part of the
connection input contributes.

## Method

Every server answers `GET /` with the same 103-byte reply: `HTTP/1.1 200 OK`, `Content-Type: text/plain`,
`Content-Length: 14`, `Connection: keep-alive` and the body `Hello, World!\n`. One reactor or one thread serves it. The
`kind` is the argument a server takes (`http_server_compare KIND PORT REACTORS`):

| kind | what answers | built by | version |
|---|---|---|---|
| `asio` | the lower bound: plain standalone [Asio](https://github.com/chriskohlhoff/asio), one acceptor on loop 0 handing sockets round-robin to one `io_context` per reactor, a read loop that finds the blank line and writes the reply; not a framework | `bench/server_compare.cc` | Asio 1.36.0, the tag `CMakeLists.txt` fetches |
| `reactor` | `reactor::TcpServer` with the same session body (the runtime without the http library); not in the table | `bench/server_compare.cc` | |
| `http` | `HttpAsioService` and a handler that writes the reply, no timeouts | `bench/server_compare.cc` | this checkout |
| `httpt` | the same with the request (30 s), idle (60 s), header (10 s) and inactivity (30 s) timeouts; a configuration of `http`, not another server | `bench/server_compare.cc` | this checkout |
| `beast` | [Boost.Beast](https://github.com/boostorg/beast) on Boost.Asio (not the standalone Asio): the same acceptor layout, one `io_context` per thread, `http::async_read` and `http::async_write` in coroutines | `bench/server_beast.cc` (`-DHTTP_BENCH_BEAST=ON`) | Boost 1.92.0 from Homebrew |
| `httplib` | [cpp-httplib](https://github.com/yhirose/cpp-httplib), a thread pool of REACTORS threads, the keep-alive request cap lifted | `bench/server_httplib.cc` (`-DHTTP_BENCH_HTTPLIB=ON`) | v0.64.0, a single header fetched by the bench target |

The two machines use the same versions of everything: Asio 1.36.0 (fetched at that tag; the Homebrew Asio is not used),
Boost 1.92.0, cpp-httplib v0.64.0 and [wrk](https://github.com/wg/wrk) 4.2.0 (Homebrew). Both are built with the same
compiler and flags (Apple clang 17, CMake's Release flags `-O3 -DNDEBUG`, C++20).

`bench/run.sh` drives one server with `wrk` from the same host over loopback, for 4 s after a 1 s warm-up, at one
connection (`wrk -t1 -c1`) and 64 (`wrk -t2 -c64`), and reads the server's user plus system CPU time from `getrusage`
(SIGUSR2) over the timed run: CPU per request is that time over the requests `wrk` counted. `bench/compare.sh` runs the
kinds at both connection counts for several rounds, rotating the builds it is given, and `bench/summarize.py` takes
medians and the spread of the latency over the rounds. `BUILD`, `KINDS`, `CONNS` and `REPS` select a piece of the run.

**Same bytes.** `bench/compare.sh` first runs `bench/verify.py`, which starts each server, sends the request `wrk`
sends twice on one connection and compares the answer with the reference reply byte for byte, headers included. It
prints every mismatch and the run aborts. `http` always states `Connection: keep-alive`, so the other servers state it
too; the field order is `Content-Type`, `Content-Length`, `Connection`, and no server sends `Date` or `Server`. Beast
sets its fields in that order, and cpp-httplib is given a header writer that writes them in that order and a post-routing
handler that drops its `Keep-Alive` field.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_BEAST=ON -DHTTP_BENCH_HTTPLIB=ON
cmake --build build --target http_server_compare http_server_beast http_server_httplib
sh bench/compare.sh > out.txt      # REPS=3 rounds
python3 bench/summarize.py < out.txt
```

To compare revisions, build each into its own directory, copy the `http_server_compare` binaries to distinct names and
pass them all to `compare.sh`. Run on a quiet machine: the spread over the rounds is in the tables.

## Numbers

Median of three rounds; latency is the `wrk` average in microseconds, CPU is per request in microseconds, req/s is at
64 connections. Release build, Apple clang 17.

| machine | server | 1 connection: latency / CPU | 64 connections: req/s / CPU |
|---|---|---|---|
| Apple M4 Pro, arm64 | minimal Asio (`asio`) | 70.8 / 32.7 | 34,823 / 28.7 |
| | `http` | 87.7 / 38.4 | 35,232 / 29.5 |
| | `http`, timeouts on (`httpt`) | 104.0 / 48.5 | 35,773 / 32.6 |
| | Boost.Beast | 73.2 / 35.5 | 35,312 / 29.9 |
| | cpp-httplib | 73.7 / 33.2 | 13,326 / 33.4 |
| Intel Core i9-9980HK, x86_64 | minimal Asio (`asio`) | 33.8 / 23.4 | 126,259 / 9.9 |
| | `http` | 56.6 / 36.9 | 84,642 / 15.0 |
| | `http`, timeouts on (`httpt`) | 87.1 / 68.1 | 47,946 / 26.5 |
| | Boost.Beast | 39.6 / 31.2 | 86,066 / 14.7 |
| | cpp-httplib | 43.0 / 29.1 | 22,472 / 29.1 |

The spread of the latency over the three rounds (maximum minus minimum, over the median) is 1% to 4% on every row of
the M4 Pro and 0% to 2% on the i9, except `http` at one connection on the i9, where one round's average read 116 us
beside 56.7 and 56.0 us (its median is 48 us in every round, so the average is one slow request) and the spread reads
106%; the table quotes the median. A second run on the M4 Pro (the host carried load from other work in both) agrees
within 8% on every cell and orders the servers the same way.

At one connection `http` takes 1.2 times Beast's latency on the M4 Pro and 1.4 times on the i9, and 1.1 and 1.2 times
its CPU. cpp-httplib is faster than `http` there too, and close to Beast. With the timeouts on, `http` is the slowest row
at one connection on both machines; at 64 connections they cost no throughput on the M4 Pro, where `wrk` is the limit,
and 43% of it on the i9 (84,642 to 47,946 req/s), and 10% and 77% more CPU per request.

`wrk` saturates at about 35,000 req/s on the M4 Pro, so there four servers read the same req/s and the CPU column
separates them; the i9 does not saturate it. The `reactor` kind (`sh bench/run.sh reactor 1 1 4`) runs the Asio session
body on `reactor::TcpServer`, to separate the runtime from the HTTP layer.

**cpp-httplib at 64 connections.** It serves a connection on a pool thread until the connection ends, so with one thread
a single connection is served and the other 63 wait: with 8 connections opened at once, one answered within a second on
one thread and all 8 on 8 threads. The 64-connection row is therefore one connection's throughput (13,326 req/s, about the 13,416 of its 1-connection row). With 64 threads
(`sh bench/run.sh httplib 64 64 4`, median of three) it serves all 64 at 33,541 req/s on the M4 Pro (`wrk` bound) and
101,636 req/s on the i9, at 37.0 and 63.9 us of CPU per request.

## Where the time goes

With the timeouts on, `http` spends 1.5x (M4 Pro) to 2.9x (i9) the CPU of a minimal server on a keep-alive request
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
interposer on the allocator and the socket calls) shows what each is worth. It is a separate run with its own
minimal Asio row as the baseline, so compare its rows with each other and not with the table above:

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
