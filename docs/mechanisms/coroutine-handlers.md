> Status: CURRENT (2026-11-09) the mechanism on `task::task` (design `docs/design/task-handlers.md` revision 2, steps H1 and H2; ledgers `docs/status/task-h1.md`, `docs/status/task-h2.md`). Supersedes the Phase 7c L3 note (`asio::awaitable` handlers, http:679c0f8).

# Coroutine handlers

How a request runs, is cancelled, is admitted and stops at shutdown. `http_asio.h`
(`serve_connection_body`) is the code; `test/coroutine_test.cc` proves each statement.

## Everything runs on the connection's loop, as tasks

`HttpHandler::handle(Request&, ResponseWriter&)` returns `task::task<void>`. Each reactor
owns one `task::asio_scheduler` over its `io_context` (`reactor(i).scheduler()`); the
connection is a task on it (a `reactor::Session`), and it awaits the handler as a child
task. task starts the child by symmetric transfer and the child's final suspend transfers
back, so a handler that finishes in its first run costs no post and no Asio operation. A
handler that waits suspends; the loop serves other connections; the handler resumes on the
same scheduler. There is no worker pool and no thread handoff.

A handler reaches its loop through the request: `request.scheduler()` is the connection's
scheduler, and its `executor()` builds timers, sockets and `task::deadline`s there. Asio
operations are awaited with `task::await_asio`, which takes an optional `task::cancel_token`.
Work that finishes on another thread resumes the handler by posting to its scheduler. A
handler may move a child to another scheduler with `task::schedule_on`; the child comes home
at its final suspend. The writer and `BodyStream::read` assert in Debug builds that they run
on the connection's loop thread, whatever scheduler wraps the task there (a cancel callback
the transport runs in a raw Asio handler passes too).

## A request's life

```
headers complete   gate.enter() -- empty: 503 + Retry-After: 1, close      (admit)
                   create_extension, then BodyStream | BodySink | buffer (cap: 413)
handler start      RequestAbort on the reactor's list; ConnectionInput::watch set (on plain
                   TCP it reads once the handler first asks for its token)
handler runs       co_await handler->handle(request, writer)          (a child task)
                     client reset (read error) -> request.cancel(http::disconnected)
                     client EOF (half-close)   -> reading stops; nothing is cancelled
                     abort_all() at shutdown   -> request.cancel(task::cancel_reason::shutdown)
handler returns    unwatch; an escaped exception is mapped; the response is written
                     the write fails           -> request.cancel(http::disconnected), close
settled            gate lease released; RequestAbort leaves the list; keep-alive continues
```

The request, the writer and the `BodyStream` are locals of the connection task, declared
before the awaited handler: a frame ends only by completing, and a teardown that destroys the
connection's frame destroys the handler's first. The session's token (`SessionContext::stop`,
requested at shutdown) reaches only the reads of the next request and the TLS handshake; the
handler, its body reads and the reply's writes never see it, so a shutdown reaches a running
request only through the request's own token.

## Cancel reads, not writes

A request has one cancellation: `request.cancel_token()`. The first call creates the
request's `task::cancel_source` (pooled) and engages the disconnect watch; `request.cancel(reason)`
requests it, first call wins, on the loop. The reason is `http::disconnected` (task's `user`)
for a hang-up and `task::cancel_reason::shutdown` for `stop()`. A handler polls
`token.requested()`, registers a `task::cancel_callback` to be woken, passes the token to
`task::await_asio` or a `task::deadline`, or bounds a computation with
`task::budget slice(200us, request.cancel_token())`, whose checkpoints throw `task::cancelled`
once it is requested and otherwise requeue the handler on the reactor's demoted queue when its
slice is spent. Writes take no token: once the request is admitted the transport attempts its
response, so a client that half-closes after a `PUT` still receives the answer. A callback that
throws is dropped by task; the next callback still runs.

One rule reads the socket watch: a read error other than EOF (a reset) is a disconnect, and EOF
is a half-close. A client may `shutdown(SHUT_WR)` after sending its request, which is legal
HTTP, and it still gets the response. So EOF ends reading and cancels nothing: the requests
already read are answered in order, no new read is started, and the connection closes after
the last response is written.

1. **A request in flight (`ConnectionInput::watch`).** A reset (any read error other than EOF
   and our own cancellation) calls `cancel(http::disconnected)`.
2. **An owned-stream reply (`ConnectionInput::monitor`).** A reset closes the
   exchange as `Disconnected`.
3. **The reply's write.** A failed write ends the connection and calls
   `cancel(http::disconnected)`, so a token the application kept past `handle()` (an owned
   stream's producer) sees it.

TCP cannot tell a half-close from a full close until the server writes: after EOF nothing more
is read, so a client that closed entirely is seen only when the reply's write fails. A handler
that waits after a half-close is therefore ended by its own bound (a search's budget, a long
poll's maximum wait, `Request::deadline()`) or by shutdown, never by the transport's
reads; its response is then written, or its write fails and the connection closes. The
transport's request timeout bounds the request's reads and writes, not the handler.

The watch reads ahead: one pending read of up to 4096 bytes into the connection's input
buffer (`ConnectionInput::kReadAhead`). If the client pipelines the next request while a
handler runs, the read takes it, or its first 4096 bytes, and that data ends the watch for
the rest of that request: a disconnect after it is not seen until the connection reads
again. `read()` returns the bytes first and in order, and nothing more is read ahead until
they are all consumed, so a connection holds at most 4096 bytes of input outside the kernel.
The pending read is also the read for the next request: the connection's next read finds it
pending and is resumed by its completion, with no coroutine frame or channel between them. A
watch armed after a reset was already seen cancels the request at once; one armed after EOF
reads nothing more.

On plain TCP a buffered request's watch waits for the handler to ask. The request holds a
`CancelWatch` hook that the first `cancel_token()` engages: one read that does not wait (`recv`
with `MSG_DONTWAIT` into the input buffer) applies the rules above to what the client did
meanwhile (data waiting is read ahead and ends the watch; EOF ends it too; a socket error
cancels the request before the call returns, so the token is already requested), and with nothing waiting
the watch waits for the socket to be readable and then reads without waiting. An asynchronous
read would first try the socket at once, a second `recv` after the one that just found it
empty; so an asking request costs the same two `recv` calls as one that never asks (one that
finds nothing, one that reads the next request). A handler that never asks pays for no read. A streamed body's watch starts when the
body is complete, the same way: lazy, or armed at once when the handler already asked during the
body. A TLS stream cannot be asked this way (its records must be decrypted to tell a
close_notify from data), so its watch arms its read when set.

## Exceptions

An exception that escapes `handle()` is mapped at the connection: an owned stream whose head
is on the wire closes the connection without its terminal chunk, so the client reads a
truncated body; otherwise the application's `on_error` answers, and what it leaves unanswered is a `500`, or a `503` with `Connection: close` when
the exception is `task::cancelled` and the request's token was requested. `task::cancelled`
from the handler's own source is any exception.

## Streamed bodies

A route that returns true from `wants_body_stream` gets `Request::body_stream`, a
`BodyStream`: a bounded channel (8 chunks) that the connection feeds while the handler reads
it. The feed and the handler are children of the connection task, joined with
`task::when_all`: no spawn, no post.

```cpp
std::string chunk;
asio::error_code ec;
while (!(ec = co_await request.body_stream->read(chunk))) { use(chunk); }
// ec == asio::error::eof: the whole body arrived
```

`read(chunk, token = {})` returns no error with a non-empty chunk, `asio::error::eof` at the
end (sticky), `operation_aborted` when its own token was requested (nothing is lost; the
next read goes on), or the error the body failed with: `connection_reset` when the client
ended before the body was complete (a close, a half-close, a reset, or a chunked body cut
short), `timed_out` at the inactivity or request timeout, `operation_aborted` at shutdown.
The request's token is already requested when a failed read returns. `eof` means only a
complete body. Buffered chunks are delivered before the end or the error. A full channel
suspends the feed, so back-pressure reaches the socket. When the handler returns, it
requests the feed's source (linked to the session's token, so shutdown also stops the feed):
a handler that answers without reading the whole body stops the feed's read and push, and
the connection closes after the response. Once the body is complete and the handler still
runs, the disconnect watch is set (lazily, as for a buffered body). The stream is not
thread-safe.

## Admission and the body cap

The reactor's `AdmissionGate` counts requests from their first byte to the settled
response (`Limits::max_in_flight`, 0 for no cap), so `wait_requests()` sees a request
whose headers are still arriving. Over the cap, the connection answers `503` with
`Retry-After: 1` at headers-complete and closes without reading the body. A
second cap of the application's own is a throw of `RequestRejected(503, ..., retry_after)`
from `create_extension`. The lease is a local of the connection task: completion or the
frame's destruction releases it once.

A body buffered into `Request::body` is capped (`Limits::max_body`, 16 MiB by default): a
`Content-Length` over the cap is a `413` at headers-complete, before any body byte is
read, and a chunked body is a `413` the moment it passes the cap. Streamed and sunk
bodies are not buffered and not capped.

## Shutdown

Teardown is task's four phases, each a call on `HttpAsioService` forwarded to reactor's
`TcpServer`; each runs the earlier ones first:

1. `stop()`: `abort_all()` on each reactor's loop requests every session's token and
   cancels every running request (`task::cancel_reason::shutdown`); an owned-stream
   reply closes with `Shutdown`. The acceptors close. An idle keep-alive
   connection ends at once, a streamed body read fails with `operation_aborted`, handlers
   stop at their next checkpoint and their responses are written. After the grace
   (`Limits::shutdown_grace`, 5 s by default) `force_all()` closes the sockets still held
   and sessions get the grace once more; then the loops stop and join. `stop()` is bounded
   by about twice the grace.
2. `close()`: the reactors' schedulers drop what is posted and destroy what is spawned.
3. `destroy_roots()`: the tasks still suspended (a handler that ignored its token) are
   destroyed with every `io_context` alive. Their responses go nowhere.
4. Destruction runs any phase not yet run.

An application with its own schedulers runs each phase across http's reactors and its own
before the next.

Phase 1 also runs in steps, so in-flight requests finish before anything is cancelled:
`stop_accepting()`, `wait_requests(token)`, `cancel_requests()`, then `stop()`. The service's
own `stop_accepting()` sets a flag every connection reads (reactor's listener state is
private to `TcpServer`), so callers stop through the service. A request reads it at its
first byte, after entering the gate, with a sequentially consistent fence between the two:
either `wait_requests()`, which reads the gates after the flag's store, counts the request,
or the request reads the flag set and is refused with `503` at headers-complete, holding no
lease. A request already counted is served, and its reply, like every reply framed once the
flag is set, says `Connection: close`. A reply framed before the flag keeps its connection open,
as it said: the decision is read once, and the next request on it is refused. The window `wait_requests()` cannot see is an accepted
connection whose first byte the server has not read: that request is refused.

## Closing idle connections

`close_idle()` closes every connection waiting for a request at once and every other one
after its reply. Two mechanisms carry it. A connection waiting for its next request's first
byte is on its loop's idle list (a `reactor::AbortList`, touched on the loop only, no
allocation); `close_idle()` posts a walk of each list to its loop, and the walk ends each
wait the way a requested token does (`ConnectionInput::cancel_read()`), so the connection
closes. And `close_idle()` bumps a count each connection read at its start: a connection
that sees it changed frames its reply with `Connection: close` and closes, or closes at its
next idle wait, so one that was busy, or between its read and the walk, still closes after
its reply. A connection accepted before the call whose session starts after it (still queued,
or in its TLS handshake) reads the new count and stays open. `Reactor::track` is an intrusive list touched on the loop only: O(1) per
request, no allocation, no lock.
