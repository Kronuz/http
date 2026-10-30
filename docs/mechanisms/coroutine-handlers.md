> Status: CURRENT (2026-10-30) the mechanism as built in Phase 7c L3 (`docs/status/phase7c-l3.md`); workstream phase7c/l3-http.

# Coroutine handlers

How a request runs, is cancelled, is admitted and stops at shutdown. `http_asio.h`
(`serve_connection_body`) is the code; `test/coroutine_test.cc` proves each statement.

## Everything runs on the connection's loop

`HttpHandler::handle(Request&, ResponseWriter&)` returns `asio::awaitable<void>` and the
connection coroutine awaits it. A handler that must wait suspends; the loop serves other
connections; the coroutine resumes on the same `io_context`. There is no worker pool, no
`should_offload`, and no thread handoff. Anything that finishes on another thread (a
chore, a device read) resumes the handler by posting to the loop, which is what
`task::await_asio` and `task::as_awaitable` do.

## A request's life

```
headers complete   gate.enter() -- empty: 503 + Retry-After: 1, close      (admit)
                   create_extension, then BodyStream | BodySink | buffer (cap: 413)
handler start      RequestAbort on the reactor's list; ConnectionInput::watch armed;
                   connection coroutine's cancellation disabled
handler runs       co_await handle(request, writer)
                     client EOF / read error   -> Request::cancel(Disconnect)
                     abort_all() at shutdown   -> Request::cancel(Shutdown)
handler returns    unwatch; the response is written (a cancelled request still gets it)
settled            gate lease released; RequestAbort leaves the list;
                   cancellation re-enabled; keep-alive loop continues
```

The request and the writer belong to the connection coroutine and live until the handler
returns. Nothing calls `destroy()` on a suspended handler: the connection coroutine's
cancellation is disabled from just before the handler starts until the response is
written, so an Asio cancellation aimed at the session cannot unwind the frame under the
handler. It is dropped, not queued, so the keep-alive loop checks `reactor->stopping()`
before reading the next request.

## Cancel reads, not writes

`Request::cancel(reason)` runs the callback registered with `Request::on_cancel` (once, on
the loop) and sets `cancelled()`. A handler stops reading at its next checkpoint, for
example by requesting a `task::cancel_source` from the callback. Writes take no token:
once the request is admitted its work completes and the transport attempts its response,
so a client that half-closes after a `PUT` still receives the answer. A task that is
queued on a lock, an event or a chore is not woken by the callback; it checks the flag
when it resumes.

Two rules read the one-byte socket watch, and they differ on purpose:

1. **A request in flight (`ConnectionInput::watch`).** EOF and any read error other than
   our own cancellation call `cancel(Disconnect)`. A client that closed has nobody to
   read what a search computes. This is wider than the deferred-reply monitor's rule
   below, and it matches Go's `net/http`.
2. **A deferred or owned-stream reply (`ConnectionInput::monitor`).** Only a read error
   other than EOF is a disconnect: the client may half-close after sending and still
   expect the reply. This rule is unchanged.

The watch reads one byte. If the client pipelines the next request while a handler runs,
that byte ends the watch for the rest of that request, and a disconnect after it is not
seen until the connection reads again. The byte is handed back to the parser. A request
whose bytes arrived in the same read as the previous one has no byte pending and is
watched normally. A watch armed after EOF was already seen cancels the request at once.

## Streamed bodies

A route that returns true from `wants_body_stream` gets `Request::body_stream`, a
`BodyStream`: a bounded `asio::experimental::channel` (8 chunks) that the connection
coroutine feeds while the handler, spawned on the same loop, reads it:

```cpp
asio::error_code ec; std::string chunk;
co_await request.body_stream->async_read(chunk, asio::redirect_error(asio::use_awaitable, ec));
// a task: co_await task::await_asio<void(asio::error_code)>([&](auto h) { stream.async_read(chunk, std::move(h)); });
```

`ec` is clear with a non-empty chunk, `asio::error::eof` at the end (sticky), or the error
the body failed with (`connection_reset` when the client closed mid-body,
`operation_aborted` at shutdown, the read deadline's error). Buffered chunks are
delivered before the end or the error. A full channel suspends the feed, so back-pressure
reaches the socket. If the handler answers without reading it all, the feed is cancelled
and the connection closes after the response. The stream is not thread-safe; use it on
the loop.

## Admission and the body cap

The reactor's `AdmissionGate` counts requests from headers-complete to the settled
response (`HttpAsioService`'s `in_flight_limit`, 0 for no cap). Over the cap, the
connection answers `503` with `Retry-After: 1` and closes without reading the body. A
second cap of the application's own (Xapiand's demoted-handle count) is a throw of
`RequestRejected(503, ..., retry_after)` from `create_extension`, with its own count.

A body buffered into `Request::body` is capped (`set_max_body`, 16 MiB by default): a
`Content-Length` over the cap is a `413` at headers-complete, before any body byte is
read, and a chunked body is a `413` the moment it passes the cap. Streamed and sunk
bodies are not buffered and not capped.

## Shutdown

`HttpAsioService::stop()` (reactor `TcpServer::stop()`):

1. `abort_all()` on each reactor, run on that reactor's loop: every tracked
   `Abortable` is asked to stop. A running request is `cancel(Shutdown)`; a deferred or
   owned-stream reply closes with `Shutdown`; an idle session's cancellation is emitted.
2. The acceptors are closed and reaped.
3. Sessions finish: reads end at their checkpoint, writes complete, an idle keep-alive
   connection ends at once, a streamed body read in progress fails with
   `operation_aborted`.
4. After the grace (`set_shutdown_grace`, 5 s by default) `force_all()` runs on what is
   still tracked: each remaining request closes its socket, so a write to a peer that
   stopped reading fails.
5. Wait for every session; stop the loops; join.

The bound on shutdown is the grace plus the time to unwind the sessions. A handler that
never returns, ignoring `Request::cancelled()`, holds `stop()` for as long as it runs:
the request's frame is never destroyed early, so the runtime waits for it.
`Reactor::track` is an intrusive list touched on the loop only: O(1) per request, no
allocation, no lock.
