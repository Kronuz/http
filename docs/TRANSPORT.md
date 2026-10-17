# The Asio transport: deferred replies, TLS, and limits

The opt-in transport features beyond buffered `send()`/`write()`. For owned
streaming responses see [OWNED_STREAMING.md](OWNED_STREAMING.md).

## Deferred replies

`defer(DeferredOptions)` returns an owned capability when supported, or null for an existing synchronous transport. Call it before writing any response fields. Supply a finite steady-clock deadline and bounded header/body limits. `complete(Response, reply_retention)` moves one buffered response into the transport and reports acceptance; acceptance does not establish delivery. Request retention and accepted reply retention remain held until the original socket operation completes or is canceled and reaped.

Settlement runs once, outside the exchange lock, with Written, TimedOut, Disconnected, Abandoned, Shutdown or Failed. Written means the local write completed, not that the client received or acted on it. Dropping an unanswered capability abandons the reply; dropping it after acceptance preserves the accepted response. Late submission after shutdown returns Closed without accessing the retired runtime. A sending half-close preserves response interest. Peer read closure while the complete deferred response is already writing preserves its authentic write outcome; explicit timeout, abandonment and shutdown still override successful completion. Deferred replies skip compression, range and conditional transforms so their owned buffers and limits remain stable. Framing headers are transport-owned.

## Optional HTTPS transport

`http_asio_tls.h` adds `tls_connection_factory(shared_context, handshake_deadline, maximum_handshakes, maximum_connections)`, passed as the optional fifth `HttpAsioService` constructor argument. Link OpenSSL only when using this adapter. The caller configures certificates and verification policy. Incoming TLS allocation is capped by a shared handshake limit (64 by default) and total connection limit (1024 by default), and each handshake has one absolute deadline (five seconds by default). The factory retains the context through original operations.

The common HTTP parser and response path operate on the plaintext stream. A deferred response retains one owned read-ahead byte across its write, then returns that byte to the next request. Fatal read errors notify the current deferred exchange. The original four-argument TCP service API remains available without an OpenSSL dependency.

## Streaming request retirement

Shutdown aborts the body reader, then joins the original pool handler with cleanup cancellation disabled. The request and response writer remain alive until the handler returns, including after a canceled body read. This uses the reactor session completion barrier in reactor `863bed0`.

`HttpAsioService` preserves the existing ConnectionFactory and constructor APIs. An additive TimedConnectionFactory receives reactor's acceptance timestamp and an explicit sixth constructor argument sets a global connection cap before destination dispatch. `set_connection_limit()` and `connection_stats()` expose the same gate for existing factories. The timed path requires the matching reactor connection-admission API; upgrade the sibling reactor headers together when adopting it.

For TLS, `tls_connection_factory_timed()` uses the original accept timestamp so destination queue wait consumes the handshake deadline. Pass it with an explicit service connection bound. The original `tls_connection_factory()` remains source-compatible and keeps its destination-side handshake/idle-connection limits; without a service-level gate it does not bound sockets queued before the factory starts. TLS allocation follows capacity and elapsed-deadline checks. Transport leases survive original serving IO and refund on retirement.

`HttpAsioService::set_request_timeout()` optionally sets one absolute header/body deadline per request, with a maximum of 24 hours and zero disabling it. Set it before starting the service. Fragments do not renew the deadline; buffered replies use the remaining budget. Deferred replies retain their own independently specified deadline. This extension does not add a deadline to arbitrary direct streaming writes.

Three finer bounds, each off by default and set the same way, suit services whose requests or replies may be long. `set_idle_timeout()` bounds the wait from a connection's start (after a TLS handshake) or a reply's end to the next request's first byte. `set_header_timeout()` bounds the time from that first byte to the end of the headers, so trickled headers cannot hold a connection. `set_inactivity_timeout()` bounds each body read and each 64 KiB of a buffered reply, so a slow transfer that keeps moving completes while a stalled one ends. Deferred and owned streaming replies keep their own deadlines.

`tls_connection_factory_timed()` takes an optional last argument, a `TlsFailureHook`, called with a `TlsFailure` for every connection that ends before serving: `deadline`, `connections` or `handshakes` (its caps), and for a failed handshake `protocol` (not TLS at all, such as plain HTTP), `version`, `cipher`, `certificate` (the client refused the server's), or `other`. Connections the service's own bound refuses at accept never reach the factory; the reactor's admission counters report those.

Handlers can throw `http::RequestRejected(status, body, content)` from header/body admission to send a bounded rejection before consuming the request body. Status must be 400–599 and body at most 512 bytes; content selects fixed text or JSON framing. The transport closes after writing the rejection, with a one-second write bound. The exception derives from `std::invalid_argument` for existing admission catches.

`connection_observer()` returns read-only shared transport admission state for background metrics. The observer outlives the HTTP service safely. These transport extensions require the matching reactor headers (`a0bc26c` or newer); existing HTTP call signatures remain available.
