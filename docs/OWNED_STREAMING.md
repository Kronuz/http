# Owned response streaming

`ResponseWriter::stream()` adds an optional response capability for asynchronous producers. Existing synchronous writers default to unsupported, and existing buffered `send()`, `write()` and deferred `defer()` behavior remains available. The Asio writer supports the capability for both plain TCP and its generic TLS stream adapter. Retain the returned `OwnedStreamCapability`, rather than a scoped writer or request.

## Admission and framing

Configure a finite absolute deadline, a maximum payload length and a maximum individual chunk. Defaults bound payloads to 256 MiB and chunks to 65,536 bytes. `start(metadata, length, keep_alive)` emits a validated header with an exact Content-Length. `start_chunked(metadata, keep_alive)` emits Transfer-Encoding: chunked and retains the same finite payload and deadline bounds; it supports producers whose final encoded length is unavailable before streaming. The request's keep-alive policy remains authoritative.

Metadata must have an empty buffered body. Header validation is shared with deferred replies: invalid field names, control characters and application overrides of transport framing are rejected. Unknown-length streams cannot use bodyless 204 or 304 statuses. An exact-length stream can finish only after accepting its declared payload length. A chunked stream also writes and reaps its terminating chunk before reporting Written.

## Ownership and backpressure

`OwnedChunk` pairs an immutable byte view with its independent shared owner. The producer must not mutate those bytes. The transport does not copy the application payload. There is one chunk slot; publication returns Busy while an accepted chunk or original socket write occupies it. `on_writable` wakes the producer after the original operation completes. Busy is a backpressure result, and the producer retains its candidate chunk for another turn.

```text
Producer -> Owned chunk slot -> Original socket write
                                  |
                         Authentic completion
                                  |
                         Next producer turn
```

The transport retains the original chunk, header and exchange until socket IO completes, including after cancellation. `close()` changes intent and the first terminal reason; it cannot settle an outstanding write. Stale or duplicate write-completion serials cannot release a newer original. Settlement runs the application hook once while request retention is still alive, then releases that retention. Hooks execute outside the exchange lock. Destroying a capability before `finish()` abandons its response; destroying it after an accepted finish preserves the pending final writes.

Peer read closure is observed separately from explicit cancellation. A fully admitted exact-length response can finish its outstanding final write and producer finish; a chunked response must complete its terminal chunk. Authentic successful completion permits Written even if peer closure was observed first. Partial responses and failed writes remain Disconnected, and timeout, abandonment and shutdown retain their cancellation precedence. A producer that never finishes remains bounded by its deadline.

Asio joins the write/deadline race before releasing payload ownership. Shutdown uses reactor's session barrier, preserving original socket operations until their completion callbacks and coroutine cleanup have run. Escaped producer capabilities retain protocol controls, while executor wakeups use weak ownership and disable before transport teardown.

## Allocation boundary and footprint

An optional shared PMR resource admits both shared controls, the escaped capability and serialized header storage. Shared allocators and unique deleters retain the originating resource through their complete physical lifetime, including surviving weak controls. Header-allocation failure leaves a retryable Waiting exchange and exposes no partial header. Independent application chunk owners remain responsible for their backing allocations; Detent supplies physically charged immutable buffers.

When no explicit resource is supplied, streaming captures the current process default PMR resource at allocation time and uses that same resource for destruction, even if the process default changes. This implicit resource is borrowed and must outlive its allocations. An explicit shared resource retains ownership through the canonical allocator and unique deleter from `allocators::resources`.

The macOS fixture measures 760 bytes of PMR requests for the two shared controls and 24 bytes for the escaped capability, before headers. The writer grows from 128 to 144 bytes compared with the preceding source using the same compiler: one additional shared pointer, including on the buffered path. Header capacity is charged separately. These requests exclude the resource object itself, caller-owned callback captures, application backing buffers, timers, coroutine frames and Asio/reactor scheduling or native allocation overhead; they are not a process RSS measurement. There is no throughput claim.

The interface is additive for source consumers. Its new virtual method and writer storage require recompiling previously built consumers; these header-only changes do not provide binary ABI compatibility.

## Qualification

Transport-independent tests cover original retention, early-settlement refusal, serial filtering, cancellation, exact-length completion, terminal chunk settlement, malformed headers and limits. Real TCP tests transfer two MiB across 32 chunks in both exact-length and chunked modes, then reuse the connection for a buffered response. Slow readers hold an eight-MiB original across deadline expiry and shutdown; payload retirement follows authentic completion. PMR tests cover control and header admission refusal, retained weak controls, escaped headers and final reclamation.

All seven native HTTP suites pass. Four focused stream/deferred suites pass under ASan/UBSan and fully instrumented TSan; all seven suites also pass on Linux and FreeBSD. Detailed timings and final integration qualification belong to the consuming component's evidence record. The generic TLS adapter compiles through Detent's service; end-to-end TLS distribution qualification remains an integration gate.

The seven-suite runs complete in 4.15 seconds natively, 3.45 seconds on Linux and 3.30 seconds on FreeBSD. The four focused stream/deferred suites complete in 2.22 seconds under ASan/UBSan and 3.11 seconds under fully instrumented TSan. Detent's service and HTTPS checks, including their TLS certificate fixture, pass in 9.14 seconds. Shared-host test durations are qualification evidence, not throughput measurements.
