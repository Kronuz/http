# HTTP clients

Clients share the existing `http-parser` engine through one bounded response parser. They can run independently of the server framework. Request preparation copies borrowed fields and body bytes into an owning request before a coroutine can suspend.

| Target | Dependencies |
| --- | --- |
| `http::client_core` | http-parser, its enum-reflection dependencies, allocators::resources |
| `http::client` | Core and a compiled nonblocking POSIX driver |
| `http::client_asio` | Core and standalone Asio |
| `http::client_tls` | Asio driver, reactor's TLS configuration helpers and OpenSSL 3 or newer |

`HTTP_BUILD_SERVER` defaults to `ON`; `HTTP_BUILD_CLIENTS` defaults to `OFF`, preserving the server-only build for existing consumers. Enable `HTTP_BUILD_CLIENTS` to obtain the client targets. `HTTP_BUILD_CLIENT_ASIO` and `HTTP_BUILD_CLIENT_TLS` default to `OFF`; TLS also provides the Asio target. A client-only configuration with Asio and TLS disabled does not fetch the router, compression libraries, Asio or reactor, or discover OpenSSL. Compiled client targets respect `BUILD_SHARED_LIBS`. Resource ownership does not link allocation interception or the retirement runtime.

```sh
cmake -S . -B build-client -DHTTP_BUILD_SERVER=OFF
cmake --build build-client
ctest --test-dir build-client --output-on-failure
```

## Plain TCP

Prepare one deadline before connection acquisition and retain it across every phase and address attempt. Numeric or pre-resolved endpoints avoid uncancellable name resolution.

```cpp
#include <http_client_posix.h>
#include <array>
#include <chrono>

http::client::BufferedResponse get_snapshot() {
    using namespace http::client;
    Deadline deadline(std::chrono::seconds(2));
    PreparedRequest request({"GET", "127.0.0.1:8317", "/v1/snapshot", {}, {}, true});
    const std::array endpoints{Endpoint::numeric("127.0.0.1", 8317)};
    PosixConnection connection(endpoints, deadline);
    return exchange(connection, std::move(request), deadline);
}
```

`resolve(host, port, output_span, deadline)` is an optional synchronous OS resolver. It checks expiry before and after resolution, preserves address order and rejects a result exceeding the supplied span. `default_resolved_endpoints` provides a 64-entry convenience capacity. The OS resolver cannot be interrupted and can delay return beyond expiry; it creates no helper thread. Subsequent socket waits use the original expiry.

The POSIX driver uses nonblocking sockets and `poll`, recomputes remaining time after EINTR, suppresses SIGPIPE per socket or send, and owns each descriptor exactly once. Connection fallback does not replay a request. An address that stalls can consume the entire deadline; this is sequential fallback, without Happy Eyeballs.

## Asio and TLS

`before(deadline, operation)` races one complete operation against the captured expiry and joins the losing original operation. Cancellation requests do not release its request, buffers or stream early. An operation that needs posted cleanup must suppress automatic cancellation throws during that cleanup and preserve its terminal result.

`exchange(connected_stream, request, deadline)` buffers through the shared parser. `exchange(tls_lease, request, deadline)` includes connection and handshake under the same deadline. Large semantic streams can use `connect(lease)`, `write(stream, prepared_request)` and `ResponseParser` inside one enclosing `before()` call. These lower-level operations borrow the stream through authentic completion. `connect(lease)` is idempotent after verified authentication, including leases without a cache; `connected()` reports that authenticated state.

Prepare an `asio::ssl::context` with `reactor::tls::configure()` on the application's startup/configuration worker. `TlsLease` retains that context, requires peer verification and uses `reactor::tls::bind_hostname()` for DNS/IP verification and SNI. Runtime requests do not reopen certificate files. Contexts and their trust policy remain immutable while any lease or cache entry retains them; reload into a new context.

`make_tls_cache(executor, capacity, allocation_context)` creates a fixed-capacity cache owned by one executor. A `TlsKey` matches endpoint, hostname, exact context ownership and an opaque 32-byte binding supplied by the application. Credential, tenant or authority partitions belong in that binding. HTTP does not interpret session identifiers or replay logical operations.

Construct a `TlsLease` with the key, executor and optional cache. After complete framing and application acceptance, call `lease.reusable(response.reusable)`. The default is retirement. Starting another buffered exchange clears any earlier reuse acceptance; accept its completed response explicitly again. Unauthenticated entries cannot persist, failed semantic validation retires the entry, and a valid Connection-close response succeeds while retiring it. Fully leased caches return pressure immediately and create no waiter list. Idle sockets retain their admission until eviction or cache destruction. Leases keep their cache alive and must settle before its executor disappears.

Reuse also requires no observable pending input. TLS checks decrypted bytes, internally buffered records and socket readiness when a lease returns and when another lease checks it out. Pending or uncertain input retires the entry; legitimate TLS control traffic can conservatively prevent reuse. These checks observe current input and cannot predict later peer traffic. The drivers do not pipeline requests. A custom Asio stream without readable-state inspection returns a nonreusable response.

## Limits and streaming

| Default | Bound |
| --- | --- |
| Raw status and headers | 64 KiB per response, 128 fields |
| Body | 64 MiB |
| Total wire bytes | 128 MiB, including chunk overhead and informational responses |
| Informational responses | 8 |
| Read and owned parser staging | 4 KiB |

Pass `Limits` for application-specific ceilings. Request bounds include generated Host, Content-Length and Connection fields. The serializer rejects request/header injection and owns fixed-length framing. Unsupported CONNECT transitions are rejected.

`ResponseParser::feed(input, budget)` advances only consumed input. When `head_ready()` is true, validate `head()` and call `accept_head()` before exposing a body. `body()` borrows one owned staging block; process it and call `consume_body()`. Feed makes no progress while the head awaits admission or a body remains unconsumed. Input storage may disappear after feed consumes it; body bytes have already been copied into parser-owned storage. A zero budget performs no work.

The parser rejects malformed syntax, obsolete folding, conflicting lengths, TE+CL, unsupported transfer codings, upgrades, trailers and excess response bytes. Header names use ASCII case folding; values retain trailing whitespace for application validation. HEAD/204/304 remain bodyless regardless of representation length. Fixed and chunked framing can persist. Explicitly enabled EOF framing always retires the connection. Repeated equal Content-Length fields require an explicit policy opt-in; parser defaults remain strict.

`BufferedReceiver` drains the same parser and returns an owning `BufferedResponse`. It does not create a second framing implementation. Content decoding remains in the codec/application adapter; encoded and decoded limits are independent.

After failure or cancellation, `discard(byte_budget, token_budget)` releases parser accumulation incrementally. Repeated calls preserve unfinished work debt. Stop network intake and retain the original operation's owners until its completion before disposing them. Bounded cleanup and uncancellable OS work can extend return latency beyond expiry.

## Allocation and errors

`allocators::AllocationContext` retains the supplied PMR resource through requests, buffered responses, parser accumulation, cache entries and allocated stream controls. Empty contexts use process-lifetime `new_delete_resource()`. Leases and surviving weak shared controls retain the required allocation owner. OpenSSL native state and Asio's internal operation/coroutine allocations use their SDK allocators; the context is not a total-process RSS limit. Bound connection concurrency and measure those costs separately.

Well-formed HTTP status codes are returned normally. `Error::kind()` distinguishes invalid options, framing, limits, pressure, resolution, connection, transport, deadline, cancellation and trust failure; `cause()` retains a system error where available. Application schema and session errors stay outside HTTP.

The parser's opt-in repeated-length policy extends its public structure. Rebuild C++ consumers together when adopting that revision. Existing `http::http` server APIs and server defaults remain available.
