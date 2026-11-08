> History. Moved here on 2026-11-08 from the unversioned `~/code/Kronuz/HANDOFF-http-server-lib.md` (2026-07-01): the handoff of the work that made Kronuz/http a server library and planned Xapiand's search as one `http::HttpHandler`. That plan has since landed (Phase 7c L3, X3 and W2a); current state is in `ARCHITECTURE.md`, `docs/mechanisms/coroutine-handlers.md` and `CHANGELOG.md`. Paths name the author's machine at the time. The text below is unchanged.

# Handoff — Inverting Xapiand onto Kronuz/http (Leg 2, architecture B)

A resume-ready summary of where the "make Kronuz/http a real HTTP server library
and collapse Xapiand's search onto it" work stands, what's next, and the gotchas.
The authoritative running notes are in project memory (see "Memory" below); this
is the curated entry point.

## The goal (architecture B)

Grow **Kronuz/http** into a genuine HTTP server library by adding generic,
app-pluggable **knobs**, then make Xapiand's search engine **one
`http::HttpHandler`** that plugs in its formats and routes to its views. The
library never learns MsgPack or Xapian; the app registers serializers, decodes
its own body, owns routing/exceptions. (We considered a Xapiand-rich seam +
thin adapter — "option A" — but pivoted to B: nothing Xapiand's HTTP layer does
is out of the box of HTTP; the lib just needs the right knobs.)

## Done (committed + validated)

**Xapiand stage-1 decoupling** (branch `extract-and-modernize`, local commits):
- `serialize_response` and `http_response` lifted off `HttpClient` → file-scope
  free functions (they're Xapiand's content-neg + response formatting, i.e. its
  future registered serializers). `ef433cfb0`, `707bfe4cc`.
- `endpoints` moved from an `HttpClient` member onto `Request` (per-request state;
  also fixed a latent pipelining bug). `fa6dc988a`.
- Each verified behavior-identical via the E2E net.

**Kronuz/http knobs** (pushed to github.com/Kronuz/http):
- **Content negotiation** — `http_accept.h` (`91b70f0`): `http::Accept` parses
  Accept-family headers (RFC 7231), `best(available)` / `quality()`,
  most-specific match, q-values, wildcards, token-only (Accept-Encoding),
  406. `negotiate(request, available)` helper. Generic.
- **Body access + streaming** — (`2b48fdd`): `Request::content_type()`;
  `HttpHandler::on_request_body(Request&) -> unique_ptr<BodySink>` (nullptr =
  buffer, default; non-null = stream chunks for NDJSON/RESTORE); connection
  finalizes the request at headers-complete, routes body to sink-or-buffer,
  reserves the buffer from Content-Length; `Request::user_data` carries per-request
  app state from `on_request_body` to `handle`. `kv_store` POST /bulk demo.
- **Dispatcher** — `http_dispatcher.h` (`4744b15`): bounded worker pool on
  Kronuz/queue; `submit()` returns false when full = the **503 backpressure**
  signal; `stop()` drains+joins. The off-reactor-work foundation. 506 self-checks.
- **Connection-offload wiring (#50) — DONE + PUSHED** (`8f20977`, on
  github.com/Kronuz/http main). HttpService owns a per-reactor `Dispatcher`
  (`create(handler, workers, queue_limit)`); HttpConnection offloads `handle()`
  **one-in-flight-per-connection** (parser paused + `io_read` stopped during
  offload; pipelined remainder stashed in `pending_input_` and re-fed on
  completion); worker drives the thread-safe write path and hands completion back
  via an `ev::async` (`complete_async_`) the reactor runs (complete_response +
  resume reading); `submit()` false → 503 inline; `share_this` keeps the conn
  alive. Also fixed: `HttpService` was missing the `Worker::deinit()` its dtor
  owes (latent, asserts-off in Release). Validated: `test/loadtest.cc` (~8 ms fast
  latency under offload vs ~450 ms inline; 503 backpressure) + `examples/
  dispatcher_bench.cc` (single per-reactor queue is fine for ms-scale handlers;
  sharding the queue would not help — the cost at high worker counts is condvar
  wakeup, not lock contention). TSAN + ASAN/UBSAN clean (Homebrew LLVM; only libev's
  async primitive shows, suppressed in `tsan.supp`).
- **Offload follow-ups — DONE + PUSHED.** Per-route classification (`3de9c2f`):
  `HttpHandler::should_offload(request)` (default true) keeps a cheap/non-blocking
  route on the inline fast path even when a Dispatcher exists — loadtest phase D
  shows a cheap route served in ~3 ms while both workers are saturated. Stall
  watchdog (`5099415`, `http_watchdog.h`): a periodic ev::timer pets a timestamp +
  a monitor thread fires `on_stall` on the healthy→stalled edge (the timer forces
  idle wakeups so stale = genuinely stuck, not I/O-wait); `HttpService::watch_stalls`
  arms it, stopped in `stop()` before the loop quiesces — loadtest phases E/F: an
  inline block trips it (1 fire), the same work offloaded keeps the loop healthy
  (0 fires). Both TSAN + ASAN/UBSAN clean.
- Earlier: the streaming `ResponseWriter` (Content-Length vs chunked) and the
  radix `Router` (Kronuz/radix-router) already shipped.

## Done-of-the-immediate-task; NEXT is the remaining knobs → then collapse Xapiand

(The connection-offload cut above is complete. Original #50 design notes kept below
for reference / the deferred purist variant.)

### (reference) connection-offload wiring (#50) — the design that was built
- **Per-reactor `Dispatcher`** (one per loop, shared-nothing across cores — see
  the concurrency principle below), not a single global pool. `HttpConnection`
  offloads `handle()` to its reactor's dispatcher, **one in-flight handler per
  connection**: pause the read io during offload so the reactor doesn't touch
  `request_` while the worker has it (no race), and responses stay ordered without
  pipelining machinery. The pool parallelizes *across* connections — the
  throughput win. (Purist shared-nothing variant: *move* the request into the
  task and order responses so the connection keeps reading; more machinery, defer.)
- `submit()` false → `writer.send(503)` inline (on the reactor).
- Completion via an `ev::async` on the connection: the worker signals it; the
  **reactor** runs `complete_response` + resumes reading (that step touches
  ev/close, so it must run on the loop, never the worker). Task captures
  `share_this<HttpConnection>` to keep the connection alive.
- Then a **stall watchdog**: a monitor thread that flags any loop that hasn't
  ticked in N ms.
- `BaseClient::write` is ALREADY thread-safe (enqueues to the per-connection
  `write_queue` + wakes the reactor via `write_start_async.send()`), so a worker
  can drive a response without touching the loop. This is the load-bearing fact.

**Validate it with a load test, not just the E2E suite** — a deliberately-slow
handler must not stall the loop (other connections keep being served). Races here
will NOT reliably show up in the doc E2E suite. Then re-run the E2E net for parity.

### Concurrency principle: avoid sharing data among THREADS (decision)
The guiding rule: the best concurrency avoids sharing data among threads —
shared-nothing, message-passing, no cross-thread locks. Apply it:
- `BaseClient::write`'s `write_queue` is **per-connection** and is really a
  *message channel* (the worker pushes bytes; the reactor drains + flushes). Keep
  `queue::Queue` — it isn't shared across threads beyond that one connection's
  worker→reactor handoff, and each connection has its own. Not a bottleneck.
- A single GLOBAL `Dispatcher` pool **violates the principle** (one task queue
  shared by every reactor thread and every worker). Prefer a **per-reactor
  (sharded) Dispatcher**: each loop owns its workers + queue — shared-nothing
  across cores (Seastar shard-per-core flavor). Benchmark global-vs-sharded, but
  lean sharded.
- **Move data to the worker; message the result back.** The worker should own a
  *moved* request, produce output, and hand it back via the per-connection
  `write_queue` + an `ev::async` signal — it must NOT directly mutate shared
  connection state (the reactor owns the connection lifecycle). The thread-safe
  `write_queue` + `ev::async` ARE the message channels; that is the offload done
  shared-nothing, and it keeps the worker from racing the reactor.
- The tension to respect: you can't both "not stall the loop" (offload to another
  thread) and "share nothing" (one thread) for Xapian's blocking CPU-bound search.
  The resolution is the move-data/message-result handoff above — minimize the
  shared surface to the explicit channels, don't share mutable connection state.

## After that (remaining knobs → then collapse Xapiand)

- **Response compression — DONE + PUSHED** (`a8414c9`, `http_compression.h`):
  `HttpService::enable_compression()` negotiates Accept-Encoding → zstd/gzip
  (Kronuz/compressors) in the Writer at `end()` (buffered path), sets
  Content-Encoding + Vary + compressed Content-Length; conservative triggers
  (explicit Accept-Encoding only, min_size, bodyless statuses, already-encoded,
  didn't-shrink). HTTP `deflate` omitted (raw-vs-zlib ambiguity). loadtest phase G
  round-trips both codecs; TSAN/ASAN clean with compression on workers.
- **Accept-negotiation LRU cache — SKIPPED (low value here, documented).** Xapiand's
  `AcceptLRU` mattered because it parsed Accept on every request and it showed in
  profiles. In this lib the compression parse only runs when compression is enabled
  AND the body clears `min_size`, where the compression CPU dwarfs the µs parse, so
  it's never the bottleneck. The real caching target is the app-level *content-type*
  negotiation (the search app's `serialize_response`), which the app owns and can
  memoize itself; a per-connection 1-entry memo (lock-free, ~100% hit on keep-alive)
  is the clean spot if it ever profiles hot. Not worth a shared, locked global cache.
- **Conditional requests — DONE + PUSHED** (`79f0cb4`, `http_conditional.h`):
  `enable_conditional()` derives a weak ETag (`W/"<fnv1a64>"`) from a buffered
  GET/HEAD 200 body and short-circuits a matching `If-None-Match` (or `*`) to a
  bodyless 304. Last-Modified left out (needs app mtime). loadtest phase H.
- **Range requests — DONE + PUSHED** (`c8906ad`, `http_range.h`):
  `enable_ranges()` serves a single byte `Range` (`bytes=N-M / N- / -N`) as 206
  Partial Content (Content-Range) + Accept-Ranges; 416 when unsatisfiable;
  multi-range/other-unit → full 200. loadtest phase I + unit tests.
- These four response transforms (compress / conditional / range) share one
  `ResponseOptions` (held by HttpService, `const ResponseOptions*` to the
  connection); the Writer runs them at `end()` in order **conditional → range →
  compression** (a 304 short-circuits all; a 206 skips compression). The
  un-stallable offload, classification, and watchdog are also all done. **So the
  generic HTTP-server-library knob set is essentially complete** — what remains is
  **#51** (collapse Xapiand's search onto one `http::HttpHandler`), the big leg.
- Benchmark each knob (perf lane) like the router was (4×–440×).

## #51 — the concrete cut (plan, ready to execute in a dedicated session)

The library side is done; #51 is all Xapiand. Full coupling map of
`src/server/http_client.cc` (4113 ln) done 2026-06-30 — verdicts + line anchors below.

**STAGE 2 DONE 2026-06-30 (all request-only helper lifts, E2E-validated, local commits).**
The nets turned out to be WARM (both binaries are at `build/bin/xapiand`, not
`build/xapiand`); after killing a leftover node squatting on :8880, the baseline
calibration was GREEN and the two stage-2 increments landed green:
- `0aff727c8` — **stage 2a**: the 6 content-neg helpers off `HttpClient` → file-scope.
- `42b227334` — **stage 2b/2c**: `url_resolve` / `query_field_maker` /
  `encoding_http_response` off `HttpClient` → file-scope.
Both: e2e_diff GREEN (0 regressions, 0 status diffs / 303 req, 73 pre-existing) +
bodydiff identical to the unchanged build (same 3 volatile divergences). So every
request-only helper is now off `HttpClient`. The validate-each-stage loop is now
scripted — `cmake --build build -j` then `harness/e2e_check.sh` (see "Validation
nets" above for the binary paths, the saved baseline report, and the gotchas).

**STAGE 3a + 3b DONE 2026-07-01 (E2E-validated, local commits).** The two safe
decoupling lifts that finish taking every request/response helper off `HttpClient`:
- `d9b691426` — **stage 3a**: the DB-bound prep (`node_obj` / `retrieve_database` /
  `expand_paths` / `resolve_index_endpoints`) → file-scope statics. They touch only
  `request` + the manager/DB singletons, never connection state — the search-side seam
  `SearchApplication` will own.
- `5fc0caa63` — **stage 3b**: the response/transport helpers (`write_status_response` /
  `write_http_response` / `readable_encoding`) → file-scope statics. The response is
  formatted from `request` state (all the content-neg / serialize / encode helpers were
  already free) and the raw bytes reach the wire through the **single transport seam
  `request.client->write()`** — the one remaining connection coupling, which the swap
  onto `http::HttpConnection` replaces. Both green (0 regressions/status diffs / 303 req,
  bodydiff = the 3 volatile fields).

**So HttpClient is now reduced to the pure async heart:** `on_read` + the parser
callbacks (`on_*`), `prepare()` (the method+URL-predicate dispatch that sets
`request.view`), the runner (`operator()` + `process()` + the `requests` deque +
`runner_mutex` + the `http_client_pool` enqueue), the blocking-body model
(`request.wait()`/`pending` CV, `objects` deque, `next_object`), `end_http_request`
(`waiting`/lifecycle), and the per-request `http_parser`/`view`/`client` back-pointer on
`Request`. Everything else is a free function taking `Request&`. **NEXT = STAGE 3c, the
real architectural cut.**

**Coupling map (what can move off `HttpClient` vs what is the async core):**
- *Request-only → liftable to file-scope (the proven stage-1 pattern; call sites
  resolve automatically since they already pass `request` with no `this->`):* the
  content-negotiation cluster `resolve_ct_type`×2 (3241/3256), `is_acceptable_type`×2
  (3286/3309, pure), `get_acceptable_type<T>` (3324), `resolve_encoding` (3462); plus
  `url_resolve` (2713), `query_field_maker` (2895), `encoding_http_response` (3515).
  The `AcceptLRU`/`AcceptEncodingLRU` caches are ALREADY file-scope statics (728/763).
- *Need the manager/DB singletons → stay search-side (move behind the application,
  not the HTTP layer):* `expand_paths` (2832), `resolve_index_endpoints` (2805),
  `node_obj` (1338), `retrieve_database` (1872).
- *Transport (call `this->write`) → become the writer/transport seam:*
  `write_status_response` (2697) + `write_http_response` (3395) — split the already-
  free formatting (`serialize_response` 3350, `http_response` 251) from the socket
  write. `end_http_request` (3161) touches the `waiting` flag (HttpClient state).
- *The async heart → maps onto the lib, not extractable in place:* `prepare()`
  (976–1248) is a `switch(method)` + URL-structure predicates (`id.empty()`,
  `is_range(id)`, `has_pth`, `cmd==":metrics"`) setting `request.view` (member-fn-ptr)
  — NOT radix-clean, so it stays a custom dispatch in the handler. The runner
  (`operator()` 1267, the `requests` deque + `runner_mutex`) and the blocking-body
  model (`request.wait()` on the `pending` CV at 1285; NDJSON via `request.next_object`)
  are exactly what Kronuz/http's **Dispatcher** + **BodySink** replace. `catch_http_errors`
  is already a free function in `http_utils.h` (reuse at the app boundary).

**Staged sequence (each ends E2E-green vs 7bd295b; batch lifts per build to amortize LTO):**
- *Stage 2 — DONE (`0aff727c8`, `42b227334`).* All request-only helpers lifted off
  `HttpClient` to file-scope: 2a the content-neg cluster, 2b `url_resolve` +
  `query_field_maker`, 2c `encoding_http_response` (forward-declared near the top
  since they're used above their definitions; `HttpClient::x` → `static`; `.h` decls
  dropped). Call sites unchanged (they already passed `request` with no `this->`).
  Both increments E2E-green + bodydiff-identical to the unchanged build.
- *Stage 3a — DONE (`d9b691426`).* The DB-bound prep helpers off `HttpClient` →
  file-scope statics (`node_obj`/`retrieve_database`/`expand_paths`/`resolve_index_endpoints`).
- *Stage 3b — DONE (`5fc0caa63`).* The response/transport helpers off `HttpClient` →
  file-scope statics (`write_status_response`/`write_http_response`/`readable_encoding`),
  transport routed through the `request.client->write()` back-pointer. Now every
  request/response helper is free and `HttpClient` is the pure async heart.
- *Stage 3c (the real cut): define `SearchApplication` and swap the transport.* Under
  architecture B the search becomes one `http::HttpHandler`: `handle(const http::Request&,
  http::ResponseWriter&)` does what `prepare()` + the chosen `*_view` do; the DB-coupled
  helpers move behind it; content-neg uses `http::Accept` + the app's registered
  serializers (the lifted `serialize_response`/`resolve_ct_type`); compression/conditional/
  range come free from the lib knobs. The request-body model (`pending` CV / `objects`
  deque) becomes a `BodySink`; the runner+deque becomes the per-reactor `Dispatcher`.
  Xapiand's `HttpServer` then creates `http::HttpConnection` instead of `HttpClient`, and
  `HttpClient`'s bespoke parser/framing/runner is dropped.

  **The load-bearing 3c design decision (decided; record why): rewrite the response path
  onto `http::ResponseWriter`, do NOT bolt a raw-bytes escape hatch onto the lib.** Today
  Xapiand formats complete raw HTTP/1.1 bytes itself (`http_response()` builds the status
  line + headers + framing; `encoding_http_response()` does manual gzip/deflate; then
  `request.client->write(bytes)`). The lib's `ResponseWriter` instead takes
  `status()`/`set_header()`/`write()`/`end()` and owns the framing (Content-Length vs
  chunked) in the connection — that clean seam is the whole point (AGENTS.md: "Framing
  lives in the connection's Writer"). So `write_http_response` gets rewritten to:
  serialize obj → bytes (keep `serialize_response`), then `writer.status(s);
  writer.set_header("Content-Type", ct); writer.write(bytes); writer.end();`. DUMP's
  chunked streaming becomes repeated `writer.write()` calls (the lib emits chunked on the
  2nd write). **Parity caveat to verify:** Xapiand's manual `Encoding` (gzip/**deflate**/
  identity via Accept-Encoding + `type_encoding`/`resolve_encoding`/`encoding_http_response`)
  is REPLACED by the lib's `enable_compression()` knob, which does **zstd/gzip only (no
  deflate)**. The doc E2E suite uses default Accept-Encoding so it should stay green, but a
  client asking for `deflate` would now get identity or gzip — an intentional, documented
  behavior change (deflate is the ambiguous-in-the-wild coding the lib deliberately drops).
  This is the piece that makes 3c a rewrite, not a lift, and why it deserves its own focus.
- *Stage 3c prerequisite — the parser de-vendor (a real blocker, found 2026-07-01):* wiring
  Kronuz/http into Xapiand's CMake is NOT conflict-free. Xapiand vendors its own parser
  (`src/http_parser.c` 70KB + `src/http_parser.h`, and `http_client.h` `#include
  "http_parser.h"` resolves to it), while Kronuz/http `FetchContent`s `Kronuz/http-parser`
  and links `http_parser::http_parser`. Pulling the lib in as-is → two `http_parser.h` on the
  include path + two `http_parser.c` TUs in the link (ODR/symbol clash). So the first 3c
  sub-step is a parser de-vendor: drop Xapiand's `src/http_parser.{c,h}`, point everything at
  Kronuz/http-parser (same Joyent fork; pairs with the planned 2.7.1 → 2.9.4 refresh). Build-
  risky; wants a sign-off alongside the deflate decision.
- *Stage 3c-1 — DONE (`deb364b26`).* Parser de-vendor: removed `src/http_parser.{c,h,enums.h}`,
  consumed Kronuz/http-parser via FetchContent (byte-identical, verified; `enum.h` already on
  the path via enum-reflection, `likely.h` via the other deps). Clears the conflict blocker.
- *Stage 3c-2 — DONE (`1ab16be91`).* Wired Kronuz/http onto the include path and added
  `src/server/search_application.{h,cc}` (`SearchApplication : http::HttpHandler`, placeholder
  handle()). The seam headers are stdlib-only so it compiles now; not yet wired into HttpServer
  (behavior unchanged). Proves the lib's seam integrates in Xapiand's build.
- *Stage 3c-3 — DONE (`8b6f3eb4f`).* Lifted the ~23 `*_view` endpoints off HttpClient to
  file-scope free functions; `view_function` is now a plain `void(*)(Request&)`, prepare()
  takes `&view` addresses, process() calls `request.view(request)`. **So the whole application
  layer (dispatch + views + response formatting) is off HttpClient**; only the parser
  callbacks, the runner, the blocking-body model, and the single `request.client->write()`
  transport seam remain.
- *Stage 3c-swap (REMAINING — the atomic, parity-risky rewrite; don't start at a checkpoint):*
  (1) rewrite `write_http_response`/`write_status_response` + the 4 raw-write view sites onto an
  `http::ResponseWriter*` carried on Request (drops `http_response()`/`encoding_http_response`/
  `readable_encoding` for the lib's framing + `enable_compression()` — the deflate→zstd/gzip
  change); (2) build the `http::Request` → Xapiand `Request` bridge in `SearchApplication::handle()`;
  (3) wire Kronuz/queue + Kronuz/compressors into the CMake (needed by the connection/compression
  headers); (4) swap `HttpServer::io_accept_cb` (`http_server.cc:133`) to make
  `http::HttpConnection(searchApp, dispatcher, response_opts)` not `HttpClient`, runner pool →
  per-reactor Dispatcher, body model → BodySink (buffer first). **Parity crux:** the lib's
  framing emits different header bytes than `http_response()` (Server/ordering/Connection), so
  the swap must reconcile those against the E2E assertions to stay green — an iterative effort.
- *Stage 4:* map Xapian/ClientError → HTTP status at the app boundary (reuse
  `catch_http_errors`). *Stage 5:* DUMP/RESTORE/bulk request-body streaming via
  `BodySink` (the cleanest form is the coroutine era, but `BodySink` covers it now).
- Then the perf lane (loadtest.py + perfdiff.py) for parity, and the Asio+coroutine
  reactor swap is a separate, later leg the value-semantic seam keeps cheap.

## Validation nets — WARM and SCRIPTED (read this before touching Xapiand)

The nets are ready; do not rebuild them from scratch. "Green" = parity with the
baseline, NOT 100% pass (the doc suite has 73 pre-existing aspirational failures
that fail on both).

**Where everything is (all on disk, durable across sessions):**
- Our node binary: **`~/Development/Kronuz/Xapiand/build/bin/xapiand`** — note it is
  `build/bin/xapiand`, NOT `build/xapiand` (looking at the wrong path is what made me
  wrongly think the nets were "cold"). `build/` is configured → incremental
  `cmake --build build -j` after a one-file edit is a relit binary in ≈ 5 min, and
  single-TU `-fsyntax-only` via `build/compile_commands.json` is the seconds-fast
  inner loop.
- Baseline node binary (the oracle): **`~/Development/Kronuz/xapiand-master-bench/
  build/bin/xapiand`** at **`7bd295b0b`** (earliest commit that builds on Apple
  Silicon; pristine `origin/master` does not). Already built; leave it.
- Saved baseline newman report: **`harness/results/e2e_base_7bd295b.json`**
  (~61 MB, gitignored). Lets you diff without re-running the baseline node.
- `newman` 6.2.2 + `node` via volta are on PATH.

**The loop (scripted — `065078719`):**
```sh
cd ~/Development/Kronuz/Xapiand
cmake --build build -j            # incremental, after your edit (~5 min)
harness/e2e_check.sh              # boots OUR node, newman, e2e_diff + bodydiff vs the saved baseline
```
`e2e_check.sh` wraps `e2e_capture.sh` (boot a fresh `--solo` node on :8880, fixed
`--name benchnode`, run the doc collection) + `e2e_diff.py OURS BASE` +
`bodydiff.py BASE OURS` (the two diff arg orders DIFFER). To re-capture the baseline
report (only if the saved one is lost): `harness/e2e_capture.sh
../xapiand-master-bench/build/bin/xapiand harness/results/e2e_base_7bd295b.json`.

**Gotchas the scripts already handle (but know them):**
- `pkill -9 -f bin/xapiand` before every run. A leftover node squatting on :8880
  makes our fresh node fail to bind (EADDRINUSE → exit) and newman then silently
  hits the STALE node — looks like a regression, isn't. The scripts verify OUR node
  bound (`kill -0 $pid` before trusting the readiness curl).
- `newman` exits 1 because of the 73 pre-existing failures — NORMAL; judge by the
  diffs, not the exit code. Use `--timeout-request 30000` (RESTORE /bank/ is heavy).
- **Green-state bodydiff = exactly 3 volatile divergences** (`POST /twitter/user/`
  base62 auto-id, `GET /` node runtime, `GET /:metrics` counters). More than those
  three → inspect (a real serialization change). Stage 2 was verified green this way.

Performance lane (unchanged, one node up at a time on :8880):
```sh
python3 harness/loadtest.py --datadir <dir> --label X --out harness/results/X.json
python3 harness/perfdiff.py harness/results/baseline.json harness/results/ours.json
```
Status: stage-2 tip is **green vs baseline** (0 regressions, bodydiff = the 3
volatile fields). The rapidjson relaxed-JSON regression the suite once caught is
fixed (`ed79dd72f` — restored `RAPIDJSON_PARSE_DEFAULT_FLAGS` as a compile def).

## Gotchas / facts

- Xapiand LTO build ≈ 15 min; reconfigure with `-DFETCHCONTENT_FULLY_DISCONNECTED=ON`
  to avoid re-fetch hiccups. Apple clang (`/usr/bin/c++`), default build type
  already implies `-O3 -flto`.
- Commit identity (all Kronuz repos + Xapiand): `Germán Méndez Bravo
  <german.mb@gmail.com>`. gh account is **Kronuz** now — push directly
  (`git -c credential.helper='!gh auth git-credential' push`). Network needs
  `dangerouslyDisableSandbox: true`. Xapiand commits stay LOCAL; Kronuz libs push.
- Parser is **Kronuz/http-parser** (the Joyent fork), NOT llhttp — Xapiand's custom
  verbs (COUNT/INFO/DUMP/RESTORE/…) need a non-validating parser. http_parser has
  no `on_header_value_complete` → pair headers via the field→value→field transition.
- A planned separate commit: refresh the http-parser fork 2.7.1 → 2.9.4.
- Toolchain: Homebrew LLVM 22 for sanitizers (Apple clang 17 hangs on macOS 26).

## Memory (project notes, authoritative)

- `kronuz-extraction-backlog` — the full ledger incl. the B pivot + per-step detail.
- `kronuz-unstallable-event-loop` — the concurrency model design note.
- `xapiand-docs-e2e-suite` — the E2E suite + baseline method + the rapidjson find.

## Resume prompt (paste to start fresh)

> Resume the Xapiand → Kronuz/http inversion (Leg 2, architecture B: grow
> Kronuz/http into a real HTTP server library; Xapiand search becomes one
> `http::HttpHandler`). Read `~/Development/Kronuz/HANDOFF-http-server-lib.md` and
> the project memory (`kronuz-extraction-backlog`, `kronuz-unstallable-event-loop`,
> `xapiand-docs-e2e-suite`). The connection-offload dispatch (#50, the un-stallable
> model) is DONE and pushed (`8f20977`), with its two follow-ups — per-route
> classification (`3de9c2f`) and the stall watchdog (`5099415`) — plus **response
> compression** (`a8414c9`, `enable_compression()` → zstd/gzip). The Accept-LRU cache
> was intentionally skipped (low value here; see the handoff). **Conditional
> (ETag/304, `79f0cb4`) and range (206, `c8906ad`) requests are now DONE too**, so the
> generic knob set is essentially complete. Big leg in progress: **#51** — collapse
> Xapiand's search onto one `http::HttpHandler`. Fully scoped (the handoff has the
> `http_client.cc` coupling map + staged plan) and **STAGES 2, 3a and 3b are DONE +
> E2E-validated** (`0aff727c8`/`42b227334` content-neg + url/query/encoding; `d9b691426`
> DB-bound prep; `5fc0caa63` write_status_response/write_http_response/readable_encoding).
> Every request/response helper is now a free function taking `Request&`; **HttpClient is
> reduced to the pure async heart** (parser callbacks + `prepare()` dispatch + the runner +
> the blocking-body model + lifecycle), and the single remaining transport coupling is the
> `request.client->write()` back-pointer. The nets are WARM (binaries at `build/bin/xapiand`;
> baseline `xapiand-master-bench/build/bin/xapiand` @7bd295b; saved report
> `harness/results/e2e_base_7bd295b.json`) and the loop is scripted (`cmake --build build -j`
> then `harness/e2e_check.sh`; green = 0 regressions + bodydiff = the 3 volatile fields).
> **#51 IS FUNCTIONALLY DONE — the transport swap landed E2E-green (`d324a4fa8`).** Xapiand's
> search is now served through `http::HttpConnection` + a stateless `SearchApplication`
> http::HttpHandler + a per-process `http::Dispatcher` (the un-stallable pool), i.e. the whole
> HTTP stack is the generic Kronuz/http library. Stages (all Xapiand-local, E2E-green
> = 0 regressions/status diffs / 303 req, bodydiff = 3 volatile fields): `d9b691426` 3a DB helpers,
> `5fc0caa63` 3b response helpers, `deb364b26` 3c-1 parser de-vendor, `1ab16be91` 3c-2 wire
> Kronuz/http + scaffold, `8b6f3eb4f` 3c-3 lift views, `51239e289` 3c-4 dual-route response,
> `d324a4fa8` **3c-5 THE FLIP** (HttpServer creates http::HttpConnection; handle() builds a Xapiand
> Request from the http::Request), `88f9664d1` **3c cleanup** (the dead HttpClient + http_client_pool
> removed), and `3349e1fc9` **3c-6 the declarative route table**.
>
> **THIS SESSION (router + fully-flagged server):**
> - **Offload race fixed in the lib** (Kronuz/http `eda1d9d`, TSAN-clean, pushed): `run_offloaded`'s
>   safety-net end() could double-fire `complete_async_` after handle()'s end() while the reactor
>   reset the writer for the next pipelined request → dropped the next response body. Fix defers the
>   completion wake to run_offloaded's tail after the writer is released.
> - **Bounded-backtracking radix router** (Kronuz/radix-router `a3ad26f`, pushed): replaced the
>   static-XOR-wildcard exclusion with priority-ordered matching + bounded backtracking, so static,
>   param and catch-all coexist at one position (`/users/new` beside `/users/:id`; non-final
>   catch-all `/*ns/:id`, `/files/*path/raw`). Wildcard-free lookup stays an O(path) descent (no
>   backtracking); bench flat vs route count. 42-check test suite; Node separates `path` (static
>   edge) from `name` (capture); `Params::truncate()` undoes a failed branch.
> - **Generic `http::MethodRouter<T>`** (Kronuz/http `c8f1653`, pushed): one radix tree per method +
>   the 404/405 distinction; `http::Router` is a thin binding over it. Xapiand uses
>   `MethodRouter<view_function>` directly (its handlers are `void(*)(Request&)` and it derives its
>   own key from the richer URL grammar).
> - **Xapiand dispatch → declarative route table** (`3349e1fc9`): `dispatch_request()`'s ~200-line
>   method+URL switch is now a `route_key()` classifier (kinds `/db /doc /meta /metrics /search`) +
>   a table registered on `MethodRouter`. PathParser still tokenizes the `:command`/`.selector`
>   sub-syntax and applies the method override; trivial verbs (OPTIONS, OPEN/CLOSE, unknown) stay
>   inline; FLUSH/QUIT became views with their admin/root guards; delete_database keeps its has_pth
>   guard; cmd-ignoring verbs register their no-id view under `/meta` too → exact parity. E2E-green.
> - **SO_REUSEPORT as a first-class server option** (Kronuz/http `f9e35e9`, pushed): `http::BindOptions`
>   (reuse_port / tcp_nodelay / defer_accept / tries) + `HttpService::set_bind_options`; the flags are
>   threaded into the bind instead of hard-coded to 0. loadtest section J: two reactors bind one port
>   with reuse_port=true, all requests served. Also fixed `e2e_capture.sh` to `exit 0` on a captured
>   report so `e2e_check.sh` prints its verdict (was short-circuiting on the killed node's wait-status).
>
> **REMAINING (the Leg-2 finale, a dedicated E2E-guarded stage):** Xapiand still runs its OWN
> `HttpServer` (MetaBaseServer, its accept loop creating `http::HttpConnection`) rather than
> `http::HttpService`. The finale is to make Xapiand *use* `http::HttpService` end-to-end — N reactors
> via `BindOptions{reuse_port=true}` mapped from `opts.num_http_servers`, each with its own loop +
> Dispatcher (shared-nothing) — and retire Xapiand's HttpServer glue + the single shared
> `search_dispatcher()`. This touches the manager/threading/shutdown coordination, so it deserves its
> own stage. Then the perf lane (loadtest.py/perfdiff.py) + the deferred BodySink true-streaming
> refinement + the Asio+coroutine reactor swap.
> The nets are WARM (binaries at `build/bin/xapiand`; baseline `xapiand-master-bench/build/bin/
> xapiand` @7bd295b; saved report `harness/results/e2e_base_7bd295b.json`) and the loop is
> scripted (`cmake --build build -j` then `harness/e2e_check.sh`; green = 0 regressions +
> bodydiff = the 3 volatile fields). Push Kronuz libs as the Kronuz gh account; Xapiand commits
> stay local; incremental Xapiand build ≈ 5 min, full ≈ 15 min LTO.
