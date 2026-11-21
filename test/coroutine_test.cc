// coroutine_test -- Phase 7c L3: handlers are coroutines on the connection's loop.
// Each section proves one claim of docs/mechanisms/coroutine-handlers.md over real TCP:
//   [A] a suspended handler does not stall the loop, and a throw after a suspension is mapped
//   [B] BodyStream semantics (order, eof, error, bound, cancel)
//   [C] a reset cancels reads but never writes; a half-close (EOF) cancels nothing; a pipelined byte ends the watch
//   [D] the in-flight gate answers 503 + Retry-After and counts the refusals
//   [E] the buffered-body cap answers 413 (Content-Length and chunked), streams are not capped
//   [F] shutdown: cancel, the response is still written, frames outlive the cancel, bounded by the grace
//   [G] pipelined and keep-alive requests are unaffected by the per-request watch
//   [H] a body cut short (FIN, half-close, RST, chunked) fails the reader, never eof, with the request cancelled
//   [I] a throwing cancel callback is dropped: the handler's frames outlive it, streamed and buffered
//   [J] a half-closed client keeps its response (D126): a plain handler, a long poll held on its
//       token to its own bound; a reset or a failed write cancels, a failed write included after
//       the peer half-closed and then left
//   [S] the streamed join: a hang-up after the body cancels the handler; a handler that answers early stops the feed
//   [T] the task loop (design T4): (b) budget demotion, (c) affinity after a std::thread resume,
//       (d) stop() with suspended handlers leaves no task frame, (e) task::cancelled after a
//       cancellation is 503 + close, (f) no checkpoint passes after a hang-up, (g) an owned
//       stream abandoned by task::cancelled writes no terminal chunk, (h) writer affinity
//   [X] the teardown phases across the service and an application's own scheduler

#include "coroutine/app.h"

void body_stream_tests();

// T4 (h), Debug: a handler body that moved to another scheduler and then writes fails the writer's
// affinity assertion. Runs in a child forked before any thread exists; true when it held.
// With NDEBUG the misuse is performed for real, a data race by construction: TSan builds skip it.
#if defined(__SANITIZE_THREAD__)
#define COROUTINE_TEST_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COROUTINE_TEST_TSAN 1
#endif
#endif
static bool affinity_death_test(const std::string& request) {
#if defined(COROUTINE_TEST_TSAN) && defined(NDEBUG)
	(void)request;
	return true;
#else
	std::fflush(stdout);   // the child must not repeat what the parent buffered
	const pid_t child = ::fork();
	if (child == 0) {
		App app;
		asio::io_context other_io;
		auto guard = asio::make_work_guard(other_io);
		task::asio_scheduler other(other_io.get_executor());
		std::thread other_thread([&] { other_io.run(); });
		app.away = &other;
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto r = one(port, request);
		std::fflush(stdout);
		::_exit(r.status == 200 ? 0 : 3);   // NDEBUG: the assertion is compiled out and the reply arrives
	}
	int status = 0;
	::waitpid(child, &status, 0);
#ifdef NDEBUG
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
#else
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
#endif
#endif
}

int main() {
	std::printf("== coroutine handler tests ==\n");
	check(affinity_death_test(get("/affine-body", "Connection: close\r\n")), "[T] (h) a handler body that moved away and then writes fails the writer's affinity check (Debug)");
	check(affinity_death_test(get("/affine-thread", "Connection: close\r\n")), "[T] (h) a raw thread (no task, off the loop) that writes fails the writer's affinity check (Debug)");
	check(affinity_death_test(post("/affine-read", "abc", "Connection: close\r\n")), "[T] (h) a handler body that moved away and then reads its BodyStream fails the read's affinity check (Debug)");
	App app;

	// [A] A coroutine handler that waits suspends; the loop serves others. One reactor.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 256});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		Resp slow;
		std::thread st([&] { slow = one(port, get("/slow", "Connection: close\r\n")); });
		std::this_thread::sleep_for(60ms);
		std::vector<double> fast_ms;
		for (int i = 0; i < 20; ++i) {
			auto t0 = clk::now();
			auto r = one(port, get("/fast", "Connection: close\r\n"));
			if (r.status != 200) { fast_ms.push_back(1e9); } else { fast_ms.push_back(since_ms(t0)); }
		}
		st.join();
		const double worst = *std::max_element(fast_ms.begin(), fast_ms.end());
		std::printf("  [A] 20 fast under 1 suspended slow on one loop -> worst %.1f ms\n", worst);
		check(slow.status == 200 && slow.body == "slow", "[A] the suspended handler completes");
		check(worst < 150.0, "[A] the loop serves other connections while a handler is suspended");
		auto r = one(port, get("/throw-later", "Connection: close\r\n"));
		check(r.status == 500 && r.body == "mapped: later", "[A] a throw after a suspension reaches on_error");
		check(app.inside_run_ok.load(), "[A] handlers run inside the reactor's loop");
	}

	body_stream_tests();

	// [C] A reset cancels reads, not writes; a half-close cancels nothing.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 256});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);

		{   // a reset: the read is cancelled early; nobody is left to answer
			auto o = app.get_obs("close");
			Conn c(port);
			c.send(get("/read?id=close&ms=3000"));
			until([&] { return o->started.load(); });
			auto t0 = clk::now();
			c.rst();
			until([&] { return o->finished.load(); }, 4000);
			std::printf("  [C] reset: cancelled after %.0f ms, finished %.0f ms after the reset\n", o->cancelled_after_ms.load(), since_ms(t0));
			check(o->reason == static_cast<int>(http::disconnected), "[C] a client reset cancels the request with Disconnect");
			check(o->finished && since_ms(t0) < 1500.0, "[C] the read stopped at its next checkpoint (not after its full 3 s)");
			check(o->frame_ok, "[C] the request frame was alive after the cancel");
		}
		{   // a write is never cancelled: it completes and its response is attempted
			auto o = app.get_obs("put");
			Conn c(port);
			c.send("PUT /write?id=put&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == -1, "[C] a half-close does not cancel the write");
			check(r.status == 200 && r.body == "write:completed" && o->frame_ok, "[C] a write completes after the client half-closed, and its response arrives");
			auto o2 = app.get_obs("put2");
			Conn c2(port);
			c2.send("PUT /write?id=put2&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o2->started.load(); });
			c2.close();
			until([&] { return o2->finished.load(); }, 3000);
			check(o2->finished.load(), "[C] a write runs to completion even when the client is gone");
		}
		{   // C17.2: a pipelined byte ends the watch for the rest of that request
			auto o = app.get_obs("pipe");
			Conn c(port);
			c.send(get("/read?id=pipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);           // the watch is reading
			c.send("G");                                 // the next request's first byte arrives while it runs
			std::this_thread::sleep_for(50ms);           // and the watch consumes it
			c.rst();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == -1, "[C] after a pipelined byte, a later reset is not seen (the documented limit)");
		}
		{   // without the byte the same close is seen (the control for the case above)
			auto o = app.get_obs("nopipe");
			Conn c(port);
			c.send(get("/read?id=nopipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.rst();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == static_cast<int>(http::disconnected), "[C] the same reset without the byte is seen");
		}
	}

	// [J] A half-closed client keeps its response (D126); a reset or a failed write cancels.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 256});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		const auto released = [&] { return until([&] { return svc.reactor(0).gate().inflight.load() == 0; }); };
		{   // a plain handler that polls its token: the half-close does not stop it, its whole answer arrives
			auto o = app.get_obs("j-read");
			Conn c(port);
			c.send(get("/read?id=j-read&ms=300"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == -1, "[J] a half-close does not cancel a request in flight (reason " + std::to_string(o->reason.load()) + ")");
			check(r.complete && r.status == 200 && r.body == "read:completed", "[J] a half-closed client gets the full response of a handler that checks its token (" + r.body + ")");
			check(c.closed_by_peer() && released(), "[J] and then the connection closes and the request is released");
		}
		{   // a long poll held on its token, half-closed before the handler first asks: answered at its own bound
			auto o = app.get_obs("j-poll");
			Conn c(port);
			c.send(get("/poll?id=j-poll&ms=300"));
			c.shut_wr();
			auto r = c.read_response();
			until([&] { return o->started.load(); });   // orders the handler's writes before this thread's reads
			std::printf("  [J] half-closed long poll: answered after %.0f ms (bound 300 ms)\n", o->finished_after_ms.load());
			check(o->reason == -1 && r.complete && r.status == 200 && r.body == "poll:bound",
				"[J] a long poll waiting on its token, half-closed before it asked, is answered when its own bound ends it (" + r.body + ")");
			check(o->finished_after_ms.load() >= 290.0, "[J] it ran to its bound, not cut short by the half-close");
			check(c.closed_by_peer() && released(), "[J] and then the connection closes and the request is released");
			o->token.reset();
		}
		{   // a reset cancels the long poll at once
			auto o = app.get_obs("j-poll-rst");
			Conn c(port);
			c.send(get("/poll?id=j-poll-rst&ms=3000"));
			until([&] { return o->started.load(); });
			c.rst();
			until([&] { return o->finished.load(); }, 4000);
			check(o->reason == static_cast<int>(http::disconnected) && o->finished_after_ms.load() < 1500.0,
				"[J] a reset cancels a held long poll with http::disconnected before its bound");
			check(released(), "[J] and the request is released");
			o->token.reset();
		}
		{   // the peer half-closes, then leaves: nothing is seen until the poll's bound, then the write fails and cancels
			auto o = app.get_obs("j-poll-gone");
			Conn c(port);
			c.send(get("/poll?id=j-poll-gone&ms=300"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			std::this_thread::sleep_for(50ms);   // the watch read the EOF
			c.rst();
			const auto t0 = clk::now();
			check(released(), "[J] a long poll whose peer half-closed and left ends by its own bound");
			const bool cancelled = o->token && o->token->requested() && o->token->reason() == http::disconnected;
			std::printf("  [J] half-closed then gone: released %.0f ms after the reset, poll ran %.0f ms (bound 300 ms)\n", since_ms(t0), o->finished_after_ms.load());
			check(o->finished_after_ms.load() >= 290.0 && o->finished_after_ms.load() < 1500.0, "[J] nothing waits past the handler's own bound");
			check(cancelled, "[J] and the failed write cancels the request (http::disconnected)");
			o->token.reset();
		}
		{   // the same on the streamed-body path: its failed write cancels too
			auto o = app.get_obs("j-stream-big");
			Conn c(port);
			c.send(post("/stream-big-token?id=j-stream-big", "abc"));
			until([&] { return o->finished.load(); });
			check(c.fill(), "[J] streamed body: the big reply starts to arrive");
			c.rst();
			check(released(), "[J] streamed body: the big reply's failed write ends the connection");
			check(o->token && o->token->requested() && o->token->reason() == http::disconnected,
				"[J] streamed body: a failed write cancels the request: the token the handler kept sees http::disconnected");
			o->token.reset();
		}
		{   // a failed write with no half-close: a token kept past handle() sees http::disconnected
			auto o = app.get_obs("j-big");
			Conn c(port);
			c.send(get("/big-token?id=j-big"));
			until([&] { return o->finished.load(); });
			check(c.fill(), "[J] the big reply starts to arrive");
			c.rst();
			check(released(), "[J] the big reply's failed write ends the connection");
			check(o->token && o->token->requested() && o->token->reason() == http::disconnected,
				"[J] a failed write cancels the request: the token the handler kept sees http::disconnected");
			o->token.reset();
		}
	}

	// [D] The in-flight gate: a request over the cap is refused with 503 + Retry-After: 1.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 2});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		std::vector<Resp> out(5);
		std::vector<std::thread> ts;
		for (int i = 0; i < 5; ++i) { ts.emplace_back([&, i] { out[static_cast<std::size_t>(i)] = one(port, get("/hold", "Connection: close\r\n")); }); }
		for (auto& t : ts) { t.join(); }
		int n200 = 0, n503 = 0; bool retry = true;
		for (auto& r : out) {
			if (r.status == 200) { ++n200; }
			if (r.status == 503) { ++n503; retry = retry && header_value(r.head, "retry-after") == "1"; }
		}
		std::printf("  [D] 5 simultaneous at a cap of 2 -> %d x 200, %d x 503, refused=%llu\n", n200, n503,
			static_cast<unsigned long long>(svc.reactor(0).gate().rejected.load()));
		check(n200 == 2 && n503 == 3, "[D] exactly the cap is admitted; the rest get 503");
		check(retry, "[D] every 503 carries Retry-After: 1");
		check(svc.reactor(0).gate().rejected == 3, "[D] the gate counts the refusals");
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[D] every lease is released once the responses settle");
		check(one(port, get("/fast", "Connection: close\r\n")).status == 200, "[D] admission resumes after the load drains");
		Conn keep(port);   // a connection that stays open: its lease must end with each response, not with the socket
		for (int i = 0; i < 3; ++i) { keep.send(get("/fast")); keep.read_response(); }
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[D] a keep-alive connection holds no lease between requests");
	}

	// [S] The streamed join (design D6): the feed and the handler, children of the connection.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		// A complete body, then a reset while the handler still runs: the watch cancels the request,
		// whether the handler asks for its token after the body (the watch waits for it, as for a
		// buffered body) or asked during it (the watch starts with the body's end). A half-close
		// instead cancels nothing: the handler runs to its bound and the answer comes.
		for (const char* when : {"?ms=2000", "?early=1&ms=2000"}) {
			Conn c(port);
			c.send(post(std::string("/body-wait") + when, std::string(5000, 'a')));
			std::this_thread::sleep_for(100ms);
			const auto t0 = clk::now();
			c.rst();
			const bool ended = until([&] { return svc.reactor(0).gate().inflight.load() == 0; }, 1500);
			check(ended && since_ms(t0) < 1500.0, std::string("[S] after a complete body, a reset cancels the running handler ") + (when[1] == 'e' ? "that asked during the body" : "that asks after it")
				+ " (released after " + std::to_string(static_cast<int>(since_ms(t0))) + " ms, bound 2000 ms)");
		}
		for (const char* when : {"?ms=300", "?early=1&ms=300"}) {
			Conn c(port);
			c.send(post(std::string("/body-wait") + when, std::string(5000, 'a')));
			std::this_thread::sleep_for(100ms);
			c.shut_wr();
			auto r = c.read_response();
			check(r.status == 200 && r.body == "5000:done" && c.closed_by_peer(), std::string("[S] after a complete body, a half-close does not cancel the running handler ") + (when[1] == 'e' ? "that asked during the body" : "that asks after it") + " (" + r.body + ")");
		}
		// A handler that answers without reading the body, or after its first chunk: the feed stops
		// (its push, or its read of the rest the client holds) and the connection closes.
		for (const char* when : {"", "?read=1"}) {
			Conn c(port, 3);
			c.send(std::string("POST /body-skip") + when + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(1000, 'a'));
			const auto t0 = clk::now();
			auto r = c.read_response();
			const double ms = since_ms(t0);
			const std::string how = *when ? "after the body's first chunk" : "without reading the body";
			check(r.status == 200 && r.body == "skipped" && ms < 1000,
				"[S] a handler that answers " + how + " is answered at once (" + std::to_string(static_cast<int>(ms)) + " ms): its end stops the feed");
			check(c.closed_by_peer(), "[S] and the connection closes after the response (" + how + ")");
		}
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[S] every request is released");
	}

	// [E] The buffered-body cap.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64, .max_body = 1024});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto post = [&](const std::string& path, std::size_t advertised, const std::string& sent) {
			Conn c(port);
			c.send("POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(advertised) + "\r\nConnection: close\r\n\r\n" + sent);
			return c.read_response();
		};
		auto r = post("/echo", 5000, "only-ten-bytes");
		check(r.status == 413 && header_value(r.head, "connection") == "close", "[E] a Content-Length over the cap is refused at headers-complete (413), before the body is read");
		r = post("/echo", 1024, std::string(1024, 'a'));
		check(r.status == 200 && r.body == "1024", "[E] a body exactly at the cap is accepted");
		r = post("/echo", 1025, std::string(1025, 'a'));
		check(r.status == 413, "[E] one byte over is refused");
		{   // chunked: no Content-Length, so the cap applies as bytes arrive
			Conn c(port);
			std::string body;
			for (int i = 0; i < 3; ++i) { body += "200\r\n" + std::string(512, 'b') + "\r\n"; }
			c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + body + "0\r\n\r\n");
			auto rc = c.read_response();
			check(rc.status == 413, "[E] a chunked body past the cap is refused as the bytes arrive");
		}
		{
			Conn c(port);
			c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n200\r\n" + std::string(512, 'c') + "\r\n0\r\n\r\n");
			auto rc = c.read_response();
			check(rc.status == 200 && rc.body == "512", "[E] a chunked body under the cap is accepted");
		}
		r = post("/stream", 100000, std::string(100000, 'z'));
		check(r.status == 200 && r.body == "streamed 100000", "[E] a streamed body is not capped");
		r = post("/sink", 100000, std::string(100000, 'z'));
		check(r.status == 200 && r.body == "sunk", "[E] a body pushed to a sink is not capped");
	}
	{   // the default cap is 16 MiB; 0 turns it off
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		Conn c(port);
		c.send("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string((16u << 20) + 1) + "\r\nConnection: close\r\n\r\n");
		check(c.read_response().status == 413, "[E] the default cap is 16 MiB");
	}

	// [F] Shutdown.
	{   // a running request is cancelled (Shutdown), its frame outlives the cancel, and its response is written
		unsigned short port = 0;
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, http::Limits{.max_in_flight = 64});
		app.service = svc.get();
		svc->start(0); port = svc->port(); wait_listen(port);
		auto o = app.get_obs("stop");
		Conn c(port);
		c.send(get("/read?id=stop&ms=5000"));
		until([&] { return o->started.load(); });
		auto t0 = clk::now();
		std::thread stopper([&] { svc->stop(); });
		auto r = c.read_response();
		stopper.join();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with a request in flight -> %.0f ms\n", ms);
		check(o->reason == static_cast<int>(task::cancel_reason::shutdown), "[F] shutdown cancels a running request with Shutdown");
		check(r.status == 200 && r.body == "read:cancelled" && o->frame_ok, "[F] the cancelled request's response is still written, from a live frame");
		check(ms < 2000.0, "[F] stop() returns promptly once the request completes");
	}
	{   // an idle keep-alive connection that already served a request does not hold stop()
		unsigned short port = 0;
		auto svc = std::make_unique<http::HttpAsioService>(app, 2, http::Limits{.max_in_flight = 64});
		app.service = svc.get();
		svc->start(0); port = svc->port(); wait_listen(port);
		std::vector<std::unique_ptr<Conn>> idle;
		for (int i = 0; i < 8; ++i) {
			idle.push_back(std::make_unique<Conn>(port));
			idle.back()->send(get("/fast"));
			if (idle.back()->read_response().status != 200) { check(false, "[F] warm-up request"); }
		}
		auto t0 = clk::now();
		svc->stop();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with 8 idle keep-alive connections -> %.0f ms\n", ms);
		check(ms < 1000.0, "[F] idle keep-alive connections (a pending watch read) are cancelled at once");
	}
	{   // a stalled writer is bounded by the grace
		unsigned short port = 0;
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, http::Limits{.max_in_flight = 64, .shutdown_grace = 300ms});
		app.service = svc.get();
		svc->start(0); port = svc->port(); wait_listen(port);
		Conn c(port);
		int rcv = 4096; ::setsockopt(c.fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
		c.send(get("/huge"));
		std::this_thread::sleep_for(300ms);   // the 64 MiB response is stuck in the socket buffers: we never read
		auto t0 = clk::now();
		svc->stop();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with a stalled write, grace 300 ms -> %.0f ms\n", ms);
		check(ms >= 250.0, "[F] the stalled write is given the grace");
		check(ms < 3000.0, "[F] after the grace the socket is closed and stop() returns");
	}

	// [G] The per-request watch does not disturb pipelining or keep-alive.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		Conn c(port);
		c.send(get("/fast") + get("/echo") + get("/fast", "Connection: close\r\n"));
		auto a = c.read_response(), b = c.read_response(), d = c.read_response();
		check(a.body == "fast" && b.body == "0" && d.body == "fast", "[G] three pipelined requests are answered in order");
		Conn k(port);
		int ok = 0;
		for (int i = 0; i < 200; ++i) {
			k.send(get("/fast"));
			if (k.read_response().body == "fast") { ++ok; }
		}
		check(ok == 200, "[G] 200 sequential keep-alive requests, each with the watch armed and released");
	}

	// [H] A body cut short never reads as a clean end: the reader gets an error other than eof,
	// and the request is already cancelled when its read returns.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		const auto head = [](const std::string& id) {
			return "POST /tbody?id=" + id + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(1000, 'a');
		};
		const auto outcome = [&](const std::string& id, const char* what) {
			auto o = app.get_obs(id);
			until([&] { return o->finished.load(); }, 3000);
			std::printf("  [H] %s: end_kind=%d cancelled=%d\n", what, o->end_kind.load(), int(o->cancelled_at_end.load()));
			check(o->finished && o->end_kind == 3 && o->cancelled_at_end,
				std::string("[H] ") + what + ": the reader gets an error (not eof) and the request is cancelled");
		};
		{ Conn c(port); c.send(head("trunc-fin")); std::this_thread::sleep_for(80ms); c.close(); outcome("trunc-fin", "Content-Length body, client close"); }
		{ Conn c(port); c.send(head("trunc-rst")); std::this_thread::sleep_for(80ms); c.rst(); outcome("trunc-rst", "Content-Length body, client reset"); }
		{ Conn c(port); c.send(head("trunc-half")); std::this_thread::sleep_for(80ms); c.shut_wr(); outcome("trunc-half", "Content-Length body, half-close"); }
		{
			Conn c(port);
			c.send("POST /tbody?id=trunc-chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n");
			std::this_thread::sleep_for(80ms); c.close();
			outcome("trunc-chunked", "chunked body cut after a chunk");
		}
		{   // a body that does complete still ends with eof
			Conn c(port);
			c.send("POST /stream HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello");
			auto r = c.read_response();
			check(r.status == 200 && r.body == "streamed 5", "[H] a complete body still ends cleanly");
		}
	}

	// [I] A cancel callback that throws is dropped; the handler's frames end by completing.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		{   // streamed: the connection frame must not unwind under the running handler
			auto o = app.get_obs("sx");
			Conn c(port);
			c.send("POST /throwcb-stream?id=sx HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(100, 'a'));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.close();
			std::this_thread::sleep_for(120ms);
			check(!o->finished && svc.reactor(0).gate().inflight.load() == 1,
				"[I] streamed: after the throwing callback the connection still holds its request (the handler runs)");
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] streamed: the handler finishes and its request was intact");
			check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[I] streamed: the request is released afterwards");
		}
		{   // buffered: the throw must not leave io_context::run()
			auto o = app.get_obs("bx");
			Conn c(port);
			c.send(get("/throwcb?id=bx"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.rst();
			std::this_thread::sleep_for(50ms);
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] buffered: the handler finishes after a throwing callback");
			check(one(port, get("/fast", "Connection: close\r\n")).body == "fast", "[I] buffered: the server keeps serving");
		}
		{   // a late registration that throws
			http::Request r;
			r.cancel(http::disconnected);
			bool threw = false;
			try { task::cancel_callback late(r.cancel_token(), [] { throw std::runtime_error("late"); }); } catch (...) { threw = true; }
			check(!threw && r.cancel_token().requested(), "[I] a late cancel callback that throws is dropped");
		}
	}

	// [T] The task loop (design T4 (b)-(h); (a) and the never-asking watch are handler_count_test).
	{   // (b) a budget loop is demoted between slices, the loop serves others meanwhile, and nothing stays queued after stop()
		unsigned short port = 0;
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, http::Limits{.max_in_flight = 64});
		app.service = svc.get();
		svc->start(0); port = svc->port(); wait_listen(port);
		app.demoted_seen = 0;
		Resp budget;
		std::thread long_one([&] { budget = one(port, get("/budget?ms=300", "Connection: close\r\n")); });
		std::this_thread::sleep_for(30ms);
		Conn c(port);
		int answered = 0;
		for (auto end = clk::now() + 150ms; clk::now() < end;) {
			c.send(get("/demoted"));
			if (c.read_response().status == 200) { ++answered; }
		}
		long_one.join();
		std::printf("  [T] (b) %d requests answered during a 300 ms budget loop; most demoted seen %zu\n", answered, app.demoted_seen.load());
		check(budget.status == 200 && budget.body == "budget", "[T] (b) the budget loop answers");
		check(answered > 10 && app.demoted_seen.load() > 0, "[T] (b) the loop ran other requests while the budget loop waited demoted");
		c.close();
		svc->stop();
		check(svc->reactor(0).scheduler().demoted() == 0, "[T] (b) nothing is left in the demoted queue after stop()");
	}
	{   // (c) a handler resumed by a std::thread finishes on the loop thread
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto r = one(port, get("/thread?id=thread", "Connection: close\r\n"));
		check(r.status == 200 && r.body == "loop" && app.get_obs("thread")->on_loop, "[T] (c) a handler resumed from a std::thread finishes on the loop thread");
	}
	{   // (d) stop() with handlers suspended in a read, an event, a checkpoint and a body read: every task frame is gone
		const auto baseline = task::detail::live_frames();
		{
			unsigned short port = 0;
			auto svc = std::make_unique<http::HttpAsioService>(app, 2, http::Limits{.max_in_flight = 64});
			app.service = svc.get();
			svc->start(0); port = svc->port(); wait_listen(port);
			Conn a(port), b(port), k(port), s(port);
			a.send(get("/suspend-read?id=d-read"));
			b.send(get("/suspend-event?id=d-event"));
			k.send(get("/checkpoint?id=d-check"));
			s.send("POST /tbody?id=d-body HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(1000, 'b'));
			check(until([&] { return app.get_obs("d-read")->started && app.get_obs("d-event")->started && app.get_obs("d-check")->started
					&& app.get_obs("d-body")->started; }),
				"[T] (d) the four handlers are suspended");
			std::this_thread::sleep_for(20ms);
			check(task::detail::live_frames() > baseline, "[T] (d) live frames are counted while handlers run");
			const auto t0 = clk::now();
			svc->stop();
			const double stop_ms = since_ms(t0);
			std::printf("  [T] (d) stop() with four suspended handlers -> %.0f ms\n", stop_ms);
			check(stop_ms < 1000, "[T] (d) stop() ends a body read without waiting for the grace (5 s): the feed takes the session's token");
			auto ra = a.read_response(), rb = b.read_response(), rk = k.read_response();
			check(ra.body == "read ended" && rb.body == "event ended" && rk.status == 503,
				"[T] (d) each ended by the request's cancellation and answered (" + ra.body + ", " + rb.body + ", " + std::to_string(rk.status) + ")");
			auto rs = s.read_response();
			const auto body = app.get_obs("d-body");
			check(rs.body == "ended" && body->end_kind == 2 && body->cancelled_at_end,
				"[T] (d) a body read ends with operation_aborted at shutdown, the request cancelled, and is answered");
			svc->close();
			svc->destroy_roots();
		}
		check(task::detail::live_frames() == baseline, "[T] (d) no task frame outlives the service (" + std::to_string(task::detail::live_frames() - baseline) + " left)");
	}
	{   // Request::scheduler(): a handler starts a timer and a task::deadline on the connection's scheduler
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto r = one(port, get("/scheduler", "Connection: close\r\n"));
		check(r.status == 200 && r.body == "same,timer,deadline", "[T] Request::scheduler() is the reactor's scheduler and serves a timer and a deadline (" + r.body + ")");
		http::Request detached;
		bool threw = false;
		try { (void)detached.scheduler(); } catch (const std::logic_error&) { threw = true; }
		check(threw, "[T] a request no transport serves has no scheduler");
	}
	{   // (h) a cancel callback on the loop thread, outside any task, touches the writer: the affinity check passes
		unsigned short port = 0;
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, http::Limits{.max_in_flight = 64});
		app.service = svc.get();
		svc->start(0); port = svc->port(); wait_listen(port);
		{   // the client resets: the watch's read completion runs the callback (nobody is left to read the answer)
			auto o = app.get_obs("h-hangup");
			Conn c(port);
			c.send(get("/cb-write?id=h-hangup"));
			until([&] { return o->started.load(); });
			c.rst();
			check(until([&] { return o->finished.load(); }) && until([&] { return svc->reactor(0).gate().inflight.load() == 0; }),
				"[T] (h) a cancel callback on the hang-up writes through the writer");
		}
		{   // the service stops: the reactor's abort walk runs the callback
			auto o = app.get_obs("h-stop");
			Conn c(port);
			c.send(get("/cb-write?id=h-stop"));
			until([&] { return o->started.load(); });
			svc->stop();
			auto r = c.read_response();
			check(r.status == 200 && r.body == "callback wrote" && header_value(r.head, "x-cancelled") == "1",
				"[T] (h) a cancel callback on shutdown writes through the writer");
		}
		svc->close();
		svc->destroy_roots();
	}
	{   // (d) a handler that ignores cancellation: stop() returns within the grace, the phases leave no frame
		const auto baseline = task::detail::live_frames();
		{
			unsigned short port = 0;
			auto svc = std::make_unique<http::HttpAsioService>(app, 1, http::Limits{.max_in_flight = 64, .shutdown_grace = 200ms});
			app.service = svc.get();
			svc->start(0); port = svc->port(); wait_listen(port);
			Conn c(port);
			c.send(get("/ignore?id=d-ignore"));
			check(until([&] { return app.get_obs("d-ignore")->started.load(); }), "[T] (d) the handler that ignores cancellation is suspended");
			std::this_thread::sleep_for(20ms);
			const auto t0 = clk::now();
			svc->stop();
			const double took = since_ms(t0);
			std::printf("  [T] (d) stop() with a handler that ignores cancellation (grace 200 ms) -> %.0f ms\n", took);
			check(took < 2 * 200 + 300, "[T] (d) stop() returns within twice the grace plus 300 ms (CI runners add jitter)");
			svc->close();
			svc->destroy_roots();
		}
		check(task::detail::live_frames() == baseline, "[T] (d) no task frame outlives the service after an ignored cancellation (" + std::to_string(task::detail::live_frames() - baseline) + " left)");
	}
	{   // (e) task::cancelled after the request's cancellation is 503 + close; from the handler's own source, 500
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto r = one(port, get("/own-cancel"));
		check(r.status == 500, "[T] (e) task::cancelled from the handler's own source is a 500 (" + std::to_string(r.status) + ")");
		{
			auto o = app.get_obs("e");
			Conn c(port);
			c.send(get("/checkpoint?id=e"));
			until([&] { return o->started.load(); });
			std::thread stopper([&] { svc.stop(); });   // a shutdown cancels the request; the client still reads
			auto answer = c.read_response();
			stopper.join();
			check(answer.status == 503 && header_value(answer.head, "connection") == "close" && c.closed_by_peer(),
				"[T] (e) task::cancelled after the request's cancellation is 503 with Connection: close, then the connection closes");
			check(svc.reactor(0).gate().inflight.load() == 0, "[T] (e) the request is released");
		}
	}
	{   // (f) a hang-up during a budget loop: no checkpoint passes once the request is cancelled
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		auto o = app.get_obs("f");
		Conn c(port);
		c.send(get("/hangup?id=f"));
		until([&] { return o->started.load(); });
		std::this_thread::sleep_for(20ms);
		const auto t0 = clk::now();
		c.rst();
		until([&] { return o->finished.load(); });
		std::printf("  [T] (f) a hang-up during a budget loop -> cancelled after %.1f ms, %d checkpoints after the cancel\n", since_ms(t0), o->checkpoints_after.load());
		check(o->finished && o->checkpoints_after == 0, "[T] (f) the first checkpoint after the hang-up throws task::cancelled");
	}
	{   // (g) an owned stream started, then task::cancelled escapes: no terminal chunk, the request released
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		Conn c(port);
		c.send(get("/owned-cancel"));
		while (c.fill()) {}
		check(c.rest.find("0\r\n\r\n") == std::string::npos, "[T] (g) the abandoned owned stream sends no terminal chunk (" + std::to_string(c.rest.size()) + " bytes)");
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[T] (g) the request is released");
	}
	{   // (h) a child that moves to another scheduler comes home; the handler then writes from its loop
		asio::io_context other_io;
		auto guard = asio::make_work_guard(other_io);
		task::asio_scheduler other(other_io.get_executor());
		std::thread other_thread([&] { other_io.run(); });
		app.away = &other;
		{
			unsigned short port = 0;
			http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
			app.service = &svc;
			svc.start(0); port = svc.port(); wait_listen(port);
			auto r = one(port, get("/affine-child", "Connection: close\r\n"));
			check(r.status == 200 && r.body == "home", "[T] (h) a child that schedule_on()s away comes home: the writer passes its check");
			auto rr = one(port, post("/affine-read-child", "abc", "Connection: close\r\n"));
			check(rr.status == 200 && rr.body == "abc", "[T] (h) a child that schedule_on()s away comes home: the body read passes its check");
			auto rw = one(port, post("/affine-wrapped", "abc", "Connection: close\r\n"));
			check(rw.status == 200 && rw.body == "abc" && header_value(rw.head, "X-Wrapped") == "1", "[T] (h) a wrapper scheduler on the loop thread reads the body and writes: the affinity checks pass");
		}
		guard.reset();
		other_io.stop();
		other_thread.join();
		other.close();
		other.destroy_roots();
		app.away = nullptr;
	}

	// [X] Teardown phases across the service and an application's own scheduler (W2 D15).
	{
		const auto baseline = task::detail::live_frames();
		{
			asio::io_context app_io;
			auto guard = asio::make_work_guard(app_io);
			task::asio_scheduler app_scheduler(app_io.get_executor());
			std::thread app_thread([&] { app_io.run(); });
			{
				unsigned short port = 0;
				http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
				app.service = &svc;
				svc.start(0); port = svc.port(); wait_listen(port);
				// A root of the application's scheduler suspended on a timer of the service's loop, and a
				// root of the service's loop suspended on an event nobody sets.
				asio::steady_timer far(svc.reactor(0).io(), 1h);
				task::event never;
				std::atomic<int> suspended{0};
				auto on_app = [&]() -> task::task<void> {
					++suspended;
					co_await task::await_asio<void(asio::error_code)>([&](auto h) { far.async_wait(std::move(h)); });
				};
				auto on_service = [&]() -> task::task<void> { ++suspended; co_await never; };
				task::spawn(task::scheduler_ref::of(app_scheduler), on_app(), [](std::exception_ptr) {});
				task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), on_service(), [](std::exception_ptr) {});
				check(until([&] { return suspended.load() == 2; }), "[X] both roots are suspended");
				check(one(port, get("/fast", "Connection: close\r\n")).body == "fast", "[X] the service serves");
				svc.stop();                                     // phase 1, on every loop
				guard.reset(); app_io.stop(); app_thread.join();
				svc.close();                                    // phase 2, on every scheduler
				app_scheduler.close();
				bool ran = false;
				auto late = [&]() -> task::task<void> { ran = true; co_return; };
				task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), late(), [](std::exception_ptr) {});
				check(!ran, "[X] a task spawned on a closed service scheduler is destroyed unrun");
				svc.destroy_roots();                            // phase 3, with every io_context alive
				app_scheduler.destroy_roots();
				check(task::detail::live_frames() == baseline, "[X] after destroy_roots() on both, no task frame is left (" + std::to_string(task::detail::live_frames() - baseline) + ")");
			}                                                   // phase 4: the service's timer dies before its io_context
		}
		{   // any order is safe: destroy_roots() alone runs stop() and close() first
			unsigned short port = 0;
			http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
			app.service = &svc;
			svc.start(0); port = svc.port(); wait_listen(port);
			task::event never;
			auto parked = [&]() -> task::task<void> { co_await never; };
			task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), parked(), [](std::exception_ptr) {});
			std::this_thread::sleep_for(20ms);
			svc.destroy_roots();
			check(task::detail::live_frames() == baseline, "[X] destroy_roots() alone stops, closes and destroys the parked root");
		}
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
