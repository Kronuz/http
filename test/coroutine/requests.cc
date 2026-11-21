// Coroutine test sections on requests: suspension, pipelining, the in-flight gate, the body cap, the streamed join, BodyStream.

#include "sections.h"

void section_A(App& app) {
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
}

void section_G(App& app) {
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
}

void section_D(App& app) {
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
}

void section_S(App& app) {
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
}

void section_E(App& app) {
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
}

// ---- [B] BodyStream, driven directly on a private loop ----
void body_stream_tests() {
	asio::io_context io;
	task::asio_scheduler scheduler(io.get_executor());
	const auto on_loop = task::scheduler_ref::of(scheduler);
	auto run = [&](task::task<void> t) {
		std::exception_ptr failure;
		task::spawn(on_loop, std::move(t), [&](std::exception_ptr e) { failure = e; });
		io.restart();
		io.run();
		if (failure) { std::rethrow_exception(failure); }
	};
	auto wait = [&](std::chrono::milliseconds d) -> task::task<void> {
		asio::steady_timer t(io, d);
		co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
	};
	using Ec = asio::error_code;
	{   // order, then a sticky eof
		http::BodyStream s(scheduler, 4);
		std::vector<std::string> got; Ec last, again;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				co_await s.push("a", {}); co_await s.push("b", {}); co_await s.push("c", {}); s.finish();
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			std::string c;
			for (;;) {
				Ec ec = co_await s.read(c);
				if (ec) { last = ec; break; }
				got.push_back(c);
			}
			again = co_await s.read(c);
		}());
		check(got == std::vector<std::string>({"a", "b", "c"}), "[B] chunks arrive in order");
		check(last == asio::error::eof && again == asio::error::eof, "[B] the end is eof, and it is sticky");
	}
	{   // fail(): buffered chunks first, then the error
		http::BodyStream s(scheduler, 4);
		std::string first; Ec e1, e2;
		run([&]() -> task::task<void> {
			co_await s.push("kept", {});
			s.fail(asio::error::connection_reset);
			std::string c;
			e1 = co_await s.read(c); first = c;
			e2 = co_await s.read(c);
		}());
		check(!e1 && first == "kept" && e2 == asio::error::connection_reset, "[B] a failed body delivers what was buffered, then the error");
	}
	{   // bounded: a push waits while the channel is full, and abandon() releases it
		http::BodyStream s(scheduler, 2);
		int pushed = 0; bool third = true;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				if (co_await s.push("1", {})) { ++pushed; }
				if (co_await s.push("2", {})) { ++pushed; }
				third = co_await s.push("3", {});   // blocks: capacity 2, nobody reads
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			co_await wait(50ms);
			if (pushed != 2) { throw std::runtime_error("did not fill"); }
			s.abandon();
			co_await wait(20ms);
		}());
		check(pushed == 2 && !third, "[B] the channel is bounded (flow control), and abandon() releases a waiting push");
	}
	{   // a push waiting for room ends when its token is requested
		http::BodyStream s(scheduler, 1);
		task::cancel_source stop;
		bool second = true;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				co_await s.push("1", stop.token());
				second = co_await s.push("2", stop.token());   // blocks: capacity 1, nobody reads
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			co_await wait(20ms);
			stop.request();
			co_await wait(20ms);
		}());
		check(!second, "[B] a requested token ends a waiting push with false");
	}
	{   // a read's token: a requested token ends a pending read, loses nothing, and the next read goes on
		http::BodyStream s(scheduler, 2);
		Ec pending = asio::error::would_block, before = asio::error::would_block, after;
		std::string kept, after_chunk;
		task::cancel_source early;
		early.request();
		run([&]() -> task::task<void> {
			before = co_await s.read(kept, early.token());   // requested before it starts: never receives
			task::cancel_source stop;
			auto canceller = [&]() -> task::task<void> { co_await wait(20ms); stop.request(); };
			task::spawn(on_loop, canceller());
			std::string c;
			pending = co_await s.read(c, stop.token());
			co_await s.push("kept", {});
			after = co_await s.read(after_chunk);
		}());
		check(before == asio::error::operation_aborted && kept.empty(), "[B] a read whose token was already requested returns operation_aborted at once");
		check(pending == asio::error::operation_aborted, "[B] a pending read ends with operation_aborted when its token is requested");
		check(!after && after_chunk == "kept", "[B] a cancelled read loses nothing; the next read gets the chunk");
	}
	{   // a deadline on a read: the handler's own bound
		http::BodyStream s(scheduler, 2);
		Ec ec; bool expired = false;
		run([&]() -> task::task<void> {
			task::deadline<task::asio_scheduler> d(scheduler, std::chrono::steady_clock::now() + 30ms);
			std::string c;
			ec = co_await s.read(c, d.token());
			expired = d.expired();
		}());
		check(ec == asio::error::operation_aborted && expired, "[B] a task::deadline bounds a read");
	}
	{   // Request::cancel_token: a late callback runs at once, and only the first cancel counts
		http::Request r;
		int calls = 0; task::cancel_reason seen = task::cancel_reason::none;
		r.cancel(http::disconnected);
		r.cancel(task::cancel_reason::shutdown);
		task::cancel_callback late(r.cancel_token(), [&](task::cancel_reason why) { ++calls; seen = why; });
		check(calls == 1 && seen == http::disconnected && r.cancel_token().reason() == http::disconnected,
			"[B] a late callback on the request's token runs at once with the first reason");
		r.clear();
		check(!r.cancel_token().requested(), "[B] clear() resets cancellation");
	}
}
