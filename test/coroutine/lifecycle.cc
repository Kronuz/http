// Coroutine test sections on the loop and its end: shutdown, the task loop, affinity, the teardown phases.

#include "sections.h"

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
bool affinity_death_test(const std::string& request) {
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

void section_F(App& app) {
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
}

void section_T(App& app) {
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
}

void section_X(App& app) {
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
}
