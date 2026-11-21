#pragma once

// The coroutine test's application: the HttpHandler whose routes the sections drive.

#include "support.h"

namespace detail {

struct App : http::HttpHandler {
	std::mutex mutex;
	std::map<std::string, std::shared_ptr<Obs>> obs;
	std::atomic<bool> inside_run_ok{true};
	std::atomic<std::size_t> demoted_seen{0};   // the most /demoted saw queued on reactor 0
	task::asio_scheduler* away = nullptr;       // another loop's scheduler, for /affine-*
	http::HttpAsioService* service = nullptr;   // the service running it: its handlers sleep on its loops
	std::shared_ptr<Obs> get_obs(const std::string& id) {
		std::lock_guard<std::mutex> lk(mutex);
		auto& o = obs[id];
		if (!o) { o = std::make_shared<Obs>(); }
		return o;
	}
	static std::string param(const std::string& query, const std::string& name) {
		auto p = query.find(name + "=");
		if (p == std::string::npos) { return ""; }
		p += name.size() + 1;
		return query.substr(p, query.find('&', p) - p);
	}
	static task::task<bool> sleep_for(const http::Request& req, std::chrono::milliseconds d) { return ::sleep_for(req.scheduler().executor(), d); }
	bool wants_body_stream(const http::Request& r) override { return r.path == "/stream" || r.path == "/tbody" || r.path == "/throwcb-stream" || r.path.rfind("/affine-read", 0) == 0 || r.path == "/affine-wrapped"
		|| r.path == "/body-wait" || r.path == "/body-skip" || r.path == "/stream-big-token"; }
	std::unique_ptr<http::BodySink> on_request_body(http::Request& r) override {
		struct Null : http::BodySink { std::size_t n = 0; void write(std::string_view c) override { n += c.size(); } void end() override {} };
		return r.path == "/sink" ? std::make_unique<Null>() : nullptr;
	}
	void on_error(std::exception_ptr e, const http::Request&, http::ResponseWriter& w) override {
		// task::cancelled is left unanswered: the transport maps it (503 when the request was cancelled).
		try { std::rethrow_exception(e); } catch (const task::cancelled&) {} catch (const std::exception& x) { w.send(500, std::string("mapped: ") + x.what()); }
	}
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (!reactor::inside_run()) { inside_run_ok = false; }
		const std::string id = param(req.query, "id");
		if (req.path == "/fast") { w.send(200, "fast"); co_return; }
		if (req.path == "/echo") { w.send(200, std::to_string(req.body.size())); co_return; }
		if (req.path == "/sink") { w.send(200, "sunk"); co_return; }
		if (req.path == "/slow") {   // suspends, then answers
			co_await sleep_for(req, 300ms);
			w.send(200, "slow");
			co_return;
		}
		if (req.path == "/throw-later") {   // throws after a suspension: the exception crosses it
			co_await sleep_for(req, 50ms);
			throw std::runtime_error("later");
		}
		if (req.path == "/hold") { co_await sleep_for(req, 1000ms); w.send(200, "held"); co_return; }
		if (req.path == "/huge") { w.send(200, std::string(64u << 20, 'x')); co_return; }
		if (req.path == "/stream") {   // reads the body with co_await; answers with the byte count
			std::size_t total = 0;
			std::string chunk;
			for (;;) {
				asio::error_code ec = co_await req.body_stream->read(chunk);
				if (ec == asio::error::eof) { break; }
				if (ec) { w.send(400, "failed: " + ec.message()); co_return; }
				total += chunk.size();
			}
			w.send(200, "streamed " + std::to_string(total));
			co_return;
		}
		if (req.path == "/tbody") {   // reads a streamed body to its end and records how it ended
			auto o = get_obs(id);
			o->started = true;
			std::string chunk;
			for (;;) {
				asio::error_code ec = co_await req.body_stream->read(chunk);
				if (!ec) { continue; }
				o->cancelled_at_end = req.cancel_token().requested();
				o->end_kind = ec == asio::error::eof ? 1 : ec == asio::error::operation_aborted ? 2 : 3;
				break;
			}
			o->finished = true;
			w.send(200, "ended");
			co_return;
		}
		if (req.path == "/throwcb" || req.path == "/throwcb-stream") {   // a cancel callback that throws
			auto o = get_obs(id);
			task::cancel_callback throws(req.cancel_token(), [] { throw std::runtime_error("callback throws"); });
			o->started = true;
			// Held until the test releases it (5 s at most), so a slow host cannot finish it early.
			for (auto end = clk::now() + 5s; !o->release.load() && clk::now() < end;) { co_await sleep_for(req, 5ms); }
			o->cancelled_late = req.cancel_token().requested() && !req.path.empty();   // touches the request after the cancel
			o->finished = true;
			w.send(200, "survived");
			co_return;
		}
		if (req.path == "/read" || req.path == "/write") {
			// /read stops at its next checkpoint once cancelled (a search); /write ignores the
			// cancellation and finishes (a PUT). Both then answer, and both touch the request
			// and the writer after the cancel: the frame must still be alive.
			auto o = get_obs(id);
			o->t0 = clk::now();
			const task::cancel_token token = req.cancel_token();
			task::cancel_callback record(token, [o](task::cancel_reason r) {
				o->reason = static_cast<int>(r);
				o->cancelled_after_ms = since_ms(o->t0);
			});
			o->started = true;
			const auto limit = std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str()));
			const auto until_t = clk::now() + limit;
			const bool reads = req.path == "/read";
			while (clk::now() < until_t && !(reads && token.requested())) { co_await sleep_for(req, 10ms); }
			co_await sleep_for(req, 100ms);
			o->frame_ok = !req.path.empty() && req.method == (reads ? "GET" : "PUT");
			w.send(200, std::string(reads ? "read:" : "write:") + (token.requested() ? "cancelled" : "completed"));
			o->finished = true;
			co_return;
		}
		if (req.path == "/budget") {   // T4 (b): a long computation in 200 us slices, demoted between them
			task::budget slice(200us, req.cancel_token());
			for (auto end = clk::now() + std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str())); clk::now() < end;) {
				spin(20us);
				co_await slice.checkpoint();
			}
			w.send(200, "budget");
			co_return;
		}
		if (req.path == "/demoted") {   // T4 (b): runs while /budget waits in the demoted queue
			const std::size_t n = service->reactor(0).scheduler().demoted();
			for (std::size_t seen = demoted_seen.load(); n > seen && !demoted_seen.compare_exchange_weak(seen, n);) {}
			w.send(200, std::to_string(n));
			co_return;
		}
		if (req.path == "/thread") {   // T4 (c): resumed by an event a std::thread sets
			auto o = get_obs(id);
			const auto loop = std::this_thread::get_id();
			task::event done;
			std::thread setter([&done] { std::this_thread::sleep_for(20ms); done.set(); });
			co_await done;
			o->on_loop = std::this_thread::get_id() == loop;
			setter.join();
			w.send(200, o->on_loop ? "loop" : "away");
			co_return;
		}
		if (req.path == "/suspend-read") {   // T4 (d): suspended in a read of a pipe nobody writes; the request's token ends it
			auto o = get_obs(id);
			int fds[2];
			if (::pipe(fds) != 0) { w.send(500, "pipe"); co_return; }
			asio::posix::stream_descriptor pipe(req.scheduler().executor(), fds[0]);
			char b[16];
			o->started = true;
			const auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
				[&](auto h) { pipe.async_read_some(asio::buffer(b), std::move(h)); }, req.cancel_token());
			(void)n;
			::close(fds[1]);
			o->finished = true;
			w.send(200, error == asio::error::operation_aborted ? "read ended" : "read " + error.message());
			co_return;
		}
		if (req.path == "/suspend-event") {   // T4 (d): suspended on an event the request's cancellation sets
			auto o = get_obs(id);
			task::event wake;
			task::cancel_callback set(req.cancel_token(), [&wake] { wake.set(); });
			o->started = true;
			co_await wake;
			o->finished = true;
			w.send(200, "event ended");
			co_return;
		}
		if (req.path == "/scheduler") {   // Request::scheduler(): the connection's reactor scheduler serves timers and task::deadline
			const bool same = &req.scheduler() == &service->reactor(0).scheduler();
			const bool timer = co_await sleep_for(req, 5ms);
			task::deadline<task::asio_scheduler> limit(req.scheduler(), clk::now() + 20ms);
			asio::steady_timer long_timer(req.scheduler().executor(), 30s);
			const auto t0 = clk::now();
			const asio::error_code error = co_await task::await_asio<void(asio::error_code)>(
				[&](auto h) { long_timer.async_wait(std::move(h)); }, limit.token());
			w.send(200, std::string(same ? "same," : "other,") + (timer ? "timer," : "no-timer,") +
				(limit.expired() && error && since_ms(t0) < 5000 ? "deadline" : "no-deadline"));
			co_return;
		}
		if (req.path == "/ignore") {   // T4 (d): a handler with no token, suspended on a long timer: only the phases end it
			auto o = get_obs(id);
			o->started = true;
			co_await sleep_for(req, 1h);
			w.send(200, "never");
			co_return;
		}
		if (req.path == "/cb-write") {   // T4 (h): the writer touched from a cancel callback on the loop thread (no current task)
			auto o = get_obs(id);
			task::event wake;
			task::cancel_callback set(req.cancel_token(), [&w, &wake] { w.set_header("X-Cancelled", "1"); wake.set(); });
			o->started = true;
			co_await wake;
			o->finished = true;
			w.send(200, "callback wrote");
			co_return;
		}
		if (req.path == "/checkpoint") {   // T4 (d), (e): a budget loop until the request is cancelled; task::cancelled escapes
			auto o = get_obs(id);
			task::budget slice(200us, req.cancel_token());
			o->started = true;
			for (auto end = clk::now() + 5s; clk::now() < end;) {
				spin(20us);
				co_await slice.checkpoint();
			}
			w.send(200, "never cancelled");
			co_return;
		}
		if (req.path == "/hangup") {   // T4 (f): no checkpoint passes once the request is cancelled
			auto o = get_obs(id);
			const task::cancel_token token = req.cancel_token();
			bool seen = false;
			int after = 0;
			task::cancel_callback note(token, [&seen] { seen = true; });
			task::budget slice(200us, token);
			o->started = true;
			try {
				for (auto end = clk::now() + 5s; clk::now() < end;) {
					spin(20us);
					co_await slice.checkpoint();
					if (seen) { ++after; }
				}
			} catch (const task::cancelled&) {
				o->checkpoints_after = after;
				o->finished = true;
				throw;
			}
			w.send(200, "never cancelled");
			co_return;
		}
		if (req.path == "/own-cancel") {   // T4 (e): task::cancelled from the handler's own source is any exception
			task::cancel_source mine;
			mine.request();
			task::budget slice(200us, mine.token());
			co_await slice.checkpoint();
			w.send(200, "not thrown");
			co_return;
		}
		if (req.path == "/owned-cancel") {   // T4 (g): an owned stream started, then task::cancelled escapes
			http::StreamOptions options;
			options.reply.deadline = clk::now() + 5s;
			auto reply = w.stream(std::move(options));
			http::Response head;
			head.status = 200;
			reply->start_chunked(head, true);
			auto body = std::make_shared<std::string>("partial");
			reply->publish({body, *body});
			throw task::cancelled{};
		}
		if (req.path == "/affine-child") {   // T4 (h): a child that moves away comes home at its end
			co_await [](task::asio_scheduler& other) -> task::task<void> { co_await task::schedule_on{task::scheduler_ref::of(other)}; }(*away);
			w.send(200, "home");
			co_return;
		}
		if (req.path == "/affine-wrapped") {   // T4 (h): under a wrapper scheduler on the loop thread, the body read and the write pass
			wrapper_scheduler wrapper{req.scheduler()};
			co_await task::schedule_on{task::scheduler_ref::of(wrapper)};
			std::string chunk;
			const asio::error_code ec = co_await req.body_stream->read(chunk);
			w.set_header("X-Wrapped", task::current() == task::scheduler_ref::of(wrapper) ? "1" : "0");
			w.send(200, ec ? "failed" : chunk);
			co_await task::schedule_on{task::scheduler_ref::of(req.scheduler())};
			co_return;
		}
		if (req.path == "/affine-body") {   // T4 (h): the handler's own body moves away, then writes: Debug asserts
			co_await task::schedule_on{task::scheduler_ref::of(*away)};
			w.send(200, "away");
			co_return;
		}
		if (req.path == "/affine-read" || req.path == "/affine-read-child") {   // T4 (h): a body read away from the loop asserts; a child that came home reads
			if (req.path == "/affine-read") { co_await task::schedule_on{task::scheduler_ref::of(*away)}; }
			else { co_await [](task::asio_scheduler& other) -> task::task<void> { co_await task::schedule_on{task::scheduler_ref::of(other)}; }(*away); }
			std::string chunk;
			const asio::error_code ec = co_await req.body_stream->read(chunk);
			co_await task::schedule_on{task::scheduler_ref::of(req.scheduler())};   // home, so only the read is checked
			w.send(200, ec ? "failed" : chunk);
			co_return;
		}
		if (req.path == "/body-wait") {   // [S]: reads the whole body, then waits for the request's cancellation (ms, 2 s by default)
			std::optional<task::cancel_token> early;   // ?early=1: asks for the token before the body completes
			if (param(req.query, "early") == "1") { early = req.cancel_token(); }
			std::string chunk;
			std::size_t total = 0;
			while (!co_await req.body_stream->read(chunk)) { total += chunk.size(); }
			const task::cancel_token token = early ? *early : req.cancel_token();
			const std::string ms = param(req.query, "ms");
			for (auto end = clk::now() + (ms.empty() ? 2000ms : std::chrono::milliseconds(std::atoi(ms.c_str())));
				!token.requested() && clk::now() < end;) { co_await sleep_for(req, 5ms); }
			w.send(200, std::to_string(total) + (token.requested() ? ":cancelled" : ":done"));
			co_return;
		}
		if (req.path == "/body-skip") {   // [S]: answers without reading the body (?read=1: after its first chunk)
			if (param(req.query, "read") == "1") { std::string chunk; (void)co_await req.body_stream->read(chunk); }
			w.send(200, "skipped");
			co_return;
		}
		if (req.path == "/poll") {   // [J]: a long poll held on its token up to its own bound (ms), as Detent's subscription polls
			auto o = get_obs(id);
			o->t0 = clk::now();
			const task::cancel_token token = req.cancel_token();
			o->token = token;
			task::cancel_callback record(token, [o](task::cancel_reason r) {
				o->reason = static_cast<int>(r);
				o->cancelled_after_ms = since_ms(o->t0);
			});
			o->started = true;
			asio::steady_timer bound(req.scheduler().executor(), std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str())));
			(void)co_await task::await_asio<void(asio::error_code)>([&](auto h) { bound.async_wait(std::move(h)); }, token);
			o->finished_after_ms = since_ms(o->t0);
			o->finished = true;
			w.send(200, token.requested() ? "poll:cancelled" : "poll:bound");
			co_return;
		}
		if (req.path == "/big-token") {   // [J]: keeps its token past handle() and answers 32 MiB, more than the socket buffers take
			auto o = get_obs(id);
			o->token = req.cancel_token();
			o->started = true;
			w.send(200, std::string(32u << 20, 'x'));
			o->finished = true;
			co_return;
		}
		if (req.path == "/stream-big-token") {   // [J]: a streamed-body handler that keeps its token and answers 32 MiB
			auto o = get_obs(id);
			std::string chunk;
			while (!co_await req.body_stream->read(chunk)) {}
			o->token = req.cancel_token();
			o->started = true;
			w.send(200, std::string(32u << 20, 'x'));
			o->finished = true;
			co_return;
		}
		if (req.path == "/affine-thread") {   // T4 (h): a raw thread (no task, not the loop) touches the writer: Debug asserts
			std::thread([&w] { w.set_header("X-Thread", "1"); }).join();
			w.send(200, "thread");
			co_return;
		}
		w.send(404, "nope");
	}
};

}  // namespace detail

using namespace detail;
