// BodyStream semantics (section [B]), driven directly on a private loop.

#include "support.h"

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
