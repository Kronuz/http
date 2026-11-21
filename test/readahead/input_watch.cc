// Read-ahead test [U] sections on the lazy watch, a held read and the waiter's lane.

#include "input.h"

#ifndef READAHEAD_TEST_INTEGRATION_ONLY

// The lazy watch (a plain TCP socket): it reads nothing until engage(), and engage() decides
// what happened meanwhile as the read would have.
void lazy_watch_tests() {
	static_assert(Input::kLazyWatch, "a plain TCP socket's watch waits for engage()");
	{   // nothing is read while nobody asks; asking reads ahead what waits, which ends the watch
		Pair p;
		const std::string data = pattern(10000);
		p.send(data);
		check(p.arrived(10000), "[L] 10000 bytes reached the server's receive queue");
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, /*lazy=*/true);
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 && fired == 0, "[L] a lazy watch reads nothing before engage()");
		in->engage();
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead) && fired == 0,
			"[L] engage() with data waiting reads ahead kReadAhead bytes at once, ends the watch, fires nothing");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[L] and a later EOF does not fire the ended watch");
		auto a = read_once(p, in, 65536), b = read_once(p, in, 65536), c = read_once(p, in, 65536);
		check(a.data + b.data + c.data == data, "[L] the bytes are read, the read-ahead first, in order");
	}
	{   // EOF (a half-close) before anyone asked: engage() reads it, ends the watch, fires nothing
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(until([&] { char b; return ::recv(p.server.native_handle(), &b, 1, MSG_PEEK | MSG_DONTWAIT) == 0; }), "[L] the EOF reached the server");
		check(fired == 0, "[L] an EOF nobody asked about fires nothing");
		in->engage();
		check(fired == 0, "[L] engage() after the EOF: a half-close is not a disconnect");
		check(in->drain().await_ready(), "[L] and no read is pending after the EOF");
		in->watch([&] { ++fired; }, true);   // the next request, already read: its watch reads nothing
		in->engage();
		p.settle();
		check(fired == 0 && in->drain().await_ready(), "[L] a watch set after the EOF fires nothing and starts no read");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[L] and the EOF is read afterwards");
	}
	{   // a reset before anyone asked is a disconnect too. The wait polls: a recv, even a peek, takes
		// the pending reset on Linux, and the next recv then reads EOF (pinned below).
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
		check(until([&] { pollfd f{p.server.native_handle(), POLLIN, 0}; return ::poll(&f, 1, 0) > 0; }), "[L] the reset reached the server");
		in->engage();
		check(fired == 1, "[L] engage() after a reset fires the disconnect");
	}
#if defined(__linux__)
	{   // Linux: a peek that returns the reset takes it, and the next recv reads EOF
		Pair p;
		linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
		int first = 0;
		check(until([&] { char b; const auto n = ::recv(p.server.native_handle(), &b, 1, MSG_PEEK | MSG_DONTWAIT); first = n < 0 ? errno : 0; return n < 0 && errno != EAGAIN; }) && first == ECONNRESET,
			"[L] Linux: a peek after a reset returns ECONNRESET");
		char b;
		check(::recv(p.server.native_handle(), &b, 1, MSG_DONTWAIT) == 0, "[L] Linux: and takes it: the next recv reads EOF, so only the transport's own read may see a reset");
	}
#endif
	{   // asked before anything happened: from then on it is the read-ahead watch
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->engage();
		p.settle();
		check(fired == 0, "[L] engage() with nothing waiting fires nothing");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0 && in->drain().await_ready(), "[L] the read it armed sees the EOF that follows: the watch ends, nothing fires, no read follows");
	}
	{   // asked before anything happened, then a reset: the armed read fires the disconnect
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->engage();
		p.settle();
		linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
		p.settle();
		check(fired == 1, "[L] and the read it armed sees a reset that follows");
	}
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->engage();
		p.settle();
		const std::string data = pattern(10000, 3);
		p.send(data);
		check(p.arrived(10000 - static_cast<int>(Input::kReadAhead)), "[L] 10000 bytes sent after engage()");
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead) && fired == 0,
			"[L] the engaged watch reads ahead kReadAhead bytes, and the data ends it");
		auto a = read_once(p, in, 65536), b = read_once(p, in, 65536);
		check(a.data + b.data == data, "[L] and the stream is intact");
	}
	{   // unwatched (the handler returned): engage() does nothing
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->unwatch();
		::shutdown(p.client, SHUT_WR);
		p.settle();
		in->engage();
		p.settle();
		check(fired == 0, "[L] engage() after unwatch() neither fires nor reads");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[L] the EOF stays for the next read");
	}
	// Request: the first cancel_token() engages the watch, once.
	struct Probe : http::CancelWatch {
		http::Request* request = nullptr;
		bool gone = false;
		int engaged = 0;
		void engage() noexcept override { ++engaged; if (gone) { request->cancel(http::disconnected); } }
	};
	{
		http::Request r;
		Probe w; w.request = &r;
		r.set_cancel_watch(&w);
		check(w.engaged == 0, "[L] setting the watch engages nothing");
		check(!r.cancel_token().requested() && w.engaged == 1, "[L] cancel_token() engages the watch");
		(void)r.cancel_token(); (void)r.cancel_token();
		check(w.engaged == 1, "[L] once");
	}
	for (int how = 0; how < 3; ++how) {
		http::Request r;
		Probe w; w.request = &r; w.gone = true;
		r.set_cancel_watch(&w);
		int calls = 0;
		bool seen = false;
		if (how == 0) { seen = r.cancel_token().requested(); }
		if (how == 1) { seen = r.cancel_token().reason() == http::disconnected; }
		if (how == 2) { task::cancel_callback cb(r.cancel_token(), [&](task::cancel_reason why) { ++calls; seen = why == http::disconnected; }); }
		check(seen && w.engaged == 1 && calls == (how == 2 ? 1 : 0),
			std::string("[L] a hang-up engage() finds is seen by the token that engaged it: ") + (how == 0 ? "requested()" : how == 1 ? "reason()" : "a callback on it runs once"));
	}
	{
		http::Request r;
		Probe w; w.request = &r;
		r.set_cancel_watch(&w);
		r.cancel(task::cancel_reason::shutdown);
		check(r.cancel_token().requested() && w.engaged == 0, "[L] a request already cancelled engages nothing");
		http::Request q;
		q.set_cancel_watch(&w);
		q.clear();
		(void)q.cancel_token();
		check(w.engaged == 0, "[L] clear() drops the watch");
		q.set_cancel_watch(&w);
		q.set_cancel_watch(nullptr);
		(void)q.cancel_token();
		check(w.engaged == 0, "[L] and so does set_cancel_watch(nullptr)");
	}
}

// A read that finds the watch's read pending: that read's completion resumes it (no frame, no
// channel between them), its token ends the wait without losing bytes, and drain() waits for it.
// One read(output) into `r`; its arguments live in its frame.
static task::task<void> read_into(std::shared_ptr<Input> in, Read& r, task::cancel_token token) {
	std::string buf(65536, '\0');
	const http::detail::ReadResult got = co_await in->read(asio::buffer(buf), token);
	if (got.error) { r.error = got.error; } else { r.data.assign(buf, 0, got.bytes); }
	r.done = true;
}
static task::task<void> drain_into(std::shared_ptr<Input> in, bool& drained) {
	co_await in->drain();
	drained = true;
}
void held_read_tests() {
	// Starts one read(output) on the pair's loop; the caller runs the loop.
	const auto start_read = [](Pair& p, const std::shared_ptr<Input>& in, Read& r, task::cancel_token token) {
		task::spawn(task::scheduler_ref::of(p.scheduler), read_into(in, r, std::move(token)));
	};
	const auto run_until = [](Pair& p, const bool& done) {
		p.io.restart();
		for (auto end = clk::now() + 2s; !done && clk::now() < end;) { p.io.run_one_for(5ms); }
	};
	{   // the reader waits on the watch's read; what the client sends later arrives whole and in order
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, /*lazy=*/true);
		in->engage();   // nothing waiting: the watch waits for the socket
		in->unwatch();
		Read r;
		start_read(p, in, r, {});
		p.settle();
		check(!r.done, "[U] a read with the watch's read pending waits for it");
		const std::string data = pattern(10000, 7);
		p.send(data);
		run_until(p, r.done);
		check(r.done && !r.error && !r.data.empty() && r.data.size() <= Input::kReadAhead && data.compare(0, r.data.size(), r.data) == 0,
			"[U] the watch's read resumes the waiting reader with the first bytes (" + std::to_string(r.data.size()) + ")");
		std::string got = r.data;
		while (got.size() < data.size()) { auto n = read_once(p, in, 65536); if (!n.done || n.error) { break; } got += n.data; }
		check(got == data && fired == 0, "[U] and the stream continues from the socket without a gap or a repeat");
	}
	{   // a requested token ends the wait with operation_aborted; the next read gets every byte
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {}, true);
		in->engage();
		in->unwatch();
		task::cancel_source source;
		Read r;
		start_read(p, in, r, source.token());
		p.settle();
		check(!r.done, "[U] the reader waits on the pending read");
		source.request();
		run_until(p, r.done);
		check(r.done && r.error == asio::error::operation_aborted, "[U] its requested token ends the wait with operation_aborted");
		const std::string data = pattern(3000, 8);
		p.send(data);
		check(p.arrived(3000), "[U] 3000 bytes sent after the cancel");
		std::string got;
		while (got.size() < data.size()) { auto n = read_once(p, in, 65536); if (!n.done || n.error) { break; } got += n.data; }
		check(got == data, "[U] and the next reads return them all, in order");
	}
	{   // a token requested after the watch's wait completed, before its handler runs, with nothing
		// to read (a spurious wake): the socket cancel finds no operation, and the read still ends.
		// A wait queued first on the same socket completes in the same reactor pass and runs first:
		// it takes the byte and requests the token, so the watch's handler wakes to EAGAIN.
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {}, true);
		task::cancel_source source;
		bool took = false;
		p.server.async_wait(asio::socket_base::wait_read, [&](asio::error_code error) {
			char b;
			took = !error && ::recv(p.server.native_handle(), &b, 1, MSG_DONTWAIT) == 1;
			source.request();
		});
		in->engage();   // the watch's wait, queued behind the one above
		in->unwatch();
		Read r;
		start_read(p, in, r, source.token());
		p.settle();
		check(!r.done, "[U] the reader waits on the pending read");
		p.send("x");
		run_until(p, r.done);
		check(took, "[U] the wake's byte is taken before the watch's handler runs");
		check(r.done && r.error == asio::error::operation_aborted, "[U] a cancel between the wake and its handler ends the read with operation_aborted");
		if (r.done) {   // a reader still waiting holds the input: no second reader may start
			const std::string data = pattern(3000, 9);
			p.send(data);
			check(p.arrived(3000), "[U] 3000 bytes sent after that cancel");
			std::string got;
			while (got.size() < data.size()) { auto n = read_once(p, in, 65536); if (!n.done || n.error) { break; } got += n.data; }
			check(got == data, "[U] and the next reads return them all, in order, after that cancel");
		}
	}
	{   // a token requested before the read waits ends it too
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {}, true);
		in->engage();
		in->unwatch();
		task::cancel_source source;
		source.request();
		Read r;
		start_read(p, in, r, source.token());
		run_until(p, r.done);
		check(r.done && r.error == asio::error::operation_aborted, "[U] a read whose token is already requested ends with operation_aborted");
	}
	{   // drain() waits for the pending read, which the socket's close completes
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {}, true);
		in->engage();
		bool drained = false;
		task::spawn(task::scheduler_ref::of(p.scheduler), drain_into(in, drained));
		p.settle();
		check(!drained, "[U] drain() waits while the watch's read is pending");
		asio::error_code ignored;
		p.server.close(ignored);
		run_until(p, drained);
		check(drained, "[U] and returns once the closed socket completes it");
	}
}

// The waiter's lane [H]: a heavy reader's read is resumed through its scheduler, in the heavy
// lane; a light reader's inline in the watch's handler, as before.
static task::task<void> read_lane(std::shared_ptr<Input> in, int& depth, bool& heavy, bool& done) {
	std::string buf(65536, '\0');
	co_await in->read(asio::buffer(buf), {});
	depth = task::detail::inline_resume_depth;
	heavy = task::current().heavy();
	done = true;
}
void lane_tests() {
	for (const bool heavy_reader : {false, true}) {
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {}, /*lazy=*/true);
		in->engage();
		in->unwatch();
		int depth = -1;
		bool heavy = false, done = false;
		const auto ref = task::scheduler_ref::of(p.scheduler);
		task::spawn(heavy_reader ? ref.with_heavy(true) : ref, read_lane(in, depth, heavy, done));
		p.settle();
		check(!done, std::string("[H] a ") + (heavy_reader ? "heavy" : "light") + " reader waits on the pending read");
		p.send(pattern(100, 3));
		p.io.restart();
		for (auto end = clk::now() + 2s; !done && clk::now() < end;) { p.io.run_one_for(5ms); }
		check(done, "[H] the read completes");
		if (heavy_reader) { check(depth == 0 && heavy, "[H] a heavy reader's read is resumed through its scheduler, in the heavy lane"); }
		else { check(depth == 1 && !heavy, "[H] a light reader's read is resumed inline in the watch's handler"); }
	}
}

#endif  // READAHEAD_TEST_INTEGRATION_ONLY
