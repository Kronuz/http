// Read-ahead test [U] sections: ConnectionInput on its own over a socket pair.

#include "support.h"

// ---- [U] ConnectionInput on its own ----
// (-DREADAHEAD_TEST_INTEGRATION_ONLY builds [P]..[W] without this section, so they also run
// against a header that has no read-ahead: they pin the behavior the watch must keep.)
#ifndef READAHEAD_TEST_INTEGRATION_ONLY
using Socket = asio::ip::tcp::socket;
using Input = http::detail::ConnectionInput<Socket>;
static int pending_bytes(int fd) { int n = 0; ::ioctl(fd, FIONREAD, &n); return n; }

struct Pair {
	asio::io_context io;
	Socket server{io};
	task::asio_scheduler scheduler{io.get_executor()};   // after io and server: destroyed first
	int client = -1;
	Pair() {
		asio::ip::tcp::acceptor acceptor(io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
		client = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(acceptor.local_endpoint().port());
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		::connect(client, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		acceptor.accept(server);
	}
	~Pair() { io.stop(); if (client >= 0) { ::close(client); } }   // a stopped loop: the scheduler may destroy a read left pending
	void send(const std::string& s) { std::size_t o = 0; while (o < s.size()) { ssize_t n = ::send(client, s.data() + o, s.size() - o, 0); if (n <= 0) { break; } o += static_cast<std::size_t>(n); } }
	void settle() { io.restart(); io.run_for(150ms); }   // the loop runs what is ready, then idles out
	// Waits until `n` bytes sit in the server socket's receive queue.
	bool arrived(int n) { return until([&] { return pending_bytes(server.native_handle()) >= n; }); }
};
struct Read { std::string data; asio::error_code error; bool done = false; };
// One read(output) of up to `max` bytes, driven on the pair's loop.
static Read read_once(Pair& p, const std::shared_ptr<Input>& in, std::size_t max) {
	Read r;
	auto reader = [&, in]() -> task::task<void> {
		std::string buf(max, '\0');
		const http::detail::ReadResult got = co_await in->read(asio::buffer(buf), {});
		if (got.error) { r.error = got.error; } else { r.data.assign(buf, 0, got.bytes); }
		r.done = true;
	};
	task::spawn(task::scheduler_ref::of(p.scheduler), reader());
	p.io.restart();
	for (auto end = clk::now() + 2s; !r.done && clk::now() < end;) { p.io.run_one_for(5ms); }
	return r;
}

// The lazy watch (a plain TCP socket): it reads nothing until engage(), and engage() decides
// what happened meanwhile as the read would have.
static void lazy_watch_tests() {
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
static void held_read_tests() {
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
static void lane_tests() {
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

void read_ahead_tests() {
	{   // appends what the watch's read took, at most kReadAhead, to what the string held
		Pair p;
		const std::string data = pattern(10000, 11);
		p.send(data);
		check(p.arrived(10000), "[A] the 10000 bytes reached the server's receive queue");
		auto in = std::make_shared<Input>(p.server);
		std::string out = "x";
		bool done = false;
		http::detail::ReadResult got{};
		auto reader = [&, in]() -> task::task<void> {
			got = co_await in->read_ahead(out, {});
			done = true;
		};
		task::spawn(task::scheduler_ref::of(p.scheduler), reader());
		p.io.restart();
		for (auto end = clk::now() + 2s; !done && clk::now() < end;) { p.io.run_one_for(5ms); }
		check(done && !got.error && got.bytes == Input::kReadAhead && out == "x" + data.substr(0, Input::kReadAhead),
			"[A] read_ahead() appends the first kReadAhead bytes behind what the string held");
		check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead), "[A] and leaves the rest in the kernel");
		auto b = read_once(p, in, 65536);
		check(b.done && !b.error && b.data == data.substr(Input::kReadAhead), "[A] the next read() continues without a gap or a repeat");
	}
	{   // a token requested before it starts: operation_aborted, nothing armed or read
		Pair p;
		p.send("abc");
		check(p.arrived(3), "[A] 3 bytes arrived");
		auto in = std::make_shared<Input>(p.server);
		task::cancel_source cancelled;
		cancelled.request();
		std::string out;
		bool done = false;
		http::detail::ReadResult got{};
		auto reader = [&, in]() -> task::task<void> {
			got = co_await in->read_ahead(out, cancelled.token());
			done = true;
		};
		task::spawn(task::scheduler_ref::of(p.scheduler), reader());
		p.settle();
		check(done && got.error == asio::error::operation_aborted && out.empty() && pending_bytes(p.server.native_handle()) == 3,
			"[A] a requested token ends read_ahead() with operation_aborted and reads nothing");
		auto r = read_once(p, in, 100);
		check(r.done && r.data == "abc", "[A] the bytes stay for the next read");
	}
}

void input_tests() {
	// Bounded and ordered: 10000 bytes wait in the kernel; the watch takes 4096 of them and no more.
	{
		Pair p;
		const std::string data = pattern(10000);
		p.send(data);
		check(p.arrived(10000), "[U] the 10000 bytes reached the server's receive queue");
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		const int left = pending_bytes(p.server.native_handle());
		check(left == 10000 - static_cast<int>(Input::kReadAhead), "[U] the watch read ahead exactly kReadAhead bytes (" + std::to_string(left) + " left in the kernel)");
		check(fired == 0, "[U] data ends the watch without a disconnect");
		in->watch([&] { ++fired; });   // nothing more is armed while the bytes wait
		p.settle();
		check(pending_bytes(p.server.native_handle()) == left && fired == 0, "[U] no further read-ahead is armed until the bytes are consumed");
		auto a = read_once(p, in, 65536);
		check(a.done && !a.error && a.data == data.substr(0, 4096), "[U] read() returns the read-ahead bytes first, in order");
		auto b = read_once(p, in, 65536);
		check(b.done && !b.error && b.data == data.substr(4096), "[U] the next read() continues from the socket without a gap or a repeat");
	}
	// A small output takes the read-ahead in pieces, in order, and the watch reads nothing new meanwhile.
	{
		Pair p;
		const std::string first = pattern(3000, 1), more = pattern(500, 2);
		p.send(first);
		check(p.arrived(3000), "[U] 3000 bytes arrived");
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {});
		p.settle();
		p.send(more);
		check(p.arrived(500), "[U] 500 more bytes arrived behind the read-ahead");
		std::string got;
		bool whole = true;
		for (int i = 0; i < 4; ++i) {
			in->watch([] {});   // re-arming must not read while read-ahead bytes remain
			p.settle();
			if (i < 3) { whole = whole && pending_bytes(p.server.native_handle()) == 500; }
			auto r = read_once(p, in, 1000);
			if (!r.done || r.error) { whole = false; break; }
			got += r.data;
			whole = whole && r.data.size() == (i < 3 ? 1000u : 500u);
		}
		check(whole, "[U] three reads of 1000 take the 3000 read-ahead bytes while the 500 behind stay in the kernel; the fourth reads them");
		check(got == first + more, "[U] the pieces are the stream, in order (" + std::to_string(got.size()) + " bytes)");
	}
	// EOF with nothing read (a half-close) ends the watch and fires nothing; a reset fires once;
	// data then EOF does not fire, and the EOF is read afterwards.
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[U] EOF with nothing read is a half-close: the watch fires nothing");
		in->watch([&] { ++fired; });   // a watch set after the EOF: no disconnect, and no new read
		check(fired == 0 && in->drain().await_ready(), "[U] a watch set after the EOF fires nothing and starts no read");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[U] and the EOF is read afterwards");
	}
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
		p.settle();
		check(fired == 1, "[U] a reset with nothing read fires the disconnect once");
		in->watch([&] { ++fired; });
		check(fired == 2, "[U] and a watch set after the reset fires at once");
	}
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		p.send("abc");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[U] data followed by EOF: the data ended the watch, so the EOF is not a disconnect");
		auto a = read_once(p, in, 100);
		check(a.done && !a.error && a.data == "abc", "[U] the data is read");
		auto b = read_once(p, in, 100);
		check(b.done && b.error == asio::error::eof, "[U] then the EOF");
	}
	// A watch set after the data arrived is ended by it at once.
	{
		Pair p;
		p.send("xyz");
		check(p.arrived(3), "[U] 3 bytes arrived");
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {});
		p.settle();
		int fired = 0;
		in->watch([&] { ++fired; });   // ready_ with data: no disconnect, and no new read
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[U] a watch set while read-ahead bytes wait never fires");
		auto a = read_once(p, in, 100);
		check(a.done && a.data == "xyz", "[U] the bytes are still delivered");
	}
	// The reply monitor (an owned-stream reply waits) uses the same read-ahead: it never
	// reads over bytes that wait, a half-close is not a disconnect, and a reset is.
	{
		const auto make = [](Pair& p) {
			http::StreamOptions options;
			options.reply.deadline = clk::now() + 30s;
			return http::detail::AsioOwnedStreamControl::create(p.io.get_executor(), std::move(options));
		};
		{
			Pair p;
			const std::string data = pattern(10000);
			p.send(data);
			check(p.arrived(10000), "[U] the monitor: 10000 bytes reached the server's receive queue");
			auto in = std::make_shared<Input>(p.server);
			in->watch([] {});
			p.settle();
			auto control = make(p);
			in->monitor(control);   // read-ahead bytes wait: nothing may be read over them
			p.settle();
			check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead), "[U] the monitor arms nothing while read-ahead bytes wait");
			auto a = read_once(p, in, 65536), b = read_once(p, in, 65536);
			check(a.data + b.data == data, "[U] and the stream is intact");
			control->disable();
		}
		{   // the watch ended with the data: after it is consumed, a monitor read that sees EOF does not fire the old watch
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			int fired = 0;
			in->watch([&] { ++fired; });
			p.settle();
			p.send("abc");
			p.settle();
			auto a = read_once(p, in, 100);
			auto control = make(p);
			in->monitor(control);
			p.settle();
			::shutdown(p.client, SHUT_WR);
			p.settle();
			check(a.data == "abc" && fired == 0, "[U] a watch that data ended stays ended: a later EOF does not call it");
			check(control->exchange->phase() == http::detail::OwnedStreamExchange::Phase::Waiting, "[U] the monitor does not treat EOF as a disconnect (a half-close still expects its reply)");
			control->disable();
		}
		{   // a reset is one
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			auto control = make(p);
			in->monitor(control);
			p.settle();
			linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
			p.settle();
			check(control->exchange->phase() != http::detail::OwnedStreamExchange::Phase::Waiting, "[U] the monitor treats a reset as a disconnect");
			control->disable();
		}
	}
	// However the sends, the watches and the reads interleave, the reads are the stream: a
	// seeded random walk over sends of 1..2000 bytes, watch calls and reads of 1..3000 bytes.
	{
		int bad = 0, total = 0;
		for (unsigned seed = 1; seed <= 40; ++seed) {
			std::uint32_t state = seed * 2654435761u;
			const auto rnd = [&](std::uint32_t n) { state = state * 1664525u + 1013904223u; return (state >> 8) % n; };
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			std::string sent, got;
			for (int step = 0; step < 60; ++step) {
				switch (rnd(3)) {
					case 0: if (sent.size() - got.size() < 60000) { auto chunk = pattern(1 + rnd(2000), static_cast<unsigned>(sent.size())); p.send(chunk); sent += chunk; } break;
					case 1: in->watch([] {}); p.io.restart(); p.io.poll(); break;
					default:
						if (got.size() < sent.size()) { auto r = read_once(p, in, 1 + rnd(3000)); if (!r.done || r.error) { ++bad; } got += r.data; }
				}
			}
			while (got.size() < sent.size() && bad == 0) { auto r = read_once(p, in, 65536); if (!r.done || r.error) { ++bad; break; } got += r.data; }
			++total;
			if (got != sent) { ++bad; std::printf("    seed %u: %zu of %zu bytes, stream %s\n", seed, got.size(), sent.size(), got == sent.substr(0, got.size()) ? "a prefix" : "corrupt"); }
		}
		check(bad == 0, "[U] " + std::to_string(total) + " random walks of sends, watches and reads return the stream byte for byte");
	}
	lazy_watch_tests();
	held_read_tests();
	lane_tests();
}
#endif  // READAHEAD_TEST_INTEGRATION_ONLY
