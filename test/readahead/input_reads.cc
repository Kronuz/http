// Read-ahead test [U] sections on read() and read_ahead(): the bound, the order and the disconnect rules.

#include "input.h"

#ifndef READAHEAD_TEST_INTEGRATION_ONLY

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
