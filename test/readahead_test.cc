// readahead_test -- the connection's disconnect watch reads ahead (ConnectionInput) and the
// reply is written in slices that span its buffers (write_with_progress). Both change how bytes
// move between the kernel and the parser, so every semantic that rides on those bytes is pinned
// here over real TCP:
//   [U] ConnectionInput on its own: the watch holds at most kReadAhead bytes outside the kernel,
//       hands them back in order whatever the output size, arms nothing until they are consumed,
//       and keeps the disconnect rules (a reset fires; EOF, a half-close, and data end the watch)
//   [L] the lazy watch: it reads nothing until the handler asks (CancelWatch::engage), and the
//       first question decides what happened meanwhile as the read would have; the Request hook
//       engages it once, from the first cancel_token(); a read that finds the watch's read
//       pending is resumed by it, its token ends the wait, and drain() waits for it
//   [H] a heavy reader's read is resumed through its scheduler, a light one's inline
//   [Q] the same over real TCP: a reset before the first question is seen by it, a half-close is not, pipelined input
//       before it ends the watch, a handler that never asks has its pipelined successors answered
//   [N] a buffered reply sent at once as far as the socket takes it: replies to 4 MiB arrive whole
//   [P] a pipelined request that arrives inside the read-ahead (while the handler of the one
//       before runs) is parsed and answered, at every alignment of the 4096-byte window against
//       the request line, the headers, the CRLF CRLF, the body and the next request
//   [F] a flood of pipelined requests is answered in order, none lost or repeated
//   [D] disconnect: the watch's data ends it for that request; a half-close after the pipelined
//       requests cancels none of them, all are answered in order and no read follows the EOF; a
//       reset is seen by the request in flight
//   [B] a client that disconnects in the middle of a buffered body never reaches the handler,
//       and the request it held is released
//   [S] request smuggling: ambiguous framing and a request hidden in a body are never executed
//       as a request, as the first request and as one that arrives inside the read-ahead; trailer
//       fields never become headers; nothing after a CONNECT, or after an HTTP/1.0 message with
//       Transfer-Encoding, is read as a request
//   [W] the reply's inactivity slices: every byte arrives, at every alignment of head and body
//       against the 64 KiB slice
//   [A] read_ahead(): the idle read goes through the watch's own buffer, appends at most
//       kReadAhead bytes, and a token requested before it starts arms nothing
//   [I] idle: the body scratch released at each idle wait comes back for the next body, after a
//       long idle, for headers split across reads and headers larger than the read-ahead
#include "readahead/support.h"

int main() {
	std::signal(SIGPIPE, SIG_IGN);
	std::printf("== read-ahead watch and sliced write tests ==\n");
#ifndef READAHEAD_TEST_INTEGRATION_ONLY
	input_tests();
	read_ahead_tests();
#endif

	// [I] The body scratch is released at each idle wait and comes back for the next body.
	{
		Server s;
		s.start();
		Conn c(s.port);
		const std::string big = pattern(300000, 2), other = pattern(200000, 6);
		c.send(post("/echo", big));
		check(c.read_response().body == big, "[I] a 300000-byte body is read through the scratch");
		std::this_thread::sleep_for(1500ms);
		c.send(get("/fast"));
		check(c.read_response().body == "fast", "[I] a request after a long idle is served");
		c.send(post("/echo", other));
		check(c.read_response().body == other, "[I] and a body after it gets the scratch back");
		const std::string head = "POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(6000, 'p') + "\r\nContent-Length: 5\r\n\r\n";
		c.send(head.substr(0, 10));
		std::this_thread::sleep_for(50ms);
		c.send(head.substr(10, 5000));
		std::this_thread::sleep_for(50ms);
		c.send(head.substr(5010) + "hello");
		check(c.read_response().body == "hello", "[I] a 6 KiB head split across three reads, larger than the read-ahead, is parsed whole");
		std::this_thread::sleep_for(1500ms);
		c.send(post("/echo", big) + get("/fast"));
		const auto r1 = c.read_response(), r2 = c.read_response();
		check(r1.body == big && r2.body == "fast", "[I] after another idle, a body and a request pipelined behind it are answered in order");
	}

	// [P] A pipelined request inside the read-ahead, at every alignment of the 4096-byte window.
	{
		Server s; s.start();
		int n = 0, ok = 0;
		const auto run = [&](const std::string& second, const std::string& expect_body, const std::string& what) {
			const std::string id = "p" + std::to_string(++n);
			Held h(s, id);
			const std::string third_body = pattern(40, 9);
			h.c.send(second + post("/echo", third_body, "Connection: close\r\n"));
			std::this_thread::sleep_for(30ms);   // the watch took the first 4096 bytes of it
			h.o->release = true;
			auto r1 = h.c.read_response(), r2 = h.c.read_response(), r3 = h.c.read_response();
			const bool good = r1.status == 200 && r1.body == "held" && r2.status == 200 && r2.body == expect_body &&
				r3.status == 200 && r3.body == third_body;
			if (good) { ++ok; } else { check(false, "[P] " + what + ": r1=" + std::to_string(r1.status) + " r2=" + std::to_string(r2.status) + "/" + std::to_string(r2.body.size()) + "B r3=" + std::to_string(r3.status)); }
		};
		const std::string body = pattern(300, 3);
		const std::string skeleton_a = "POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: \r\nContent-Length: 300\r\n\r\n";
		// The head's CRLF CRLF straddles the window: head lengths 4096 + d, d from -6 to +3.
		for (int d = -6; d <= 3; ++d) {
			const std::size_t pad = 4096 + d - skeleton_a.size();
			run("POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(pad, 'p') + "\r\nContent-Length: 300\r\n\r\n" + body, body, "head ends at window" + std::string(d >= 0 ? "+" : "") + std::to_string(d));
		}
		// The window ends inside the body.
		for (std::size_t cut : {1u, 2u, 17u, 150u, 299u}) {
			const std::size_t head = 4096 - cut;
			const std::string skeleton = "POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: \r\nContent-Length: 300\r\n\r\n";
			const std::size_t pad = head - skeleton.size();
			run("POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(pad, 'p') + "\r\nContent-Length: 300\r\n\r\n" + body, body, "window ends " + std::to_string(cut) + " bytes into the body");
		}
		// The window ends at, or one either side of, the end of the second request.
		for (int d = -2; d <= 2; ++d) {
			const std::string skeleton = "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0000\r\n\r\n";
			const std::size_t blen = 4096 + d - skeleton.size();
			const std::string b = pattern(blen, 4);
			run("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(blen) + "\r\n\r\n" + b, b, "request ends at window" + std::string(d >= 0 ? "+" : "") + std::to_string(d));
		}
		check(ok == n, "[P] " + std::to_string(ok) + " of " + std::to_string(n) + " alignments: the pipelined request inside the read-ahead is answered in order, intact");
		check(s.app.hit_count("/echo") == 2 * n, "[P] the handler ran exactly two echoes per connection (no request repeated or dropped)");
	}

	// [F] A flood of pipelined requests, far more than the window, is answered in order.
	{
		Server s; s.start();
		Held h(s, "flood");
		constexpr int N = 3000;
		std::string burst;
		for (int i = 0; i < N; ++i) {
			burst += post("/echo", "n" + std::to_string(i), i == N - 1 ? "Connection: close\r\n" : "");
		}
		std::thread sender([&] { h.c.send(burst); });   // the kernel's buffers bound what the server holds
		std::this_thread::sleep_for(50ms);
		h.o->release = true;
		auto first = h.c.read_response();
		bool ordered = first.body == "held";
		int got = 0;
		for (int i = 0; i < N && ordered; ++i) {
			auto r = h.c.read_response();
			ordered = r.complete && r.status == 200 && r.body == "n" + std::to_string(i);
			if (ordered) { ++got; }
		}
		sender.join();
		check(ordered && got == N, "[F] " + std::to_string(N) + " pipelined requests (" + std::to_string(burst.size() / 1024) + " KiB) behind a held one are answered in order, none lost or repeated");
		check(s.app.hit_count("/echo") == N, "[F] each ran exactly once");
	}

	// [D] The watch's data ends it; a half-close after the pipelined request is that request's to see.
	{
		Server s; s.start();
		Held h(s, "d1");
		auto o2 = s.app.get_obs("d2");
		h.c.send(get("/hold?id=d2"));   // arrives inside the read-ahead
		std::this_thread::sleep_for(30ms);
		h.c.shut_wr();
		std::this_thread::sleep_for(30ms);
		check(h.o->reason == -1, "[D] the first request is not cancelled by a half-close that came after pipelined input");
		h.o->release = true;
		auto r1 = h.c.read_response();
		until([&] { return o2->started.load(); });
		std::this_thread::sleep_for(30ms);   // its watch has read the EOF
		check(o2->reason == -1, "[D] the pipelined request's own watch reads the half-close and does not cancel it");
		o2->release = true;
		auto r2 = h.c.read_response();
		check(r1.body == "held" && r2.status == 200 && r2.body == "held", "[D] both are answered, the half-closed client included");
		check(h.c.ends_silently(), "[D] and then the connection closes");
	}
	{   // a half-close behind three pipelined requests, the last two already read: all answered in order
		Server s; s.start();
		Held h(s, "d4");
		auto o5 = s.app.get_obs("d5");
		h.c.send(get("/hold?id=d5") + get("/echo") + post("/echo", "abc"));
		h.c.shut_wr();
		std::this_thread::sleep_for(30ms);
		h.o->release = true;
		until([&] { return o5->started.load(); });
		std::this_thread::sleep_for(30ms);
		o5->release = true;
		auto r1 = h.c.read_response(), r2 = h.c.read_response(), r3 = h.c.read_response(), r4 = h.c.read_response();
		check(h.o->reason == -1 && o5->reason == -1, "[D] a half-close after pipelined requests cancels none of them");
		check(r1.body == "held" && r2.body == "held" && r3.status == 200 && r3.body.empty() && r4.body == "abc",
			"[D] the requests read before the half-close are all answered, in order");
		check(h.c.ends_silently(), "[D] and then the connection closes");
	}
	{   // without pipelined input the half-close is the first request's: it is not cancelled
		Server s; s.start();
		Held h(s, "d3");
		h.c.shut_wr();
		std::this_thread::sleep_for(30ms);
		check(h.o->reason == -1, "[D] a half-close with nothing pipelined does not cancel the request in flight");
		h.o->release = true;
		check(h.c.read_response().body == "held" && h.c.ends_silently(), "[D] its response is written, then the connection closes");
	}
	{   // a reset is a disconnect
		Server s; s.start();
		Held h(s, "d6");
		h.c.rst();
		until([&] { return h.o->reason.load() != -1; }, 1000);
		check(h.o->reason == static_cast<int>(http::disconnected), "[D] a reset with nothing pipelined cancels the request in flight");
		h.o->release = true;
	}

	// [B] A client that goes away in the middle of a buffered body never reaches the handler.
	{
		Server s; s.start();
		const auto head = [](const std::string& path) { return "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n"; };
		{ Conn c(s.port); c.send(head("/echo") + std::string(1000, 'a')); std::this_thread::sleep_for(60ms); c.close(); }
		{ Conn c(s.port); c.send(head("/echo") + std::string(1000, 'a')); std::this_thread::sleep_for(60ms); c.shut_wr(); check(c.ends_silently(), "[B] a half-close mid-body ends the connection with no response"); }
		{ Conn c(s.port); c.send(head("/echo") + std::string(5000, 'a')); std::this_thread::sleep_for(60ms); linger l{1, 0}; ::setsockopt(c.fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); c.close(); }
		{   // chunked, cut after a chunk
			Conn c(s.port); c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n"); std::this_thread::sleep_for(60ms); c.close();
		}
		check(until([&] { return s.inflight() == 0; }), "[B] every cut-short request released its admission");
		check(s.app.hit_count("/echo") == 0, "[B] the handler never ran for a body that did not complete");
		check(Conn(s.port).fd >= 0 && [&] { Conn c(s.port); c.send(get("/fast", "Connection: close\r\n")); return c.read_response().body == "fast"; }(), "[B] the server keeps serving");
	}

	// [S] Request smuggling: ambiguous framing is never executed, and a request inside a body is data.
	{
		Server s; s.start();
		const std::string hidden = get("/smuggled");
		struct Case { const char* what; std::string request; };
		const std::vector<Case> bad = {
			{"Content-Length with Transfer-Encoding: chunked",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(5 + hidden.size()) + "\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" + hidden},
			{"two different Content-Lengths",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: " + std::to_string(5 + hidden.size()) + "\r\n\r\nhello" + hidden},
			{"a space before the colon of Content-Length",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length : 5\r\n\r\nhello" + hidden},
			{"a signed Content-Length",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\nhello" + hidden},
			{"a Content-Length with trailing junk",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5 5\r\n\r\nhello" + hidden},
			{"a chunk size that is not hex",
				"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhello\r\n0\r\n\r\n" + hidden},
		};
		int rejected_first = 0, rejected_inside = 0;
		for (const auto& c : bad) {
			{   // as the first request on its connection
				Conn k(s.port);
				k.send(c.request);
				auto r = k.read_response();
				if (r.status != 200 && s.app.hit_count("/smuggled") == 0) { ++rejected_first; }
				else { check(false, std::string("[S] ") + c.what + " (first): status " + std::to_string(r.status) + ", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x"); }
			}
			{   // arriving inside the read-ahead, behind a request still running
				static int n = 0;
				Held h(s, "s" + std::to_string(++n));
				h.c.send(c.request);
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response();
				auto r2 = h.c.read_response();
				const bool good = r1.body == "held" && !r2.complete && s.app.hit_count("/smuggled") == 0;
				if (good) { ++rejected_inside; }
				else { check(false, std::string("[S] ") + c.what + " (inside the read-ahead): second response " + std::to_string(r2.status) + ", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x"); }
			}
		}
		check(rejected_first == static_cast<int>(bad.size()), "[S] " + std::to_string(rejected_first) + " of " + std::to_string(bad.size()) + " ambiguous requests were refused as the first request, none ran /smuggled");
		check(rejected_inside == static_cast<int>(bad.size()), "[S] " + std::to_string(rejected_inside) + " of " + std::to_string(bad.size()) + " arriving inside the read-ahead were refused after the held request's response, none ran /smuggled");
		// A request in a body is body, whether the body fits the window or crosses it.
		for (std::size_t pad : {0u, 3000u, 6000u}) {
			const std::string inner = std::string(pad, 'x') + hidden + std::string(pad / 2, 'y');
			{
				Held h(s, "inner" + std::to_string(pad));
				h.c.send(post("/echo", inner));
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response(), r2 = h.c.read_response();
				check(r1.body == "held" && r2.body == inner && s.app.hit_count("/smuggled") == 0,
					"[S] a request inside a Content-Length body of " + std::to_string(inner.size()) + " bytes is data, answered once, never run");
			}
			{
				const std::string chunked = "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";
				char size[16]; std::snprintf(size, sizeof size, "%zx", inner.size());
				Held h(s, "chunk" + std::to_string(pad));
				h.c.send(chunked + size + "\r\n" + inner + "\r\n0\r\n\r\n");
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response(), r2 = h.c.read_response();
				check(r1.body == "held" && r2.body == inner && s.app.hit_count("/smuggled") == 0,
					"[S] a request inside a chunk of " + std::to_string(inner.size()) + " bytes is data, answered once, never run");
			}
		}
	}

	// [S] The trailer section of a chunked body: a front end that sets identity headers in the
	// header section cannot be overridden from the trailers (RFC 9110 section 6.5.1).
	{
		Server s; s.start();
		const std::vector<std::pair<std::string, std::string>> cases = {
			{"X-User: admin\r\nX-Forwarded-For: 6.6.6.6\r\nX-Pad: 1\r\n", "X-User:admin,X-Forwarded-For:6.6.6.6,X-Pad:1,"},
			{"X-User: admin\r\nX-Forwarded-For: 6.6.6.6\r\n", "X-User:admin,X-Forwarded-For:6.6.6.6,"},
			{"X-User: admin\r\n", "X-User:admin,"},
		};
		for (const auto& [trailers, want] : cases) {
			Conn k(s.port);
			k.send("POST /who HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n" + trailers + "\r\n");
			auto r = k.read_response();
			check(r.status == 200 && r.body == "x-user=;xff=;headers=3;trailers=" + want, "[S] trailer fields are trailers, never headers: " + r.body);
		}
	}
	// [S] CONNECT opens no tunnel, and has no content (RFC 9110 section 9.3.6): it is answered,
	// the connection closes, and nothing after its header section runs as a request, whatever
	// framing it named. And after an HTTP/1.0 message with Transfer-Encoding the connection
	// closes too (RFC 9112 section 6.1).
	{
		Server s; s.start();
		const std::string hidden = get("/smuggled");
		const std::vector<std::pair<std::string, std::string>> cases = {
			{"CONNECT with Content-Length", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\nContent-Length: " + std::to_string(hidden.size()) + "\r\n\r\n" + hidden},
			{"CONNECT with Transfer-Encoding", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\nTransfer-Encoding: chunked\r\n\r\n" + hidden},
			{"CONNECT", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\n\r\n" + hidden},
			{"HTTP/1.0 with Transfer-Encoding and keep-alive",
				"POST /echo HTTP/1.0\r\nHost: x\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n" + hidden},
		};
		for (const auto& [what, request] : cases) {
			Conn k(s.port);
			k.send(request);
			auto r = k.read_response();
			const bool closed = k.ends_silently();
			check(r.complete && closed && s.app.hit_count("/smuggled") == 0,
				"[S] " + what + ": answered " + std::to_string(r.status) + ", then the connection " + (closed ? "closes" : "stays open") +
				", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x");
		}
	}

	// [Q] The watch starts when the handler first asks about cancellation, and the first question
	// sees what happened before it: a hang-up is a disconnect (polled or by callback, which runs
	// as it is registered), pipelined input ends the watch, and a handler that never asks still has
	// the requests pipelined behind it answered in order.
	{
		Server s;
		s.start();
		for (const std::string ask : {"poll", "cb"}) {
			Conn c(s.port);
			const std::string id = "q-" + ask;
			auto o = s.app.get_obs(id);
			c.send(get("/lazy?id=" + id + "&ask=" + ask));
			until([&] { return o->started.load(); });
			c.shut_wr();
			std::this_thread::sleep_for(30ms);
			o->release = true;
			until([&] { return o->finished.load(); });
			check(o->reason == (ask == "poll" ? 0 : -1), "[Q] " + ask + ": a half-close before the first question is not a disconnect at that question");
			check(c.read_response().body == "lazy" && c.ends_silently(), "[Q] " + ask + ": the response is written, then the connection closes");
		}
		for (const std::string ask : {"poll", "cb"}) {
			Conn c(s.port);
			const std::string id = "q-rst-" + ask;
			auto o = s.app.get_obs(id);
			c.send(get("/lazy?id=" + id + "&ask=" + ask));
			until([&] { return o->started.load(); });
			c.rst();
			std::this_thread::sleep_for(30ms);
			o->release = true;
			until([&] { return o->finished.load(); });
			check(o->reason == static_cast<int>(http::disconnected), "[Q] " + ask + ": a reset before the first question is a disconnect at that question");
			if (ask == "cb") { check(o->at_once.load(), "[Q] cb: and the callback ran as it was registered"); }
		}
		{
			Conn c(s.port);
			auto o = s.app.get_obs("q-pipe");
			c.send(get("/lazy?id=q-pipe&ask=poll"));
			until([&] { return o->started.load(); });
			c.send(get("/fast"));
			c.shut_wr();
			std::this_thread::sleep_for(30ms);
			o->release = true;
			until([&] { return o->finished.load(); });
			check(o->reason == 0, "[Q] input pipelined before the first question ends the watch: the half-close after it is not this request's");
			auto r1 = c.read_response(), r2 = c.read_response();
			check(r1.body == "lazy" && r2.body == "fast" && c.ends_silently(), "[Q] both are answered in order, then the connection closes");
		}
		{
			Conn c(s.port);
			auto o = s.app.get_obs("q-none");
			c.send(get("/lazy?id=q-none&ask=none") + get("/echo") + post("/echo", "abc"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(20ms);
			o->release = true;
			auto r1 = c.read_response(), r2 = c.read_response(), r3 = c.read_response();
			check(r1.body == "lazy" && r2.status == 200 && r2.body.empty() && r3.body == "abc", "[Q] a handler that never asks: the requests pipelined behind it are answered in order");
		}
	}

	// [N] A buffered reply leaves at once as far as the socket takes it, and the rest is written
	// after: with no timeouts, replies far larger than the send buffer reach a client that reads
	// late byte for byte, on one keep-alive connection.
	{
		Server s;
		s.start();
		Conn c(s.port);
		int ok = 0, n = 0;
		for (std::size_t size : {1u, 1000u, 65536u, 300000u, 1u << 20, 4u << 20, 1000u}) {
			c.send(get("/body?n=" + std::to_string(size)));
			std::this_thread::sleep_for(30ms);   // the send buffer fills before the client reads
			auto r = c.read_response();
			++n;
			if (r.complete && r.body == pattern(size, 5)) { ++ok; }
			else { check(false, "[N] reply of " + std::to_string(size) + " bytes arrived as " + std::to_string(r.body.size())); }
		}
		check(ok == n, "[N] " + std::to_string(ok) + " of " + std::to_string(n) + " replies from 1 byte to 4 MiB arrive byte for byte to a client that reads late");
	}

	// [W] The reply goes out in 64 KiB slices of head and body together; every byte arrives.
	{
		Server s(http::Limits{.max_in_flight = 64, .inactivity_timeout = 2000ms});
		s.start();
		std::size_t head = 0;
		{   // the head's length for a five-digit body
			Conn c(s.port);
			c.send(get("/body?n=65000", "Connection: close\r\n"));
			auto r = c.read_response();
			head = r.head.size() + 4;
			check(r.body == pattern(65000, 5), "[W] a 65000 byte reply arrives intact (head " + std::to_string(head) + " bytes)");
		}
		int ok = 0, n = 0;
		for (std::size_t total : {65536u - 1, 65536u, 65536u + 1, 131072u - 1, 131072u, 131072u + 1}) {
			for (int extra : {-1, 0, 1}) {   // the head and the body together land at, and either side of, a slice end
				const std::size_t size = total - head + extra;
				Conn c(s.port, 2);
				c.send(get("/body?n=" + std::to_string(size), "Connection: close\r\n"));
				auto r = c.read_response();
				++n;
				if (r.complete && r.body == pattern(size, 5)) { ++ok; }
				else { check(false, "[W] reply of " + std::to_string(size) + " bytes arrived as " + std::to_string(r.body.size())); }
			}
		}
		check(ok == n, "[W] " + std::to_string(ok) + " of " + std::to_string(n) + " replies around the slice ends arrive byte for byte");
		Conn k(s.port, 2);
		int keep = 0;
		for (int i = 0; i < 50 && keep == i; ++i) {   // and keep-alive carries on after sliced replies
			k.send(get("/body?n=70000"));
			if (k.read_response().body == pattern(70000, 5)) { ++keep; }
		}
		check(keep == 50, "[W] 50 sliced replies in a row on one keep-alive connection");
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
