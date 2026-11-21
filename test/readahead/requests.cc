// Read-ahead test sections on requests: the idle scratch, pipelining, smuggling, buffered and sliced replies.

#include "sections.h"

void section_I() {
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
}

void section_P() {
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
}

void section_F() {
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
}

void section_S() {
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
}

void section_N() {
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
}

void section_W() {
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
}
