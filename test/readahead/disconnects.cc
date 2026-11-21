// Read-ahead test sections on the watch: a hang-up, a body cut short, and the lazy first question.

#include "sections.h"

void section_D() {
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
}

void section_B() {
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
}

void section_Q() {
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
}
