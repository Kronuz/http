// Coroutine test sections on hang-ups: reset, half-close, a body cut short, a throwing cancel callback.

#include "sections.h"

void section_C(App& app) {
	// [C] A reset cancels reads, not writes; a half-close cancels nothing.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 256});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);

		{   // a reset: the read is cancelled early; nobody is left to answer
			auto o = app.get_obs("close");
			Conn c(port);
			c.send(get("/read?id=close&ms=3000"));
			until([&] { return o->started.load(); });
			auto t0 = clk::now();
			c.rst();
			until([&] { return o->finished.load(); }, 4000);
			std::printf("  [C] reset: cancelled after %.0f ms, finished %.0f ms after the reset\n", o->cancelled_after_ms.load(), since_ms(t0));
			check(o->reason == static_cast<int>(http::disconnected), "[C] a client reset cancels the request with Disconnect");
			check(o->finished && since_ms(t0) < 1500.0, "[C] the read stopped at its next checkpoint (not after its full 3 s)");
			check(o->frame_ok, "[C] the request frame was alive after the cancel");
		}
		{   // a write is never cancelled: it completes and its response is attempted
			auto o = app.get_obs("put");
			Conn c(port);
			c.send("PUT /write?id=put&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == -1, "[C] a half-close does not cancel the write");
			check(r.status == 200 && r.body == "write:completed" && o->frame_ok, "[C] a write completes after the client half-closed, and its response arrives");
			auto o2 = app.get_obs("put2");
			Conn c2(port);
			c2.send("PUT /write?id=put2&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o2->started.load(); });
			c2.close();
			until([&] { return o2->finished.load(); }, 3000);
			check(o2->finished.load(), "[C] a write runs to completion even when the client is gone");
		}
		{   // C17.2: a pipelined byte ends the watch for the rest of that request
			auto o = app.get_obs("pipe");
			Conn c(port);
			c.send(get("/read?id=pipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);           // the watch is reading
			c.send("G");                                 // the next request's first byte arrives while it runs
			std::this_thread::sleep_for(50ms);           // and the watch consumes it
			c.rst();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == -1, "[C] after a pipelined byte, a later reset is not seen (the documented limit)");
		}
		{   // without the byte the same close is seen (the control for the case above)
			auto o = app.get_obs("nopipe");
			Conn c(port);
			c.send(get("/read?id=nopipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.rst();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == static_cast<int>(http::disconnected), "[C] the same reset without the byte is seen");
		}
	}
}

void section_J(App& app) {
	// [J] A half-closed client keeps its response (D126); a reset or a failed write cancels.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 256});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		const auto released = [&] { return until([&] { return svc.reactor(0).gate().inflight.load() == 0; }); };
		{   // a plain handler that polls its token: the half-close does not stop it, its whole answer arrives
			auto o = app.get_obs("j-read");
			Conn c(port);
			c.send(get("/read?id=j-read&ms=300"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == -1, "[J] a half-close does not cancel a request in flight (reason " + std::to_string(o->reason.load()) + ")");
			check(r.complete && r.status == 200 && r.body == "read:completed", "[J] a half-closed client gets the full response of a handler that checks its token (" + r.body + ")");
			check(c.closed_by_peer() && released(), "[J] and then the connection closes and the request is released");
		}
		{   // a long poll held on its token, half-closed before the handler first asks: answered at its own bound
			auto o = app.get_obs("j-poll");
			Conn c(port);
			c.send(get("/poll?id=j-poll&ms=300"));
			c.shut_wr();
			auto r = c.read_response();
			until([&] { return o->started.load(); });   // orders the handler's writes before this thread's reads
			std::printf("  [J] half-closed long poll: answered after %.0f ms (bound 300 ms)\n", o->finished_after_ms.load());
			check(o->reason == -1 && r.complete && r.status == 200 && r.body == "poll:bound",
				"[J] a long poll waiting on its token, half-closed before it asked, is answered when its own bound ends it (" + r.body + ")");
			check(o->finished_after_ms.load() >= 290.0, "[J] it ran to its bound, not cut short by the half-close");
			check(c.closed_by_peer() && released(), "[J] and then the connection closes and the request is released");
			o->token.reset();
		}
		{   // a reset cancels the long poll at once
			auto o = app.get_obs("j-poll-rst");
			Conn c(port);
			c.send(get("/poll?id=j-poll-rst&ms=3000"));
			until([&] { return o->started.load(); });
			c.rst();
			until([&] { return o->finished.load(); }, 4000);
			check(o->reason == static_cast<int>(http::disconnected) && o->finished_after_ms.load() < 1500.0,
				"[J] a reset cancels a held long poll with http::disconnected before its bound");
			check(released(), "[J] and the request is released");
			o->token.reset();
		}
		{   // the peer half-closes, then leaves: nothing is seen until the poll's bound, then the write fails and cancels
			auto o = app.get_obs("j-poll-gone");
			Conn c(port);
			c.send(get("/poll?id=j-poll-gone&ms=300"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			std::this_thread::sleep_for(50ms);   // the watch read the EOF
			c.rst();
			const auto t0 = clk::now();
			check(released(), "[J] a long poll whose peer half-closed and left ends by its own bound");
			const bool cancelled = o->token && o->token->requested() && o->token->reason() == http::disconnected;
			std::printf("  [J] half-closed then gone: released %.0f ms after the reset, poll ran %.0f ms (bound 300 ms)\n", since_ms(t0), o->finished_after_ms.load());
			check(o->finished_after_ms.load() >= 290.0 && o->finished_after_ms.load() < 1500.0, "[J] nothing waits past the handler's own bound");
			check(cancelled, "[J] and the failed write cancels the request (http::disconnected)");
			o->token.reset();
		}
		{   // the same on the streamed-body path: its failed write cancels too
			auto o = app.get_obs("j-stream-big");
			Conn c(port);
			c.send(post("/stream-big-token?id=j-stream-big", "abc"));
			until([&] { return o->finished.load(); });
			check(c.fill(), "[J] streamed body: the big reply starts to arrive");
			c.rst();
			check(released(), "[J] streamed body: the big reply's failed write ends the connection");
			check(o->token && o->token->requested() && o->token->reason() == http::disconnected,
				"[J] streamed body: a failed write cancels the request: the token the handler kept sees http::disconnected");
			o->token.reset();
		}
		{   // a failed write with no half-close: a token kept past handle() sees http::disconnected
			auto o = app.get_obs("j-big");
			Conn c(port);
			c.send(get("/big-token?id=j-big"));
			until([&] { return o->finished.load(); });
			check(c.fill(), "[J] the big reply starts to arrive");
			c.rst();
			check(released(), "[J] the big reply's failed write ends the connection");
			check(o->token && o->token->requested() && o->token->reason() == http::disconnected,
				"[J] a failed write cancels the request: the token the handler kept sees http::disconnected");
			o->token.reset();
		}
	}
}

void section_H(App& app) {
	// [H] A body cut short never reads as a clean end: the reader gets an error other than eof,
	// and the request is already cancelled when its read returns.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		const auto head = [](const std::string& id) {
			return "POST /tbody?id=" + id + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(1000, 'a');
		};
		const auto outcome = [&](const std::string& id, const char* what) {
			auto o = app.get_obs(id);
			until([&] { return o->finished.load(); }, 3000);
			std::printf("  [H] %s: end_kind=%d cancelled=%d\n", what, o->end_kind.load(), int(o->cancelled_at_end.load()));
			check(o->finished && o->end_kind == 3 && o->cancelled_at_end,
				std::string("[H] ") + what + ": the reader gets an error (not eof) and the request is cancelled");
		};
		{ Conn c(port); c.send(head("trunc-fin")); std::this_thread::sleep_for(80ms); c.close(); outcome("trunc-fin", "Content-Length body, client close"); }
		{ Conn c(port); c.send(head("trunc-rst")); std::this_thread::sleep_for(80ms); c.rst(); outcome("trunc-rst", "Content-Length body, client reset"); }
		{ Conn c(port); c.send(head("trunc-half")); std::this_thread::sleep_for(80ms); c.shut_wr(); outcome("trunc-half", "Content-Length body, half-close"); }
		{
			Conn c(port);
			c.send("POST /tbody?id=trunc-chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n");
			std::this_thread::sleep_for(80ms); c.close();
			outcome("trunc-chunked", "chunked body cut after a chunk");
		}
		{   // a body that does complete still ends with eof
			Conn c(port);
			c.send("POST /stream HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello");
			auto r = c.read_response();
			check(r.status == 200 && r.body == "streamed 5", "[H] a complete body still ends cleanly");
		}
	}
}

void section_I(App& app) {
	// [I] A cancel callback that throws is dropped; the handler's frames end by completing.
	{
		unsigned short port = 0;
		http::HttpAsioService svc(app, 1, http::Limits{.max_in_flight = 64});
		app.service = &svc;
		svc.start(0); port = svc.port(); wait_listen(port);
		{   // streamed: the connection frame must not unwind under the running handler
			auto o = app.get_obs("sx");
			Conn c(port);
			c.send("POST /throwcb-stream?id=sx HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(100, 'a'));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.close();
			std::this_thread::sleep_for(120ms);
			check(!o->finished && svc.reactor(0).gate().inflight.load() == 1,
				"[I] streamed: after the throwing callback the connection still holds its request (the handler runs)");
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] streamed: the handler finishes and its request was intact");
			check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[I] streamed: the request is released afterwards");
		}
		{   // buffered: the throw must not leave io_context::run()
			auto o = app.get_obs("bx");
			Conn c(port);
			c.send(get("/throwcb?id=bx"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.rst();
			std::this_thread::sleep_for(50ms);
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] buffered: the handler finishes after a throwing callback");
			check(one(port, get("/fast", "Connection: close\r\n")).body == "fast", "[I] buffered: the server keeps serving");
		}
		{   // a late registration that throws
			http::Request r;
			r.cancel(http::disconnected);
			bool threw = false;
			try { task::cancel_callback late(r.cancel_token(), [] { throw std::runtime_error("late"); }); } catch (...) { threw = true; }
			check(!threw && r.cancel_token().requested(), "[I] a late cancel callback that throws is dropped");
		}
	}
}
