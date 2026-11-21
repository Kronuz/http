#pragma once

// Serving one connection: what the sessions of a service share, and serve_connection, the keep-alive loop of parse, handler and reply.

#include "http_asio/connection_io.h"

namespace http {
namespace detail {

template <class Stream>
inline task::task<bool> write_owned_response(Stream &socket, const std::shared_ptr<AsioOwnedStreamControl> &control,
											  const std::shared_ptr<ConnectionInput<Stream>> &input, task::asio_scheduler &scheduler) {
	input->monitor(control);
	while (true) {
		if (std::chrono::steady_clock::now() >= control->exchange->deadline())
			control->exchange->close(ReplySettlement::TimedOut);
		if (control->exchange->settle())
			co_return control->exchange->closing_reason() == ReplySettlement::Written;
		auto original = control->exchange->take_write();
		if (!original) {
			co_await control->wait();
			continue;
		}
		const auto serial = original->serial;
		std::array<char, 32> chunk_head;
		std::string_view prefix, suffix;
		if (original->terminal)
			prefix = "0\r\n\r\n";
		else if (original->chunked && !original->chunk.bytes.empty()) {
			auto number = std::to_chars(chunk_head.data(), chunk_head.data() + chunk_head.size() - 2,
										original->chunk.bytes.size(), 16);
			if (number.ec != std::errc{})
				throw std::logic_error("HTTP chunk length overflow");
			*number.ptr++ = '\r';
			*number.ptr++ = '\n';
			prefix = {chunk_head.data(), static_cast<std::size_t>(number.ptr - chunk_head.data())};
			suffix = "\r\n";
		}
		std::array<asio::const_buffer, 4> buffers{asio::buffer(original->head), asio::buffer(prefix),
												  asio::buffer(original->chunk.bytes), asio::buffer(suffix)};
		const ReplySettlement outcome =
			co_await write_before_deadline(socket, buffers, control->exchange->deadline(), scheduler, control->write_token());
		// The write has completed, so the socket no longer reads the original's bytes. Only now
		// may its immutable body owner retire and wake the next producer turn.
		original.reset();
		control->exchange->complete_write(serial, outcome);
	}
}
// The transport's reads of a request's cancellation, which engage no watch and make no source.
struct RequestAccess {
	static bool cancelled(const Request& request) noexcept { return request.cancel_ && request.cancel_->requested(); }
	// Whether the request has its source: the handler asked for its token, or it was cancelled.
	static bool asked(const Request& request) noexcept { return request.cancel_.has_value(); }
};

// Maps an exception that escaped handle() to the response: an owned-stream reply
// is abandoned; otherwise the app's on_error first, then for whatever it left unanswered a
// 503 that closes the connection when the exception is task::cancelled from the request's own
// cancellation (a hang-up or shutdown), else a generic 500.
inline void map_handler_error(HttpHandler* handler, Request& request, AsioResponseWriter& writer, std::exception_ptr error) {
	if (writer.is_streamed()) { writer.abandon_stream(); return; }
	if (!writer.ended()) { handler->on_error(error, request, writer); }
	if (writer.is_streamed() || writer.ended()) { return; }
	bool cancelled = false;
	try { std::rethrow_exception(error); } catch (const task::cancelled&) { cancelled = true; } catch (...) {}
	if (cancelled && RequestAccess::cancelled(request)) {
		writer.status(503);
		writer.set_close();
		writer.write("Service Unavailable\n");
	} else {
		writer.status(500);
		writer.write("Internal Server Error\n");
	}
	writer.end();
}

// Shutdown's hook into one running request: abort() cancels it (its reads stop at their next
// checkpoint, its writes finish), and force() -- after the shutdown grace -- closes the
// socket a stalled peer holds. Tracked for the request's whole run, on the reactor's list.
template<class Stream>
struct RequestAbort final : reactor::Abortable {
	RequestAbort(reactor::Reactor& owner, Request& req, Stream& stream) : reactor(owner), request(req), socket(stream) { reactor.track(*this); }
	~RequestAbort() override { reactor.untrack(*this); }
	void abort() override { request.cancel(task::cancel_reason::shutdown); }
	void force() override { asio::error_code ignored; socket.lowest_layer().close(ignored); }
	reactor::Reactor& reactor;
	Request& request;
	Stream& socket;
};

// Clears the connection's disconnect watch on every exit, so a late read cannot reach a
// request that is gone, and the request's handle on it.
template<class Stream>
struct WatchGuard {
	explicit WatchGuard(ConnectionInput<Stream>& in, Request* req = nullptr) : input(in), request(req) {}
	~WatchGuard() { input.unwatch(); if (request) { request->set_cancel_watch(nullptr); } }
	ConnectionInput<Stream>& input;
	Request* request;
};

// Sends what the socket takes at once, without waiting, and advances `buffers` past it: a
// reply that fits the send buffer leaves with no asynchronous write, whose completion costs a
// trip through the loop's queue. False when the connection failed. A plain TCP socket in
// non-blocking mode only.
template<std::size_t N>
inline bool send_now(asio::ip::tcp::socket& socket, std::array<asio::const_buffer, N>& buffers) {
	asio::error_code error;
	std::size_t sent = socket.write_some(buffers, error);
	if (error && error != asio::error::would_block && error != asio::error::try_again) { return false; }
	for (auto& buffer : buffers) {
		const std::size_t n = (std::min)(sent, buffer.size());
		buffer += n;
		sent -= n;
	}
	return true;
}
template<std::size_t N>
inline std::size_t bytes_left(const std::array<asio::const_buffer, N>& buffers) {
	std::size_t n = 0;
	for (const auto& buffer : buffers) { n += buffer.size(); }
	return n;
}

// The connections of one loop waiting for their next request's first byte: close_idle() walks it
// on the loop. Each is tracked and untracked on that loop only.
struct IdleList {
	reactor::Reactor* reactor = nullptr;
	reactor::AbortList waiting;
};

// What the connections of one service share: their limits and response transforms, fixed before
// start(), and what stop_accepting() and close_idle() tell them.
struct ServiceState {
	ServiceState(const Limits& bounds, Tls secure, std::size_t loops) : limits(bounds), tls(std::move(secure)) {
		for (std::size_t i = 0; i < loops; ++i) { idle.push_back(std::make_unique<IdleList>()); }
	}
	const Limits limits;
	const Tls tls;
	AsioResponseOptions response;
	std::atomic<std::size_t> handshakes{0};   // TLS handshakes in progress, across the loops
	// stop_accepting() ran: a request whose first byte comes later is refused, a reply closes its connection.
	std::atomic<bool> closing{false};
	// close_idle() calls: a connection idle before the latest one closes after its reply.
	std::atomic<std::uint64_t> idle_closes{0};
	std::vector<std::unique_ptr<IdleList>> idle;   // one per loop
	reactor::AbortList& idle_of(const reactor::Reactor& loop) {
		for (auto& list : idle) { if (list->reactor == &loop) { return list->waiting; } }
		std::abort();   // every loop of the service has its list
	}
};

// A connection waiting for its next request, on its loop's idle list: close_idle() ends the wait,
// and with it the connection.
template<class Stream>
struct IdleWait final : reactor::Abortable {
	IdleWait(reactor::AbortList& list, ConnectionInput<Stream>& waiting) : input(waiting) { list.add(*this); }
	void abort() override { input.cancel_read(); }
	ConnectionInput<Stream>& input;
};

// Serve one connection: a keep-alive loop of parse -> handler -> write, all tasks on the
// connection's loop. handle() is awaited as a child task, so a handler that waits suspends and
// the loop serves other connections. A request enters the reactor's gate at its first byte, so
// wait_requests() counts it from then; one over the gate's cap, or one whose first byte comes
// after stop_accepting(), is answered 503 at headers-complete. From then until its response is
// written it is tracked for shutdown and watched for a client hang-up; both cancel it
// (Request::cancel) without destroying its frame. Its reply says Connection: close, and the
// connection closes after it, once the service stops accepting or close_idle() ran since the
// connection last waited idle.
// `stop` (the session's token) ends only the reads of a request, each through a deadline
// linked to it when a timeout bounds the read; the handler and the reply's writes never see
// it. A handler that opted into wants_body_stream runs beside the feed of its BodyStream.
template<class Stream>
inline task::task<void> serve_connection_body(Stream& socket, const std::shared_ptr<ConnectionInput<Stream>>& input, HttpHandler* handler,
	reactor::Reactor* reactor, ServiceState* state, const task::cancel_token& stop) {
	std::optional<RequestRejected> rejected;
	const Limits& limits = state->limits;
	const AsioResponseOptions* options = &state->response;
	task::asio_scheduler& scheduler = reactor->scheduler();
	try {
		asio::error_code opt_ec;
		socket.lowest_layer().set_option(asio::ip::tcp::no_delay(true), opt_ec);
		std::string remote;  // the peer's address, for every request on this connection
		if (auto peer = socket.lowest_layer().remote_endpoint(opt_ec); !opt_ec) { remote = peer.address().to_string(); }
		const asio::any_io_executor ex = socket.get_executor();
		RequestParser parser;
		parser.set_body_limit(limits.max_body);
		reactor::AdmissionGate::Lease admitted;   // held from the request's first byte to its settled response
		bool refused = false;                     // its first byte came after stop_accepting()
		// The request's first byte: it enters the gate, then reads whether the service stops
		// accepting. The fence orders the two against stop_accepting()'s store and wait_requests()'
		// read of the gates: a request that read the flag clear is counted there.
		const auto first_byte = [&] {
			admitted = reactor->gate().enter();
			std::atomic_thread_fence(std::memory_order_seq_cst);
			refused = state->closing.load(std::memory_order_seq_cst);
			if (refused) { admitted.reset(); }
		};
		// close_idle()'s count when the connection started: a later call closes it, at once when it
		// waits for a request (the walk on its loop), else at its next wait.
		const std::uint64_t idle_epoch = state->idle_closes.load(std::memory_order_relaxed);
		const auto idle_closed = [&] { return state->idle_closes.load(std::memory_order_relaxed) != idle_epoch; };
		// Whether the connection may carry another request after this reply, read once, as the reply
		// is framed: a stop after it refuses the next request (503), a close_idle() closes at the idle wait.
		const auto stays_open = [&] { return !state->closing.load(std::memory_order_relaxed) && !idle_closed(); };
		reactor::AbortList& idle_list = state->idle_of(*reactor);
		// The headers-complete hook: refuse a request the gate or the stop did not admit, build the
		// app's per-request extension, then choose how to take the body -- a BodyStream, an
		// on_request_body sink, or the buffer in Request::body.
		parser.set_headers_callback([handler, &remote, &admitted, &refused](Request& req) -> RequestParser::BodyIntake {
			if (refused) { throw RequestRejected(503, "Service Unavailable\n"); }
			if (!admitted) { throw RequestRejected(503, "Service Unavailable\n", RejectionContent::Text, 1); }
			req.remote = remote;
			req.extension = handler->create_extension(req);
			RequestParser::BodyIntake intake;
			if (handler->wants_body_stream(req)) {
				intake.stream = true;
			} else {
				intake.sink = handler->on_request_body(req);
			}
			return intake;
		});
		std::string buffered;
		// A body's read buffer: allocated by its first read, released at the next idle wait.
		constexpr std::size_t kScratch = 64 * 1024;
		std::unique_ptr<char[]> scratch;
		const auto body_buffer = [&] {
			if (!scratch) { scratch.reset(new char[kScratch]); }
			return asio::buffer(scratch.get(), kScratch);
		};
		std::string head;                          // a buffered reply's head; the storage is reused
		std::array<asio::const_buffer, 2> reply;   // what of the reply is still to be written
		// A plain TCP socket sends a buffered reply at once (send_now), so it is put in
		// non-blocking mode: a full send buffer then returns instead of waiting.
		bool send_first = false;
		if constexpr (std::is_same_v<Stream, asio::ip::tcp::socket>) {
			asio::error_code mode_ec;
			socket.non_blocking(true, mode_ec);
			send_first = !mode_ec;
		}

		using clock = std::chrono::steady_clock;
		const auto ms = [](std::chrono::milliseconds t) { return t.count() > 0; };
		const auto idle = limits.idle_timeout, header = limits.header_timeout, inactivity = limits.inactivity_timeout;
		// The next body read's deadline: the request's, or sooner when nothing arrives.
		const auto body_deadline = [&](clock::time_point request_deadline) {
			return ms(inactivity) ? std::min(request_deadline, clock::now() + inactivity) : request_deadline;
		};
		// A read's timeout: a deadline linked to `parent` when `until` bounds the read, else
		// `parent` itself. One read at a time holds it; the caller resets it after the read.
		std::optional<task::deadline<task::asio_scheduler>> timeout;
		const auto bounded = [&](clock::time_point until, const task::cancel_token& parent) -> task::cancel_token {
			if (until == clock::time_point::max()) { return parent; }
			timeout.emplace(scheduler, until, parent);
			return timeout->token();
		};
		// Ends the read's timeout; a read it cut short reports timed_out.
		const auto settle_read = [&](ReadResult& r) {
			if (timeout) {
				if (r.error == asio::error::operation_aborted && timeout->expired()) { r.error = asio::error::timed_out; }
				timeout.reset();
			}
		};
		// A parse that stopped for the body cap is a 413; any other parse error drops the connection.
		const auto parse_failed = [&] {
			if (parser.body_limit_exceeded()) { throw RequestRejected(413, "Payload Too Large\n"); }
		};
		// Writes the response; false when the connection cannot carry on, and the caller cancels
		// the request (a token the application kept sees the failed write). A buffered reply has
		// been framed by send_reply(), and this writes what it left.
		const auto respond = [&](AsioResponseWriter& writer, bool keep_alive, clock::time_point request_deadline) -> task::task<bool> {
			if (writer.is_streamed()) { co_return co_await write_owned_response(socket, writer.owned_stream(), input, scheduler); }
			co_return co_await write_with_progress(socket, reply, inactivity, request_deadline, scheduler) == ReplySettlement::Written;
		};
		// Frames a buffered reply into `reply` and, on plain TCP, sends what the socket takes at
		// once (send_now). 1: all of it left; 0: the connection failed; -1: respond() writes
		// the rest, or the reply is an owned stream.
		const auto send_reply = [&](AsioResponseWriter& writer, bool keep_alive) -> int {
			if (writer.is_streamed()) { return -1; }
			head.clear();
			writer.serialize_head(head, keep_alive);
			reply = {asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
			if constexpr (std::is_same_v<Stream, asio::ip::tcp::socket>) {
				if (send_first) {
					if (!send_now(socket, reply)) { return 0; }
					if (bytes_left(reply) == 0) { return 1; }
				}
			}
			return -1;
		};
		for (;;) {
			// A shutdown that arrived while the last request ran: its next read would not start.
			if (stop.requested()) { co_return; }
			const auto request_deadline = limits.request_timeout.count() > 0
				? clock::now() + limits.request_timeout
				: clock::time_point::max();
			// Phase 1: parse the request line + headers. Until a byte arrives the idle
			// timeout bounds the wait; from the first byte, the header timeout.
			auto header_deadline = clock::time_point::max();
			bool started = !buffered.empty();   // the request's first byte has arrived (pipelined)
			if (started) {
				first_byte();
				if (ms(header)) { header_deadline = clock::now() + header; }
			}
			while (!parser.headers_done()) {
				if (buffered.empty()) {
					std::optional<IdleWait<Stream>> idle_wait;
					if (!started) {
						// The connection waits holding only the input's own buffer, on its loop's idle list.
						scratch.reset();
						if (buffered.capacity() > 2 * ConnectionInput<Stream>::kReadAhead) { std::string().swap(buffered); }
						if (idle_closed()) { co_return; }
						idle_wait.emplace(idle_list, *input);
					}
					const bool waiting = header_deadline == clock::time_point::max();
					const auto until = std::min(request_deadline, waiting ? (ms(idle) ? clock::now() + idle : clock::time_point::max()) : header_deadline);
					ReadResult r = co_await input->read_ahead(buffered, bounded(until, stop));
					idle_wait.reset();
					settle_read(r);
					if (r.error || r.bytes == 0) { co_return; }
					if (!started) {
						started = true;
						first_byte();
					}
					if (waiting && ms(header)) { header_deadline = clock::now() + header; }
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (parser.streaming()) {
				// ---- STREAMED BODY: the handler runs while the feed reads the body ----
				Request request = parser.take_headers();
				request.remote = remote;
				request.set_scheduler(&scheduler);
				request.set_deadline(request_deadline);
				reactor->lag().note_activity();
				bool keep_alive = request.keep_alive;
				auto stream = std::make_shared<BodyStream>(scheduler, /*capacity=*/8);
				request.body_stream = stream;
				AsioResponseWriter writer(request, options, &ex, reactor);
				RequestAbort<Stream> tracked(*reactor, request, socket);
				WatchGuard<Stream> watching(*input, &request);
				// Stops the feed when the handler completes; linked, so shutdown stops it too.
				task::cancel_source feed_stop{stop};
				bool handler_done = false;

				enum class Fed { Complete, Failed, Abandoned };
				// The body failed: cancel the request first, so a reader woken by the error sees it cancelled.
				const auto fail_body = [&](asio::error_code error) {
					request.cancel(stop.requested() ? task::cancel_reason::shutdown : disconnected);
					stream->fail(error);
				};
				// Feeds the body until it ends, or until the handler completes first (it answered
				// without reading it all) and requests `token`.
				const auto feed = [&](task::cancel_token token) -> task::task<Fed> {
					try {
					for (;;) {
						// Drain whatever body has been parsed so far -- including bytes that
						// arrived in the same read as the headers (a small body completes
						// during phase 1, so this must run before the complete() check).
						std::string chunk = parser.take_pending_body();
						if (!chunk.empty() && !co_await stream->push(std::move(chunk), token)) {
							if (handler_done || !stop.requested()) { co_return Fed::Abandoned; }
							fail_body(asio::error::operation_aborted);
							co_return Fed::Failed;
						}
						if (parser.complete()) {
							stream->finish();
							// The handler still runs: a hang-up from here cancels the request. As for a
							// buffered body, the watch waits for the handler to ask, unless it already did.
							if (!handler_done) {
								const bool lazy = !RequestAccess::asked(request);
								input->watch([&request] { request.cancel(disconnected); }, lazy);
								if (lazy) { request.set_cancel_watch(input.get()); }
							}
							co_return Fed::Complete;
						}
						if (buffered.empty()) {
							ReadResult r = co_await input->read(body_buffer(), bounded(body_deadline(request_deadline), token));
							settle_read(r);
							if (r.error == asio::error::operation_aborted && handler_done) { co_return Fed::Abandoned; }
							if (r.error || r.bytes == 0) {
								// The peer ended (or the read timed out, or the service stops) before the body
								// was complete: the reader sees a failed body, never eof, and the request is
								// already cancelled.
								asio::error_code failure = r.error;
								if (!failure || failure == asio::error::eof) { failure = asio::error::connection_reset; }
								fail_body(failure);
								co_return Fed::Failed;
							}
							buffered.append(scratch.get(), r.bytes);
						}
						std::size_t used = parser.feed(buffered.data(), buffered.size());
						buffered.erase(0, used);
						if (parser.errored()) { fail_body(asio::error::invalid_argument); co_return Fed::Failed; }
					}
					} catch (...) {
						// The reader sees a failed body, never a hung read; when_all rethrows this.
						if (!stream->closed()) { fail_body(asio::error::operation_aborted); }
						throw;
					}
				};
				// The handler, with its exception mapped; its completion stops the feed.
				const auto run_handler = [&]() -> task::task<void> {
					try { co_await handler->handle(request, writer); }
					catch (...) {
						try { map_handler_error(handler, request, writer, std::current_exception()); } catch (...) {}
					}
					handler_done = true;
					stream->abandon();
					feed_stop.request();
				};
				// Both are children of this frame: when_all returns once both completed, so the
				// handler and the request and writer it references end only by completing.
				auto [fed, ignored] = co_await task::when_all(feed(feed_stop.token()), run_handler());
				(void)ignored;
				input->unwatch();
				request.set_cancel_watch(nullptr);
				if (fed != Fed::Complete || !stays_open()) { keep_alive = false; }

				if (!writer.is_streamed() && !writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
				const int sent = send_reply(writer, keep_alive);
				if (sent == 0 || (sent < 0 && !co_await respond(writer, keep_alive, request_deadline))) { request.cancel(disconnected); co_return; }
				admitted.reset();
				if (!keep_alive || writer.wants_close()) { break; }
				parser.reset();
				continue;
			}

			// ---- BUFFERED / PUSH path ----
			while (!parser.complete()) {
				if (buffered.empty()) {
					ReadResult r = co_await input->read(body_buffer(), bounded(body_deadline(request_deadline), stop));
					settle_read(r);
					if (r.error || r.bytes == 0) { co_return; }
					buffered.append(scratch.get(), r.bytes);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (request_deadline != clock::time_point::max() && clock::now() >= request_deadline) { co_return; }
			Request request = parser.take();
			request.remote = remote;
			request.set_scheduler(&scheduler);
			request.set_deadline(request_deadline);
			reactor->lag().note_activity();
			parser.resume();
			bool keep_alive = request.keep_alive;
			// The extension and the body (pushed to the app's sink, or buffered into
			// request.body) were already handled at headers-complete by the parser hook.

			AsioResponseWriter writer(request, options, &ex, reactor);
			RequestAbort<Stream> tracked(*reactor, request, socket);
			{
				WatchGuard<Stream> watching(*input, &request);
				// The watch starts when the handler first asks for its cancel token (CancelWatch).
				input->watch([&request] { request.cancel(disconnected); }, /*lazy=*/true);
				request.set_cancel_watch(input.get());
				// A child task: declared after the request and writer, so a teardown that destroys
				// this frame destroys the handler's first; otherwise it ends only by completing.
				try {
					co_await handler->handle(request, writer);
				} catch (...) {
					map_handler_error(handler, request, writer, std::current_exception());
				}
			}
			keep_alive = keep_alive && stays_open();
			const int sent = send_reply(writer, keep_alive);
			if (sent == 0 || (sent < 0 && !co_await respond(writer, keep_alive, request_deadline))) { request.cancel(disconnected); co_return; }
			admitted.reset();
			if (!keep_alive || writer.wants_close()) { break; }
		}
	} catch (const RequestRejected& error) {
		rejected.emplace(error);
	} catch (const std::exception&) {
		// a read-write or parse failure -- drop the connection
	}
	if (rejected) {
		// Parser/body ownership has already unwound. The bounded error body
		// consumes no publisher reservation and cannot wait on a slow reader.
		std::string text(rejected->what());
		std::string head = "HTTP/1.1 " + std::to_string(rejected->status()) + " " + reason_phrase(rejected->status()) +
			"\r\nContent-Type: " + std::string(rejected->content_type()) + "\r\nContent-Length: " + std::to_string(text.size()) +
			(rejected->retry_after() ? "\r\nRetry-After: " + std::to_string(rejected->retry_after()) : std::string()) +
			"\r\nConnection: close\r\n\r\n";
		std::array<asio::const_buffer, 2> buffers{asio::buffer(head), asio::buffer(text)};
		co_await write_before_deadline(socket, buffers, std::chrono::steady_clock::now() + std::chrono::seconds(1), scheduler);
	}
}

// The reactor::Session body of a connection: serve it, close the socket, then wait for the
// watch's read to complete, so the socket and the input outlive it.
template<class Stream>
inline task::task<void> serve_connection(Stream socket, HttpHandler* handler,
    reactor::Reactor* reactor, ServiceState* state, reactor::SessionContext context) {
	auto input = std::make_shared<ConnectionInput<Stream>>(socket);
	try { co_await serve_connection_body(socket, input, handler, reactor, state, context.stop); } catch (...) {}
	asio::error_code error; socket.lowest_layer().close(error);
	co_await input->drain();
}

}  // namespace detail
}  // namespace http
