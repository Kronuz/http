#pragma once

// What a served connection shares: the owned-stream reply write, handler errors, shutdown hooks, idle list, ServiceState.

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

}  // namespace detail
}  // namespace http
