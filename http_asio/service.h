#pragma once

// HttpAsioService: the HTTP service over a reactor::TcpServer.

#include "http_asio/serve_connection.h"

namespace http {

// The HTTP service: an application HttpHandler served over a reactor::TcpServer. The
// server owns the runtime (N shared-nothing reactors on N threads, one port via
// SO_REUSEPORT or a portable shared acceptor, per-reactor admission gate, graceful
// shutdown); this class is the thin HTTP-shaped adapter -- it turns the handler, the Limits
// and the response transforms into the per-connection Session (detail::serve_connection).
class HttpAsioService {
	using Serve = task::task<void> (*)(asio::ip::tcp::socket, HttpHandler*, reactor::Reactor*, detail::ServiceState*, reactor::SessionContext);
	static const Limits& checked(const Limits& limits) {
		for (const auto timeout : {limits.idle_timeout, limits.header_timeout, limits.inactivity_timeout, limits.request_timeout,
				limits.shutdown_grace, limits.tls_handshake}) {
			if (timeout < std::chrono::milliseconds::zero() || timeout > std::chrono::hours(24)) {
				throw std::invalid_argument("http::Limits: each timeout is between zero and 24 hours");
			}
		}
		if (limits.max_in_flight > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
			throw std::invalid_argument("http::Limits: max_in_flight past int");
		}
		return limits;
	}
	HttpAsioService(HttpHandler& handler, std::size_t reactors, const Limits& limits, Tls tls, Serve serve)
		: handler_(handler), state_(checked(limits), std::move(tls), reactors != 0 ? reactors : 1), serve_(serve),
		  server_(reactors, static_cast<int>(limits.max_in_flight), reactor::Session{
			[this](asio::ip::tcp::socket socket, reactor::Reactor& reactor, reactor::SessionContext context) {
				return serve_(std::move(socket), &handler_, &reactor, &state_, std::move(context));
			}}, limits.max_clients) {
		server_.set_shutdown_grace(limits.shutdown_grace);
		for (std::size_t i = 0; i < server_.reactors(); ++i) { state_.idle[i]->reactor = &server_.reactor(i); }
	}
public:
	// `reactors` loops serving plain HTTP within `limits` (the tls_ fields unused). Throws
	// std::invalid_argument for a timeout outside zero to 24 hours or a max_in_flight past int.
	HttpAsioService(HttpHandler& handler, std::size_t reactors, const Limits& limits)
		: HttpAsioService(handler, reactors, limits, {}, &detail::serve_connection<asio::ip::tcp::socket>) {}
	// The same over TLS with `tls.context`, plain without one; defined in http_asio_tls.h, which
	// serves it. Also throws for a zero tls_handshake or tls_handshakes.
	inline HttpAsioService(HttpHandler& handler, std::size_t reactors, const Limits& limits, Tls tls);

	HttpAsioService(const HttpAsioService&) = delete;
	HttpAsioService& operator=(const HttpAsioService&) = delete;

	const Limits& limits() const noexcept { return state_.limits; }
	void set_bind_options(const AsioBindOptions& options) { server_.set_bind_options(options); }
	reactor::ConnectionStats connection_stats() const noexcept { return server_.connection_stats(); }
	std::shared_ptr<const reactor::ConnectionAdmission> connection_observer() const noexcept { return server_.connection_observer(); }
	void enable_compression(CompressionOptions options = {}) {
		state_.response.compress = true;
		state_.response.compression = std::move(options);
	}
	// A repeat GET of an unchanged resource costs a hash + a header instead of the body
	// (weak ETag / If-None-Match -> 304).
	void enable_conditional() { state_.response.conditional = true; }
	// A buffered GET 200 advertises Accept-Ranges and serves a single byte Range as 206
	// (or 416) -- what media players and resumable downloads use to seek.
	void enable_ranges() { state_.response.ranges = true; }

	// Bind + run all reactors (each on its own thread). Returns once the threads are
	// listening; the caller keeps its thread and stops with stop().
	void start(unsigned short port) { server_.start(port); }
	// The port the listeners are bound to, as the OS reports it (the answer for start(0)); see
	// reactor::TcpServer::port(). Valid after start() returns, until stop(); 0 before start().
	unsigned short port() const noexcept { return server_.port(); }

	// Closes the connections waiting for a request at once, and every other one after its reply,
	// which says Connection: close. Returns at once, from any thread (a loop's too); each loop
	// closes its idle connections on its next turn.
	void close_idle() {
		state_.idle_closes.fetch_add(1, std::memory_order_relaxed);
		for (auto& list : state_.idle) {
			asio::post(list->reactor->io(), [idle = list.get()] { idle->waiting.each([](reactor::Abortable& wait) { wait.abort(); }); });
		}
	}

	// Teardown is task's four phases, each a call forwarded to reactor::TcpServer, so an
	// application with its own schedulers runs each phase on all of them before the next:
	//   stop()           cancel in-flight requests (their responses are still written), end the
	//                    connections' reads, force the sockets still busy after the grace, stop
	//                    the loops and join;
	//   close()          the reactors' schedulers drop what is posted and destroy what is spawned;
	//   destroy_roots()  destroy the tasks still suspended on them, with every io_context alive;
	//   destruction      runs every phase not yet run (server_'s destructor), so a service going
	//                    out of scope alone tears down cleanly.
	// Each phase first runs any earlier one not yet run, so every call order is safe.
	// stop()'s first part runs in steps for a caller that lets requests finish first, from outside
	// the loops and from one thread: stop_accepting(), wait_requests(token), cancel_requests(), then
	// stop() (force_connections() is stop()'s own after the grace). From stop_accepting() on, a
	// request whose first byte comes later is answered 503 on the connection it came on, and every
	// reply says Connection: close and closes its connection. wait_requests() counts a request from
	// its first byte: an accepted connection that sent nothing yet holds none.
	void stop_accepting() {
		state_.closing.store(true, std::memory_order_seq_cst);
		server_.stop_accepting();
	}
	bool wait_requests(const task::cancel_token& token) { return server_.wait_requests(token); }
	void cancel_requests() { server_.cancel_requests(); }
	void force_connections() { server_.force_connections(); }
	void stop() {
		state_.closing.store(true, std::memory_order_seq_cst);
		server_.stop();
	}
	void close() { stop(); server_.close(); }
	void destroy_roots() { close(); server_.destroy_roots(); }

	std::size_t reactors() const { return server_.reactors(); }
	// Reactor `i`, for its lag probe (reactor_lag.h): `reactor(i).lag().snapshot()`.
	reactor::Reactor& reactor(std::size_t i) { return server_.reactor(i); }
	// Run a lag probe on every reactor; before start().
	void enable_lag_probe() { server_.enable_lag_probe(); }

private:
	// Declared before server_: the Session captures &handler_ and &state_, and member
	// destruction is reverse-declaration, so server_ (its threads joined in ~TcpServer)
	// is torn down before what it referenced.
	HttpHandler& handler_;
	detail::ServiceState state_;
	Serve serve_;
	reactor::TcpServer server_;
};

}  // namespace http
