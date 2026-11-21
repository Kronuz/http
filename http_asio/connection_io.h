#pragma once

// The connection's I/O: deadline-bound reply writes and ConnectionInput, the owned read-ahead of the socket.

#include "http_asio/response_writer.h"

namespace http {

namespace detail {

// Writes buffers before `at`: the write takes a deadline linked to `stop` (a reply control's
// token, or none), so a deadline or a request of `stop` cancels it.
template<class Stream, std::size_t N>
inline task::task<ReplySettlement> write_before_deadline(Stream& socket, std::array<asio::const_buffer, N> buffers,
	std::chrono::steady_clock::time_point at, task::asio_scheduler& scheduler, task::cancel_token stop = {}) {
	task::deadline<task::asio_scheduler> deadline(scheduler, at, stop);
	const auto [error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto handler) { asio::async_write(socket, buffers, std::move(handler)); }, deadline.token());
	(void)written;
	if (!error) { co_return ReplySettlement::Written; }
	if (deadline.expired()) { co_return ReplySettlement::TimedOut; }
	co_return stop.requested() ? ReplySettlement::Failed : ReplySettlement::Disconnected;
}
// Writes buffers before `deadline`, and with an inactivity bound, 64 KiB of the buffers
// taken together at a time (a slice spans the buffers it covers, so a head and a small body
// leave in one send), each slice before `inactivity` from its start (whichever deadline
// comes first). No token: a reply's write finishes, or a deadline or the peer ends it.
template<class Stream, std::size_t N>
inline task::task<ReplySettlement> write_with_progress(Stream& socket, std::array<asio::const_buffer, N> buffers,
	std::chrono::milliseconds inactivity, std::chrono::steady_clock::time_point deadline, task::asio_scheduler& scheduler) {
	if (inactivity.count() <= 0) {
		if (deadline == std::chrono::steady_clock::time_point::max()) {
			const auto [error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
				[&](auto handler) { asio::async_write(socket, buffers, std::move(handler)); });
			(void)written;
			co_return error ? ReplySettlement::Disconnected : ReplySettlement::Written;
		}
		co_return co_await write_before_deadline(socket, buffers, deadline, scheduler);
	}
	constexpr std::size_t slice = 64 * 1024;
	std::size_t total = 0;
	for (const auto& b : buffers) { total += b.size(); }
	for (std::size_t at = 0; at < total; at += slice) {
		// The window [at, at + slice) of the buffers laid end to end: a slice of each buffer it touches.
		std::array<asio::const_buffer, N> window{};
		std::size_t skip = at, room = slice;
		for (std::size_t i = 0; i < N; ++i) {
			if (skip >= buffers[i].size()) { skip -= buffers[i].size(); continue; }
			const std::size_t n = std::min(buffers[i].size() - skip, room);
			window[i] = asio::const_buffer(static_cast<const char*>(buffers[i].data()) + skip, n);
			room -= n;
			skip = 0;
		}
		const auto until = std::min(deadline, std::chrono::steady_clock::now() + inactivity);
		const auto r = co_await write_before_deadline(socket, window, until, scheduler);
		if (r != ReplySettlement::Written) { co_return r; }
	}
	co_return ReplySettlement::Written;
}

// What a read of the connection returned: an error (eof, a socket error, or
// operation_aborted when its token was requested), or the bytes.
struct ReadResult {
	asio::error_code error;
	std::size_t bytes = 0;
};

// One owned read keeps watching the socket while a handler runs or an owned-stream reply
// waits. It reads ahead, up to kReadAhead bytes into the input's own buffer, so the request
// a quick keep-alive or pipelining client sends meanwhile arrives with that one read: the
// pending read is the next request's read, and a request costs one recv, not a one-byte
// recv and a second one for the rest. What the watch took is returned to the parser by
// read(), first and in order, never discarded by a race, and no further read-ahead is
// armed until it is all consumed, so at most kReadAhead bytes are held outside the kernel
// however much the client sends. One rule reads it: a read error other than EOF (a reset) is a
// disconnect; EOF is a half-close, legal HTTP, so it ends reading but cancels nothing, and the
// requests already read are answered. A request in flight (watch) is cancelled on a reset (not
// our own cancellation), an owned-stream reply (monitor) is closed on one. After EOF
// nothing more is read, so a later reset is seen only when the reply's write fails, which also
// cancels the request. Pipelined input (the client sent the next request early) ends the watch
// for the rest of that request: a later disconnect is not seen until the next read.
// On a plain TCP socket a request's watch is lazy: it reads nothing until the handler first
// asks for its cancel token (engage(), through Request's CancelWatch hook), and then one read
// that does not wait decides what happened meanwhile by the same rules. A handler that never
// asks costs no read. Between requests the connection reads its next head through the same
// read (read_ahead()), so an idle connection holds no buffer but this one.
template<class Stream>
class ConnectionInput : public std::enable_shared_from_this<ConnectionInput<Stream>>, public CancelWatch {
public:
	// The most the watch reads ahead of the parser.
	static constexpr std::size_t kReadAhead = 4096;
	// Whether watch(..., lazy) can wait for engage(): a plain TCP socket can be asked, at once,
	// what the peer did while nobody watched (a recv with MSG_DONTWAIT into the input's buffer).
	// A TLS stream cannot (its records must be decrypted to tell a close_notify from data), so
	// its watch reads from the start.
#if defined(MSG_DONTWAIT)
	static constexpr bool kLazyWatch = std::is_same_v<Stream, asio::ip::tcp::socket>;
#else
	static constexpr bool kLazyWatch = false;
#endif
	explicit ConnectionInput(Stream& stream) : stream_(stream) {}
	void monitor(const std::shared_ptr<AsioOwnedStreamControl>& control) {
		observer_ = control;
		if (ready_ && error_ && error_ != asio::error::eof) {
			control->exchange->observe_peer_disconnect(); return;
		}
		arm();
	}
	// Calls `on_disconnect` once, on the loop, when the peer closes while the watch is set.
	// A lazy watch (kLazyWatch streams) reads nothing until engage(): until the handler asks
	// about cancellation, the request costs no read and no wake-up.
	void watch(std::function<void()> on_disconnect, bool lazy = false) {
		on_disconnect_ = std::move(on_disconnect);
		if (ready_) {
			if (reset(error_)) { fire(); }
			else if (size_ || error_ == asio::error::eof) { on_disconnect_ = nullptr; }
			return;
		}
		if (lazy && kLazyWatch) { return; }
		arm();
	}
	void unwatch() noexcept { on_disconnect_ = nullptr; }
	// CancelWatch: the handler asked about cancellation; starts a lazy watch. One read that does
	// not wait decides what the peer did while nobody watched, as the watch's read would have:
	// data (a pipelined request) is read ahead and ends the watch, EOF (a half-close) ends it, a
	// socket error is a disconnect, and nothing yet arms the wait.
	void engage() noexcept override {
		if (!on_disconnect_ || pending_ || ready_) { return; }
		if constexpr (kLazyWatch) {
			asio::error_code error;
			std::size_t size = 0;
			if (receive_now(error, size)) { completed(error, size); return; }
		}
		try { arm(); } catch (...) {}   // no memory for the read: the request goes unwatched
	}

	// The awaitable drain() returns: it waits for the pending read to complete, and nothing
	// cancels the wait. A frame destroyed while it waits leaves no waiter behind.
	class Drain {
	public:
		explicit Drain(ConnectionInput& input) noexcept : input_(input) {}
		Drain(const Drain&) = delete;
		Drain& operator=(const Drain&) = delete;
		~Drain() { input_.forget(waiting_); }
		bool await_ready() const noexcept { return !input_.pending_; }
		void await_suspend(std::coroutine_handle<> h) noexcept { input_.wait_for_read(waiting_ = h); }
		void await_resume() noexcept { waiting_ = {}; }
	private:
		ConnectionInput& input_;
		std::coroutine_handle<> waiting_;
	};
	// Waits for the pending read to complete (the session's last step, so the socket and the
	// input outlive the read).
	Drain drain() noexcept { return Drain(*this); }

	// The awaitable read() returns (see read()).
	class Read {
	public:
		Read(ConnectionInput& input, asio::mutable_buffer output, task::cancel_token token)
			: input_(input), output_(output), token_(std::move(token)) {}
		Read(ConnectionInput& input, std::string& append, task::cancel_token token)
			: input_(input), append_(&append), token_(std::move(token)) {}
		Read(const Read&) = delete;
		Read& operator=(const Read&) = delete;
		~Read() { input_.forget(waiting_); }
		bool await_ready() {
			if (append_ && !input_.pending_ && !input_.ready_) {
				if (token_.requested()) { aborted_ = true; return true; }
				input_.arm();
			}
			if (!input_.pending_ && !input_.ready_) {
				direct_.emplace(SocketRead{&input_.stream_, output_}, std::move(token_));
				return direct_->await_ready();
			}
			return input_.ready_;
		}
		std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
			if (direct_) { return direct_->await_suspend(h) ? std::noop_coroutine() : h; }
			// The watch's read is pending: its completion resumes h. A requested token ends it
			// by cancelling it on the socket and marking the wait cancelled (see readable()).
			input_.wait_for_read(waiting_ = h);
			cancel_.emplace(token_, Cancel{&input_});
			return std::noop_coroutine();
		}
		ReadResult await_resume() {
			if (aborted_) { return {asio::error::operation_aborted, 0}; }
			if (direct_) {
				const auto [error, bytes] = direct_->await_resume();
				return {error, bytes};
			}
			cancel_.reset();
			waiting_ = {};
			return append_ ? input_.take(*append_) : input_.take(output_);
		}
	private:
		struct SocketRead {
			Stream* stream;
			asio::mutable_buffer output;
			template<class Handler> void operator()(Handler&& handler) const { stream->async_read_some(output, std::forward<Handler>(handler)); }
		};
		struct Cancel {
			ConnectionInput* input;
			void operator()() const { input->cancel_read(); }
		};
		using Direct = decltype(task::await_asio<void(asio::error_code, std::size_t)>(std::declval<SocketRead>(), std::declval<task::cancel_token>()));
		ConnectionInput& input_;
		asio::mutable_buffer output_;
		std::string* append_ = nullptr;   // read_ahead(): the bytes go here
		bool aborted_ = false;            // read_ahead() with its token already requested
		task::cancel_token token_;
		std::optional<Direct> direct_;
		std::optional<task::cancel_callback<Cancel>> cancel_;
		std::coroutine_handle<> waiting_;
	};
	// The next bytes: what the watch read ahead first, else a read of the socket that `token`
	// cancels (a deadline's, the session's, or none). With nothing held it is the socket's own
	// operation, awaited in place; with the watch's read pending, its completion resumes the
	// reader. Neither makes a coroutine frame.
	Read read(asio::mutable_buffer output, task::cancel_token token) { return Read(*this, output, std::move(token)); }
	// The next bytes, appended to `out`: at most kReadAhead, always through the watch's read into
	// the input's own buffer, so a connection waiting here holds no buffer of its own.
	Read read_ahead(std::string& out, task::cancel_token token) { return Read(*this, out, std::move(token)); }
	// Ends the waiting reader's read as a requested token does: with operation_aborted.
	void cancel_read() noexcept {
		cancelled_ = true;
		if constexpr (kLazyWatch) {
			asio::error_code ignored;
			stream_.lowest_layer().cancel(ignored);
		} else {
			// The slot reaches the TLS read also after its lowest-layer read completed, before its
			// continuation: that checks the slot and does not read again.
			watch_cancel_.emit(asio::cancellation_type::terminal);
		}
	}
private:
	// The one coroutine the pending read resumes when it completes: a reader or the drain, with
	// the scheduler it suspended on.
	void wait_for_read(std::coroutine_handle<> h) noexcept {
		assert(!waiter_);
		waiter_ = h;
		waiter_scheduler_ = task::current();
		cancelled_ = false;
	}
	void forget(std::coroutine_handle<> h) noexcept { if (h && waiter_ == h) { waiter_ = {}; } }
	// By task::await_asio's own rule (task_asio.hh): in place in a top-level handler on the
	// waiter's loop, within the inline-resume bound, for a light reference; else, and always for
	// a heavy one, through its scheduler (a heavy request's reads stay in the heavy lane).
	void resume_waiter() {
		if (const auto h = std::exchange(waiter_, {})) { task::detail::asio_park::resume(waiter_scheduler_, h); }
	}
	// What the watch's read left, for read(): the bytes read ahead, oldest first (the rest stays
	// for the next read), else its EOF or error.
	ReadResult take(asio::mutable_buffer output) noexcept {
		assert(ready_);
		if (size_) {
			const std::size_t n = (std::min)(output.size(), size_ - taken_);
			std::memcpy(output.data(), buffer_ + taken_, n);
			taken_ += n;
			if (taken_ == size_) { size_ = taken_ = 0; ready_ = false; }
			return ReadResult{{}, n};
		}
		ready_ = false;
		return ReadResult{error_ ? error_ : asio::error::eof, 0};
	}
	ReadResult take(std::string& out) {
		assert(ready_);
		if (size_) {
			const std::size_t n = size_ - taken_;
			out.append(buffer_ + taken_, n);
			size_ = taken_ = 0;
			ready_ = false;
			return ReadResult{{}, n};
		}
		ready_ = false;
		return ReadResult{error_ ? error_ : asio::error::eof, 0};
	}
	// The watch's read. On plain TCP it waits for the socket to be readable and then reads
	// without waiting: an asynchronous read would first try the socket at once, a second
	// recv after the one engage() or the last read already found empty.
	void arm() {
		if (pending_ || ready_) { return; }
		pending_ = true;
		auto owner = this->shared_from_this();
		try {
			if constexpr (kLazyWatch) {
				stream_.async_wait(asio::socket_base::wait_read, [owner](asio::error_code error) { owner->readable(error); });
			} else {
				stream_.async_read_some(asio::buffer(buffer_, sizeof buffer_),
					asio::bind_cancellation_slot(watch_cancel_.slot(), [owner](asio::error_code error, std::size_t size) {
						owner->completed(error, size);
					}));
			}
		} catch (...) { pending_ = false; throw; }
	}
	// A wake that finds nothing waits again, unless the waiting reader's token was requested
	// after the wait completed (its socket cancel found nothing to cancel): that ends the read.
	void readable(asio::error_code error) {
		std::size_t size = 0;
		if (!error && !receive_now(error, size)) {
			if (waiter_ && cancelled_) { error = asio::error::operation_aborted; }
			else {
				pending_ = false;
				try { arm(); return; } catch (...) { error = asio::error::no_memory; }
			}
		}
		completed(error, size);
	}
	// A read into the input's buffer that does not wait; false when there is nothing to read.
	// EOF is asio::error::eof, a failed read its errno.
	bool receive_now(asio::error_code& error, std::size_t& size) {
#if defined(MSG_DONTWAIT)
		if constexpr (kLazyWatch) {
			ssize_t n;
			do { n = ::recv(stream_.native_handle(), buffer_, sizeof buffer_, MSG_DONTWAIT); } while (n < 0 && errno == EINTR);
			if (n > 0) { error = {}; size = static_cast<std::size_t>(n); return true; }
			if (n == 0) { error = asio::error::eof; size = 0; return true; }
			if (errno == EAGAIN || errno == EWOULDBLOCK) { return false; }
			error = asio::error_code(errno, asio::error::get_system_category()); size = 0;
			return true;
		}
#endif
		(void)error; (void)size;
		return false;
	}
	void completed(asio::error_code error, std::size_t size) {
		pending_ = false; ready_ = true; error_ = error; size_ = size; taken_ = 0;
		if (error && error != asio::error::eof) {
			if (auto c = observer_.lock()) { c->exchange->observe_peer_disconnect(); }
		}
		if (on_disconnect_) {
			if (reset(error)) { fire(); }
			else if (error != asio::error::operation_aborted) { on_disconnect_ = nullptr; }
		}
		resume_waiter();   // last: the reader may arm the next read
	}
	// A read error that is the peer's disconnect: neither EOF (a half-close) nor our own cancellation.
	static bool reset(const asio::error_code& error) noexcept {
		return error && error != asio::error::eof && error != asio::error::operation_aborted;
	}
	void fire() {
		auto callback = std::move(on_disconnect_);
		on_disconnect_ = nullptr;
		if (callback) { callback(); }
	}
	Stream& stream_;
	std::coroutine_handle<> waiter_;   // resumed when the pending read completes
	task::scheduler_ref waiter_scheduler_;
	char buffer_[kReadAhead];
	std::size_t size_ = 0, taken_ = 0;   // bytes the last read returned, and how many read() has handed out
	asio::error_code error_;
	bool pending_ = false, ready_ = false;
	bool cancelled_ = false;   // the waiting reader's token was requested
	asio::cancellation_signal watch_cancel_;   // the TLS watch's read: cancel_read() ends it
	std::function<void()> on_disconnect_;
	std::weak_ptr<AsioOwnedStreamControl> observer_;
};

}  // namespace detail
}  // namespace http
