#pragma once
#include "http_client_core.h"
#include <asio.hpp>
#include <functional>

namespace http::test {
// One executor parks authentic write completions after their bytes reach the peer.
class WriteGate {
  public:
	explicit WriteGate(asio::any_io_executor executor) : changed_(executor) {}
	void arm() {
		if (armed_ || held_) throw std::logic_error("write gate already armed");
		parser_.emplace();
		armed_ = true;
		read_ = false;
	}
	bool held() const noexcept { return held_; }
	const client::ResponseHead &held_head() const {
		if (!held_ || !parser_) throw std::logic_error("write gate has no held response");
		return parser_->head();
	}
	std::size_t observed() const noexcept { return observed_; }
	std::size_t delivered() const noexcept { return delivered_; }
	void observe_read(std::size_t bytes) {
		if (held_ && bytes) { read_ = true; changed_.cancel(); }
	}
	template <class Buffers, class Completion>
	void complete_write(const Buffers &buffers, asio::error_code error, std::size_t bytes, Completion operation) {
		++observed_;
		bool park = false;
		try {
			if (armed_ && !error) {
				auto remaining = bytes;
				for (auto position = asio::buffer_sequence_begin(buffers);
					position != asio::buffer_sequence_end(buffers) && remaining; ++position) {
					const asio::const_buffer buffer(*position);
					const auto size = std::min(buffer.size(), remaining);
					auto input = std::span<const char>(static_cast<const char *>(buffer.data()), size);
					while (!input.empty()) {
						parser_->feed(input);
						if (parser_->head_ready()) parser_->accept_head();
						parser_->consume_body(parser_->body().size());
					}
					remaining -= size;
				}
				if (remaining) throw std::logic_error("write completion exceeds original buffers");
				park = parser_->done();
			}
		} catch (...) { failure_ = std::current_exception(); changed_.cancel(); }
		if (park) {
			auto pending = std::make_shared<std::optional<Completion>>();
			std::function<void()> completion = [this, pending, error, bytes] {
				if (!*pending) return;
				auto original = std::move(**pending);
				pending->reset();
				++delivered_;
				original.complete(error, bytes);
			};
			pending->emplace(std::move(operation));
			completion_ = std::move(completion);
			armed_ = false;
			held_ = true;
			changed_.cancel();
			return;
		}
		++delivered_;
		operation.complete(error, bytes);
	}
	bool release() {
		if (!completion_) return false;
		auto original = std::exchange(completion_, {});
		held_ = false;
		original();
		return true;
	}
	asio::awaitable<void> wait_held() { co_await wait(false); }
	asio::awaitable<void> wait_read() { co_await wait(true); }
  private:
	asio::steady_timer changed_;
	std::optional<client::ResponseParser> parser_;
	std::function<void()> completion_;
	std::exception_ptr failure_;
	bool armed_ = false, held_ = false, read_ = false;
	std::size_t observed_ = 0, delivered_ = 0;
	asio::awaitable<void> wait(bool read) {
		while (!(read ? read_ : held_)) {
			if (failure_) std::rethrow_exception(failure_);
			changed_.expires_at(std::chrono::steady_clock::time_point::max());
			asio::error_code error;
			co_await changed_.async_wait(asio::redirect_error(asio::use_awaitable, error));
			const auto cancellation = co_await asio::this_coro::cancellation_state;
			if (cancellation.cancelled() != asio::cancellation_type::none)
				throw std::system_error(asio::error::operation_aborted);
		}
		if (failure_) std::rethrow_exception(failure_);
	}
};
template <class Stream> class HeldWriteStream {
  public:
	using executor_type = typename Stream::executor_type;
	HeldWriteStream(Stream stream, std::shared_ptr<WriteGate> gate)
		: stream_(std::move(stream)), gate_(std::move(gate)) {}
	auto get_executor() noexcept { return stream_.get_executor(); }
	auto &lowest_layer() noexcept { return stream_.lowest_layer(); }
	template <class Buffers, class Token> auto async_read_some(Buffers buffers, Token &&token) {
		return asio::async_compose<Token, void(asio::error_code, std::size_t)>(
			[this, buffers, started = false](auto &self, asio::error_code error = {}, std::size_t bytes = 0) mutable {
				if (!std::exchange(started, true)) { stream_.async_read_some(buffers, std::move(self)); return; }
				gate_->observe_read(bytes);
				self.complete(error, bytes);
			}, token, stream_);
	}
	template <class Buffers, class Token> auto async_write_some(Buffers buffers, Token &&token) {
		return asio::async_compose<Token, void(asio::error_code, std::size_t)>(
			[this, buffers, started = false](auto &self, asio::error_code error = {}, std::size_t bytes = 0) mutable {
				if (!std::exchange(started, true)) { stream_.async_write_some(buffers, std::move(self)); return; }
				gate_->complete_write(buffers, error, bytes, std::move(self));
			}, token, stream_);
	}
  private:
	Stream stream_;
	std::shared_ptr<WriteGate> gate_;
};
} // namespace http::test
