#pragma once
#include "http_deferred.h"
#include "http_owned_storage.h"
#include <memory_resource>
#include <string_view>
namespace http {
struct OwnedChunk {
	std::shared_ptr<const void> owner;
	std::string_view bytes;
};
struct StreamOptions {
	DeferredOptions reply;
	std::uint64_t maximum_bytes = 256ull * 1024 * 1024;
	std::size_t chunk_bytes = 65536;
	bool allow_keep_alive = true;
	std::shared_ptr<std::pmr::memory_resource> resource;
	std::function<void()> on_writable;
};
enum class StreamSubmit { Accepted, Busy, Closed, Invalid, TooLarge };
namespace detail {
// Transport owns settlement: close() cancels intent but cannot release an
// original write. The next chunk is admitted only after complete_write().
class OwnedStreamExchange : public std::enable_shared_from_this<OwnedStreamExchange> {
  public:
	enum class Phase { Waiting, Ready, Streaming, Closing, Settled };
	struct Write {
		std::uint64_t serial;
		std::string_view head;
		OwnedChunk chunk;
		std::shared_ptr<OwnedStreamExchange> retention;
		bool chunked = false, terminal = false;
	};
	OwnedStreamExchange(StreamOptions options, std::function<void()> wake)
		: options_(std::move(options)), wake_(std::move(wake)),
		  head_(options_.resource ? options_.resource.get() : std::pmr::get_default_resource()) {
		if (options_.reply.deadline == std::chrono::steady_clock::time_point::max() ||
			!options_.chunk_bytes || !options_.maximum_bytes)
			throw std::invalid_argument("owned stream requires finite limits");
	}
	StreamSubmit start(const Response &metadata, std::optional<std::uint64_t> length, bool keep_alive) {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (closed())
				return StreamSubmit::Closed;
			if (phase_ != Phase::Waiting)
				return StreamSubmit::Busy;
			if (expired())
				return StreamSubmit::Closed;
			if (length && *length > options_.maximum_bytes)
				return StreamSubmit::TooLarge;
			if (!metadata.body.empty() ||
				((metadata.status == 204 || metadata.status == 304) && (!length || *length)))
				return StreamSubmit::Invalid;
			auto validation = validate_reply(metadata, options_.reply.limits);
			if (validation != ReplySubmit::Accepted)
				return validation == ReplySubmit::TooLarge ? StreamSubmit::TooLarge : StreamSubmit::Invalid;
			// Build only after validation. A failed allocation leaves a retryable
			// Waiting exchange and never exposes a partial header.
			std::pmr::string head(head_.get_allocator());
			head = "HTTP/1.1 ";
			head += std::to_string(metadata.status);
			head += ' ';
			head += reason_phrase(metadata.status);
			head += "\r\nContent-Type: ";
			head += metadata.content_type;
			head += "\r\n";
			for (const auto &[name, value] : metadata.headers) {
				head += name;
				head += ": ";
				head += value;
				head += "\r\n";
			}
			if (metadata.status != 304 && metadata.status != 204) {
				if (length) {
					head += "Content-Length: ";
					head += std::to_string(*length);
					head += "\r\n";
				} else
					head += "Transfer-Encoding: chunked\r\n";
			}
			close_after_ = !keep_alive || !options_.allow_keep_alive || metadata.close;
			head += !close_after_ ? "Connection: keep-alive\r\n\r\n" : "Connection: close\r\n\r\n";
			head_.swap(head);
			expected_ = length;
			phase_ = Phase::Ready;
			wake = wake_;
		}
		notify(wake);
		return StreamSubmit::Accepted;
	}
	StreamSubmit publish(OwnedChunk chunk) {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (closed() || expired())
				return StreamSubmit::Closed;
			if (phase_ == Phase::Waiting || pending_ || writing_ || finished_)
				return StreamSubmit::Busy;
			if (!chunk.owner || chunk.bytes.empty())
				return StreamSubmit::Invalid;
			if (chunk.bytes.size() > options_.chunk_bytes ||
				chunk.bytes.size() > expected_.value_or(options_.maximum_bytes) - accepted_)
				return StreamSubmit::TooLarge;
			accepted_ += chunk.bytes.size();
			pending_.emplace(std::move(chunk));
			wake = wake_;
		}
		notify(wake);
		return StreamSubmit::Accepted;
	}
	StreamSubmit finish() {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (closed() || expired())
				return StreamSubmit::Closed;
			if (phase_ == Phase::Waiting || finished_)
				return StreamSubmit::Busy;
			if (expected_ && accepted_ != *expected_)
				return StreamSubmit::Invalid;
			finished_ = true;
			wake = wake_;
		}
		notify(wake);
		return StreamSubmit::Accepted;
	}
	std::optional<Write> take_write() {
		std::lock_guard lock(mutex_);
		if (closed() || writing_ || phase_ == Phase::Waiting || expired())
			return {};
		const bool header = phase_ == Phase::Ready;
		const bool terminal = !expected_ && finished_ && !pending_ && !header && !terminal_sent_;
		if (!header && !pending_ && !terminal)
			return {};
		auto retention = shared_from_this();
		writing_ = true;
		terminal_writing_ = terminal;
		++serial_;
		phase_ = Phase::Streaming;
		original_ = std::move(pending_);
		pending_.reset();
		return Write{serial_,
					 header ? std::string_view(head_) : std::string_view{},
					 original_.value_or(OwnedChunk{}),
					 std::move(retention),
					 !expected_,
					 terminal};
	}
	bool complete_write(std::uint64_t serial, ReplySettlement outcome) {
		std::optional<OwnedChunk> retired;
		std::function<void()> writable, wake;
		{
			std::lock_guard lock(mutex_);
			if (!writing_ || serial != serial_)
				return false;
			writing_ = false;
			if (terminal_writing_ && outcome == ReplySettlement::Written)
				terminal_sent_ = true;
			terminal_writing_ = false;
			retired = std::move(original_);
			original_.reset();
			if (outcome != ReplySettlement::Written && !closed()) {
				phase_ = Phase::Closing;
				reason_ = outcome;
			}
			if (!closed() && !finished_)
				writable = options_.on_writable;
			wake = wake_;
		}
		// Drop the accepted original before permitting the next producer turn.
		retired.reset();
		notify(writable);
		notify(wake);
		return true;
	}
	bool observe_peer_disconnect() {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (closed())
				return false;
			// A peer read closure does not cancel an accepted final write.
			if (!pending_ && phase_ == Phase::Streaming &&
				((expected_ && accepted_ == *expected_) ||
				 (finished_ && (terminal_writing_ || terminal_sent_))))
				return false;
			phase_ = Phase::Closing;
			reason_ = ReplySettlement::Disconnected;
			wake = wake_;
		}
		notify(wake);
		return true;
	}
	bool close(ReplySettlement outcome) {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (closed())
				return false;
			phase_ = Phase::Closing;
			reason_ = outcome;
			wake = wake_;
		}
		notify(wake);
		return true;
	}
	bool settle() {
		std::function<void(ReplySettlement)> hook;
		std::function<void()> wake, writable;
		std::shared_ptr<void> request;
		std::optional<OwnedChunk> pending;
		ReplySettlement reason;
		{
			std::lock_guard lock(mutex_);
			if (phase_ == Phase::Settled || writing_)
				return false;
			if (phase_ != Phase::Closing &&
				!(finished_ && !pending_ && phase_ == Phase::Streaming && (expected_ || terminal_sent_)))
				return false;
			reason = phase_ == Phase::Closing ? reason_ : ReplySettlement::Written;
			phase_ = Phase::Settled;
			reason_ = reason;
			hook = std::move(options_.reply.on_settled);
			request = std::move(options_.reply.request_retention);
			pending = std::move(pending_);
			pending_.reset();
			wake = std::move(wake_);
			writable = std::move(options_.on_writable);
		}
		if (hook) {
			try {
				hook(reason);
			} catch (...) {
			}
		}
		return true;
	}
	Phase phase() const {
		std::lock_guard lock(mutex_);
		return phase_;
	}
	ReplySettlement closing_reason() const {
		std::lock_guard lock(mutex_);
		return reason_;
	}
	auto deadline() const noexcept { return options_.reply.deadline; }
	bool next_ready() const {
		std::lock_guard lock(mutex_);
		return closed() || phase_ == Phase::Ready || pending_.has_value() || finished_;
	}
	bool close_after() const {
		std::lock_guard lock(mutex_);
		return close_after_;
	}

  private:
	static void notify(const std::function<void()> &callback) noexcept {
		if (callback) {
			try {
				callback();
			} catch (...) {
			}
		}
	}
	bool closed() const noexcept { return phase_ == Phase::Closing || phase_ == Phase::Settled; }
	bool expired() noexcept {
		if (std::chrono::steady_clock::now() < options_.reply.deadline)
			return false;
		phase_ = Phase::Closing;
		reason_ = ReplySettlement::TimedOut;
		return true;
	}
	mutable std::mutex mutex_;
	StreamOptions options_;
	std::function<void()> wake_;
	std::pmr::string head_;
	std::optional<OwnedChunk> pending_, original_;
	std::optional<std::uint64_t> expected_;
	std::uint64_t accepted_ = 0, serial_ = 0;
	Phase phase_ = Phase::Waiting;
	ReplySettlement reason_ = ReplySettlement::Failed;
	bool writing_ = false, finished_ = false;
	bool close_after_ = true;
	bool terminal_writing_ = false, terminal_sent_ = false;
};
} // namespace detail
class OwnedStreamReply {
  public:
	explicit OwnedStreamReply(std::shared_ptr<detail::OwnedStreamExchange> exchange)
		: exchange_(std::move(exchange)) {
		if (!exchange_)
			throw std::invalid_argument("null owned stream exchange");
	}
	OwnedStreamReply(const OwnedStreamReply &) = delete;
	OwnedStreamReply &operator=(const OwnedStreamReply &) = delete;
	~OwnedStreamReply() {
		if (!completed_)
			abandon();
	}
	StreamSubmit start(const Response &metadata, std::uint64_t length, bool keep_alive = false) {
		return exchange_->start(metadata, length, keep_alive);
	}
	StreamSubmit start_chunked(const Response &metadata, bool keep_alive = false) {
		return exchange_->start(metadata, std::nullopt, keep_alive);
	}
	StreamSubmit publish(OwnedChunk chunk) { return exchange_->publish(std::move(chunk)); }
	StreamSubmit finish() {
		auto result = exchange_->finish();
		if (result == StreamSubmit::Accepted)
			completed_ = true;
		return result;
	}
	void abandon() noexcept {
		try {
			exchange_->close(ReplySettlement::Abandoned);
		} catch (...) {
		}
	}

  private:
	std::shared_ptr<detail::OwnedStreamExchange> exchange_;
	bool completed_ = false;
};
using OwnedStreamCapability = detail::ResourceUnique<OwnedStreamReply>;
} // namespace http
