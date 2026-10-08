#pragma once
#include "http_owned_storage.h"
#include "http_owned_stream.h"
#include <asio.hpp>
#include <limits>
#include <reactor.h>
namespace http::detail {
class AsioOwnedStreamControl : public reactor::Abortable,
							   public std::enable_shared_from_this<AsioOwnedStreamControl> {
  public:
	static std::shared_ptr<AsioOwnedStreamControl> create(asio::any_io_executor executor,
														  StreamOptions options) {
		auto resource = options.resource;
		auto control = std::allocate_shared<AsioOwnedStreamControl>(
			StreamAllocator<AsioOwnedStreamControl>(resource), std::move(executor));
		std::weak_ptr<AsioOwnedStreamControl> weak = control;
		control->exchange = std::allocate_shared<OwnedStreamExchange>(
			StreamAllocator<OwnedStreamExchange>(resource), std::move(options), [weak] {
				if (auto owner = weak.lock())
					owner->notify();
			});
		return control;
	}
	explicit AsioOwnedStreamControl(asio::any_io_executor executor)
		: executor_(executor), timer_(std::make_unique<asio::steady_timer>(executor)),
		  cancellation_(std::make_unique<asio::cancellation_signal>()) {}
	void abort() override { exchange->close(ReplySettlement::Shutdown); }
	void disable() noexcept {
		std::lock_guard lock(gate_);
		active_ = false;
		executor_.reset();
		timer_.reset();
		cancellation_.reset();
	}
	asio::awaitable<void> wait() {
		while (!exchange->next_ready()) {
			timer_->expires_at(exchange->deadline());
			asio::error_code error;
			co_await timer_->async_wait(asio::redirect_error(asio::use_awaitable, error));
			if (!error)
				exchange->close(ReplySettlement::TimedOut);
			else if (error != asio::error::operation_aborted)
				exchange->close(ReplySettlement::Failed);
			else {
				auto state = co_await asio::this_coro::cancellation_state;
				if (state.cancelled() != asio::cancellation_type::none)
					co_return;
			}
		}
	}
	asio::cancellation_slot write_cancellation() { return cancellation_->slot(); }
	std::shared_ptr<OwnedStreamExchange> exchange;

  private:
	void notify() {
		std::lock_guard lock(gate_);
		if (!active_)
			return;
		std::weak_ptr<AsioOwnedStreamControl> weak = shared_from_this();
		asio::post(*executor_, [weak] {
			if (auto control = weak.lock()) {
				{
					std::lock_guard lock(control->gate_);
					if (!control->active_)
						return;
				}
				control->timer_->cancel();
				if (control->exchange->phase() == OwnedStreamExchange::Phase::Closing)
					control->cancellation_->emit(asio::cancellation_type::all);
			}
		});
	}
	std::mutex gate_;
	bool active_ = true;
	std::optional<asio::any_io_executor> executor_;
	std::unique_ptr<asio::steady_timer> timer_;
	std::unique_ptr<asio::cancellation_signal> cancellation_;
};
} // namespace http::detail
