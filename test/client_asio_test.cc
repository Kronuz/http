#include "http_client_asio.h"
#include <asio/use_future.hpp>
#include <iostream>

namespace client = http::client;
void check(bool value) {
	if (!value) throw std::runtime_error("HTTP Asio client invariant failed");
}
asio::awaitable<int> success() { co_return 7; }
asio::awaitable<void> empty_success() { co_return; }
asio::awaitable<int> failure() { throw std::runtime_error("original failure"); co_return 0; }
asio::awaitable<int> original(std::shared_ptr<int> owner) {
	const auto executor = co_await asio::this_coro::executor;
	asio::steady_timer timer(executor, std::chrono::seconds(30));
	asio::error_code error;
	*owner = 1;
	co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, error));
	check(error == asio::error::operation_aborted);
	co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
	co_await asio::this_coro::throw_if_cancelled(false);
	co_await asio::post(executor, asio::use_awaitable);
	*owner = 2;
	throw std::system_error(error);
}
asio::awaitable<void> exercise() {
	const client::Deadline deadline(std::chrono::milliseconds(1000));
	check(co_await client::before(deadline, success()) == 7);
	co_await client::before(deadline, empty_success());
	bool failed = false;
	try { (void)co_await client::before(deadline, failure()); }
	catch (const std::runtime_error &error) { failed = std::string_view(error.what()) == "original failure"; }
	check(failed);
	auto owner = std::make_shared<int>(0);
	bool expired = false;
	try { (void)co_await client::before(client::Deadline(std::chrono::milliseconds(100)), original(owner)); }
	catch (const client::Error &error) { expired = error.kind() == client::ErrorKind::Deadline; }
	check(expired && *owner == 2 && owner.use_count() == 1);
}
int main() {
	asio::io_context io;
	auto result = asio::co_spawn(io, exercise(), asio::use_future);
	io.run(); result.get();
	io.restart();
	asio::cancellation_signal cancellation;
	auto owner = std::make_shared<int>(0);
	bool cancelled = false;
	asio::co_spawn(io, client::before(client::Deadline(std::chrono::milliseconds(1000)), original(owner)),
		asio::bind_cancellation_slot(cancellation.slot(), [&](std::exception_ptr error, int) {
			check(error != nullptr);
			try { std::rethrow_exception(error); }
			catch (const client::Error &failure) { cancelled = failure.kind() == client::ErrorKind::Cancelled; }
		}));
	asio::post(io, [&] { cancellation.emit(asio::cancellation_type::all); });
	io.run();
	check(cancelled && *owner == 2 && owner.use_count() == 1);
	std::cout << "HTTP Asio success, original errors, timeout and cancellation join checks passed\n";
}
