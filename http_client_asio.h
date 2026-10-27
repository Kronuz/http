/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_client_request.h"
#include "http_client_response.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <variant>

namespace http::client {
namespace detail {
template <class T> struct AsyncResult {
	std::optional<T> value;
	std::exception_ptr error;
};
template <class T> asio::awaitable<AsyncResult<T>> capture(asio::awaitable<T> operation) {
	try { co_return AsyncResult<T>{co_await std::move(operation), {}}; }
	catch (...) { co_return AsyncResult<T>{{}, std::current_exception()}; }
}
inline asio::awaitable<std::monostate> value_of(asio::awaitable<void> operation) {
	co_await std::move(operation);
	co_return std::monostate{};
}
} // namespace detail
// The race joins the original operation before returning or releasing its owners.
template <class T> asio::awaitable<T> before(Deadline deadline, asio::awaitable<T> operation) {
	(void)deadline.remaining();
	const auto executor = co_await asio::this_coro::executor;
	asio::steady_timer timer(executor, deadline.expires());
	asio::error_code timer_error;
	using namespace asio::experimental::awaitable_operators;
	auto result = co_await (detail::capture(std::move(operation)) ||
		timer.async_wait(asio::redirect_error(asio::use_awaitable, timer_error)));
	if (result.index() != 0)
		throw Error(timer_error ? ErrorKind::Cancelled : ErrorKind::Deadline,
			timer_error ? "HTTP operation cancelled" : "HTTP deadline exceeded", timer_error);
	auto received = std::move(std::get<0>(result));
	if (received.error) {
		try { std::rethrow_exception(received.error); }
		catch (const allocators::ResourcePressure &) { throw Error(ErrorKind::Pressure, "HTTP operation allocation pressure"); }
		catch (const std::bad_alloc &) { throw Error(ErrorKind::Pressure, "HTTP operation allocation failed"); }
		catch (const std::system_error &failure) {
			if (failure.code() == asio::error::operation_aborted)
				throw Error(ErrorKind::Cancelled, "HTTP operation cancelled", failure.code());
			throw;
		}
	}
	co_return std::move(*received.value);
}
inline asio::awaitable<void> before(Deadline deadline, asio::awaitable<void> operation) {
	(void)co_await before(deadline, detail::value_of(std::move(operation)));
}
template <class Stream> asio::awaitable<void> write(Stream &stream, PreparedRequest request) {
	const auto bytes = request.wire();
	try { co_await asio::async_write(stream, asio::buffer(bytes.data(), bytes.size()), asio::use_awaitable); }
	catch (const std::system_error &error) {
		throw Error(error.code() == asio::error::operation_aborted ? ErrorKind::Cancelled : ErrorKind::Transport,
			"HTTP request write failed", error.code());
	}
}
namespace detail {
template <class Stream> asio::awaitable<BufferedResponse> exchange_io(Stream &stream, PreparedRequest request,
	Deadline deadline, Limits limits, ResponsePolicy policy) {
	policy.head_request = request.head_request();
	BufferedReceiver receiver(limits, policy, request.context());
	(void)deadline.remaining();
	co_await write(stream, std::move(request));
	std::array<char, ResponseParser::read_bytes> buffer;
	while (!receiver.done()) {
		(void)deadline.remaining();
		asio::error_code error;
		const auto size = co_await stream.async_read_some(asio::buffer(buffer),
			asio::redirect_error(asio::use_awaitable, error));
		if (size) {
			auto input = std::span<const char>(buffer.data(), size);
			while (!input.empty()) receiver.feed(input);
		}
		if (error == asio::error::eof) receiver.eof();
		else if (error) throw Error(error == asio::error::operation_aborted ? ErrorKind::Cancelled : ErrorKind::Transport,
			"HTTP response read failed", error);
		else if (!size) throw Error(ErrorKind::Transport, "HTTP response read made no progress");
		if (!receiver.done()) co_await asio::post(co_await asio::this_coro::executor, asio::use_awaitable);
	}
	(void)deadline.remaining();
	co_return receiver.take_response();
}
} // namespace detail
template <class Stream> asio::awaitable<BufferedResponse> exchange(Stream &stream, PreparedRequest request,
	Deadline deadline, Limits limits = {}, ResponsePolicy policy = {}) {
	return before(deadline, detail::exchange_io(stream, std::move(request), deadline, limits, policy));
}
} // namespace http::client
