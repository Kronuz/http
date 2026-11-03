// Helpers for test handlers that wait on their own loop: a task handler awaits Asio
// operations with task::await_asio, on an I/O object of the loop it runs on.
#pragma once

#include <chrono>
#include <stdexcept>

#include "http_asio.h"

// The executor of the service's reactor whose loop runs the caller (a handler).
inline asio::any_io_executor this_loop(http::HttpAsioService& service) {
	for (std::size_t i = 0; i < service.reactors(); ++i) {
		if (service.reactor(i).on_loop()) { return service.reactor(i).io().get_executor(); }
	}
	throw std::logic_error("not on a reactor loop");
}

// Suspends the calling task for `d` on `loop`; true when the timer expired.
inline task::task<bool> sleep_for(asio::any_io_executor loop, std::chrono::steady_clock::duration d) {
	asio::steady_timer timer(loop, d);
	const asio::error_code error = co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); });
	co_return !error;
}

// Reads the next chunk of a streamed body (BodyStream::async_read) from a task.
inline task::task<asio::error_code> read_chunk(http::BodyStream& body, std::string& chunk) {
	co_return co_await task::await_asio<void(asio::error_code)>([&](auto h) { body.async_read(chunk, std::move(h)); });
}
