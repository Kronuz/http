// Helpers for test handlers that wait on their own loop: a task handler awaits Asio
// operations with task::await_asio, on an I/O object of the loop it runs on
// (Request::scheduler().executor()).
#pragma once

#include <chrono>
#include <stdexcept>

#include "http_asio.h"

// Suspends the calling task for `d` on `loop`; true when the timer expired.
inline task::task<bool> sleep_for(asio::any_io_executor loop, std::chrono::steady_clock::duration d) {
	asio::steady_timer timer(loop, d);
	const asio::error_code error = co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); });
	co_return !error;
}

