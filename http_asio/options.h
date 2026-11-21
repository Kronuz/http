#pragma once

// The service's options: bind alias, response transforms, Limits and TLS (what http_asio.h includes first).

#include <asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <coroutine>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <type_traits>
#include <charconv>

#include "http_compression.h"
#include "http_conditional.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_range.h"
#include "http_request_parser.h"
#include "reactor.h"
#include "http_owned_stream_asio.h"
#include "task_asio.hh"

namespace asio::ssl { class context; }   // http_asio_tls.h holds what serves it

namespace http {

// The reactor pool, accept plumbing, admission gate, and graceful shutdown live in the
// generic Kronuz/reactor runtime now; http rides on it. The bind options are the
// runtime's -- alias kept so existing callers (Xapiand, the tests, the bench) are
// unchanged. reuse_port is what lets the N reactors share one port.
using AsioBindOptions = reactor::BindOptions;

// Response transforms applied on the buffered response before it goes on the wire, in
// order: conditional (weak ETag / If-None-Match -> 304), then range (a byte Range ->
// 206 / 416), then compression (Accept-Encoding -> zstd/gzip). Same shape and policy as
// the libev writer had.
struct AsioResponseOptions {
	bool compress = false;
	bool conditional = false;   // derive a weak ETag + answer 304 on If-None-Match
	bool ranges = false;        // serve a byte Range as 206 Partial Content
	CompressionOptions compression{};
};

// Every bound of a service, taken whole by its constructor, plain and TLS alike. Each timeout is
// between zero (off) and 24 hours.
struct Limits {
	std::size_t max_clients = 1024;     // open connections, idle ones counted; more are closed at accept (0 = no bound)
	std::size_t max_in_flight = 1024;   // requests admitted per loop; more are answered 503 (0 = no cap)
	// idle: from a connection's start or a reply's end to the next request's first byte. header:
	// from that byte to the end of the headers. inactivity: per body read and per 64 KiB of a
	// buffered reply, so a slow transfer that moves completes. None bounds a running handler.
	std::chrono::milliseconds idle_timeout{75'000}, header_timeout{30'000}, inactivity_timeout{60'000};
	// Absolute, from the wait for a request's first byte (idle time included) to its reply's end,
	// so trickled bytes cannot renew it.
	std::chrono::milliseconds request_timeout{0};
	std::chrono::milliseconds shutdown_grace{5'000};   // stop() between asking requests to stop and forcing them
	// The largest body buffered into Request::body (0 = no cap); a larger request gets 413.
	// A route that streams its body (wants_body_stream / on_request_body) is not capped.
	std::size_t max_body = 16u << 20;
	std::chrono::milliseconds tls_handshake{5'000};    // from the accept, queue time included
	std::size_t tls_handshakes = 64;                   // handshakes at once, across the loops
};

// Why a TLS connection ended before serving: past the handshake deadline (queue time
// included), the handshake cap, or the handshake's own failure, by kind.
enum class TlsFailure { deadline, handshakes, version, cipher, certificate, protocol, other };
using TlsFailureHook = std::function<void(TlsFailure)>;

// A service's TLS: the context its connections present (none: plain HTTP), and the hook that
// hears each connection that ends before serving, and why. Serving it takes http_asio_tls.h.
struct Tls {
	std::shared_ptr<asio::ssl::context> context;
	TlsFailureHook failed;
};
}  // namespace http
