/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_client_request.h"
#include "http_client_response.h"
#include <sys/socket.h>

namespace http::client {
inline constexpr std::size_t default_resolved_endpoints = 64;
struct Endpoint {
	sockaddr_storage address{};
	socklen_t size = 0;
	static Endpoint numeric(std::string_view address, std::uint16_t port);
};
// OS name resolution is synchronous and cannot be interrupted by the deadline.
std::size_t resolve(std::string_view host, std::uint16_t port, std::span<Endpoint> output, Deadline deadline);
// All socket waits consume one absolute deadline; endpoints are already resolved.
class PosixConnection {
  public:
	PosixConnection(std::span<const Endpoint> endpoints, Deadline deadline);
	~PosixConnection();
	PosixConnection(const PosixConnection &) = delete;
	PosixConnection &operator=(const PosixConnection &) = delete;
	PosixConnection(PosixConnection &&other) noexcept;
	PosixConnection &operator=(PosixConnection &&) = delete;
	void write(std::span<const char> bytes, Deadline deadline);
	std::size_t read(std::span<char> bytes, Deadline deadline);
  private:
	int socket_ = -1;
	std::optional<ErrorKind> input_error() const noexcept;
	void close() noexcept;
	void wait(short events, Deadline deadline);
	friend BufferedResponse exchange(PosixConnection &, PreparedRequest, Deadline, Limits, ResponsePolicy);
};
BufferedResponse exchange(PosixConnection &connection, PreparedRequest request, Deadline deadline,
	Limits limits = {}, ResponsePolicy policy = {});
} // namespace http::client
