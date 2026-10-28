/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#include "http_client_posix.h"
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <memory>
#include <poll.h>
#include <unistd.h>

namespace http::client {
namespace {
class ResolveCategory final : public std::error_category {
  public:
	const char *name() const noexcept override { return "getaddrinfo"; }
	std::string message(int code) const override { return ::gai_strerror(code); }
};
[[noreturn]] void socket_error(ErrorKind kind, const char *message, int error = errno) {
	throw Error(kind, message, {error, std::generic_category()});
}
int control(int socket, int command, int flags = 0) {
	int result;
	do { result = ::fcntl(socket, command, flags); } while (result == -1 && errno == EINTR);
	return result;
}
void configure(int socket) {
	const auto status = control(socket, F_GETFL);
	if (status == -1 || control(socket, F_SETFL, status | O_NONBLOCK) == -1)
		socket_error(ErrorKind::Connect, "HTTP nonblocking socket setup failed");
	const auto descriptor = control(socket, F_GETFD);
	if (descriptor == -1 || control(socket, F_SETFD, descriptor | FD_CLOEXEC) == -1)
		socket_error(ErrorKind::Connect, "HTTP socket close-on-exec setup failed");
#ifdef SO_NOSIGPIPE
	const int enabled = 1;
	if (::setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) == -1)
		socket_error(ErrorKind::Connect, "HTTP socket signal setup failed");
#endif
}
} // namespace
std::size_t resolve(std::string_view host, std::uint16_t port, std::span<Endpoint> output, Deadline deadline) {
	if (host.empty() || host.size() > 253 || !port || output.empty() ||
		!std::all_of(host.begin(), host.end(), [](unsigned char c) { return c > 32 && c < 127; }))
		throw Error(ErrorKind::Options, "invalid HTTP resolution options");
	(void)deadline.remaining();
	std::array<char, 254> name{};
	std::copy(host.begin(), host.end(), name.begin());
	std::array<char, 6> service{};
	const auto converted = std::to_chars(service.data(), service.data() + service.size() - 1, port);
	if (converted.ec != std::errc{}) throw Error(ErrorKind::Options, "invalid HTTP endpoint port");
	addrinfo hints{};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	addrinfo *addresses = nullptr;
	const auto result = ::getaddrinfo(name.data(), service.data(), &hints, &addresses);
	const auto system_error = errno;
	std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> owner(addresses, ::freeaddrinfo);
	(void)deadline.remaining();
	if (result != 0) {
		static const ResolveCategory category;
		throw Error(ErrorKind::Resolve, "HTTP address resolution failed", result == EAI_SYSTEM
			? std::error_code(system_error, std::generic_category()) : std::error_code(result, category));
	}
	std::size_t count = 0;
	for (const auto *address = addresses; address; address = address->ai_next) {
		if (!((address->ai_family == AF_INET && address->ai_addrlen == sizeof(sockaddr_in)) ||
			(address->ai_family == AF_INET6 && address->ai_addrlen == sizeof(sockaddr_in6)))) continue;
		if (count == output.size()) throw Error(ErrorKind::Limit, "HTTP resolved endpoint bound");
		auto &endpoint = output[count++];
		endpoint = {};
		endpoint.size = static_cast<socklen_t>(address->ai_addrlen);
		std::memcpy(&endpoint.address, address->ai_addr, endpoint.size);
	}
	if (!count) throw Error(ErrorKind::Resolve, "HTTP address resolution returned no TCP endpoint");
	return count;
}
Endpoint Endpoint::numeric(std::string_view address, std::uint16_t port) {
	if (address.empty() || address.size() >= INET6_ADDRSTRLEN || !port || address.find('\0') != std::string_view::npos)
		throw Error(ErrorKind::Options, "invalid numeric HTTP endpoint");
	std::array<char, INET6_ADDRSTRLEN> text{};
	std::copy(address.begin(), address.end(), text.begin());
	Endpoint endpoint;
	sockaddr_in ipv4{};
#if defined(__APPLE__) || defined(__FreeBSD__)
	ipv4.sin_len = sizeof(ipv4);
#endif
	ipv4.sin_family = AF_INET; ipv4.sin_port = htons(port);
	if (::inet_pton(AF_INET, text.data(), &ipv4.sin_addr) == 1) {
		endpoint.size = sizeof(ipv4);
		std::memcpy(&endpoint.address, &ipv4, sizeof(ipv4));
		return endpoint;
	}
	sockaddr_in6 ipv6{};
#if defined(__APPLE__) || defined(__FreeBSD__)
	ipv6.sin6_len = sizeof(ipv6);
#endif
	ipv6.sin6_family = AF_INET6; ipv6.sin6_port = htons(port);
	if (::inet_pton(AF_INET6, text.data(), &ipv6.sin6_addr) != 1)
		throw Error(ErrorKind::Options, "HTTP endpoint must be numeric or pre-resolved");
	endpoint.size = sizeof(ipv6);
	std::memcpy(&endpoint.address, &ipv6, sizeof(ipv6));
	return endpoint;
}
PosixConnection::PosixConnection(std::span<const Endpoint> endpoints, Deadline deadline) {
	if (endpoints.empty()) throw Error(ErrorKind::Options, "no HTTP endpoints");
	for (const auto &endpoint : endpoints) {
		if (!((endpoint.address.ss_family == AF_INET && endpoint.size == sizeof(sockaddr_in)) ||
			(endpoint.address.ss_family == AF_INET6 && endpoint.size == sizeof(sockaddr_in6))))
			throw Error(ErrorKind::Options, "invalid resolved HTTP endpoint");
	}
	std::error_code last;
	for (const auto &endpoint : endpoints) {
		(void)deadline.remaining();
		int type = SOCK_STREAM;
#ifdef SOCK_CLOEXEC
		type |= SOCK_CLOEXEC;
#endif
		socket_ = ::socket(endpoint.address.ss_family, type, IPPROTO_TCP);
		if (socket_ == -1) { last = {errno, std::generic_category()}; continue; }
		try {
			configure(socket_);
			if (::connect(socket_, reinterpret_cast<const sockaddr *>(&endpoint.address), endpoint.size) == -1) {
				if (errno != EINPROGRESS && errno != EINTR && errno != EWOULDBLOCK)
					socket_error(ErrorKind::Connect, "HTTP connect failed");
				wait(POLLOUT, deadline);
				int error = 0;
				socklen_t size = sizeof(error);
				if (::getsockopt(socket_, SOL_SOCKET, SO_ERROR, &error, &size) == -1)
					socket_error(ErrorKind::Connect, "HTTP connect result failed");
				if (error) socket_error(ErrorKind::Connect, "HTTP connect failed", error);
			}
			return;
		} catch (const Error &error) {
			::close(std::exchange(socket_, -1));
			if (error.kind() == ErrorKind::Deadline) throw;
			last = error.cause();
		} catch (...) {
			::close(std::exchange(socket_, -1));
			throw;
		}
	}
	throw Error(ErrorKind::Connect, "HTTP endpoints exhausted", last);
}
PosixConnection::~PosixConnection() { close(); }
void PosixConnection::close() noexcept {
	if (socket_ != -1) ::close(std::exchange(socket_, -1));
}
std::optional<ErrorKind> PosixConnection::input_error() const noexcept {
	if (socket_ == -1) return ErrorKind::Transport;
	char byte;
	int flags = MSG_PEEK;
#ifdef MSG_DONTWAIT
	flags |= MSG_DONTWAIT;
#endif
	const auto bytes = ::recv(socket_, &byte, 1, flags);
	if (bytes > 0) return ErrorKind::Framing;
	if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
	return ErrorKind::Transport;
}
PosixConnection::PosixConnection(PosixConnection &&other) noexcept
	: socket_(std::exchange(other.socket_, -1)) {}
void PosixConnection::wait(short events, Deadline deadline) {
	for (;;) {
		pollfd descriptor{socket_, events, 0};
		const auto result = ::poll(&descriptor, 1, deadline.poll_milliseconds());
		if (result == -1) {
			if (errno == EINTR) continue;
			socket_error(ErrorKind::Transport, "HTTP socket poll failed");
		}
		if (result == 0) { (void)deadline.remaining(); continue; }
		if (descriptor.revents & POLLNVAL) socket_error(ErrorKind::Transport, "invalid HTTP socket", EBADF);
		(void)deadline.remaining();
		return;
	}
}
void PosixConnection::write(std::span<const char> bytes, Deadline deadline) {
	while (!bytes.empty()) {
		(void)deadline.remaining();
		int flags = 0;
#ifdef MSG_NOSIGNAL
		flags = MSG_NOSIGNAL;
#endif
		const auto sent = ::send(socket_, bytes.data(), std::min<std::size_t>(bytes.size(), std::numeric_limits<ssize_t>::max()), flags);
		if (sent > 0) { bytes = bytes.subspan(static_cast<std::size_t>(sent)); continue; }
		if (sent == -1 && errno == EINTR) continue;
		if (sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) { wait(POLLOUT, deadline); continue; }
		socket_error(ErrorKind::Transport, "HTTP socket write failed", sent == 0 ? EPIPE : errno);
	}
}
std::size_t PosixConnection::read(std::span<char> bytes, Deadline deadline) {
	if (bytes.empty()) return 0;
	for (;;) {
		(void)deadline.remaining();
		const auto received = ::recv(socket_, bytes.data(), std::min<std::size_t>(bytes.size(), std::numeric_limits<ssize_t>::max()), 0);
		if (received >= 0) return static_cast<std::size_t>(received);
		if (errno == EINTR) continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK) { wait(POLLIN, deadline); continue; }
		socket_error(ErrorKind::Transport, "HTTP socket read failed");
	}
}
BufferedResponse exchange(PosixConnection &connection, PreparedRequest request, Deadline deadline,
	Limits limits, ResponsePolicy policy) {
	try {
		if (const auto error = connection.input_error())
			throw Error(*error, "HTTP connection has unconsumed input or is closed");
		policy.head_request = request.head_request();
		BufferedReceiver receiver(limits, policy, request.context());
		const auto wire = request.wire();
		connection.write({wire.data(), wire.size()}, deadline);
		// Batch socket reads independently of the bounded parser feed.
		std::array<char, 16 * 1024> buffer;
		while (!receiver.done()) {
			const auto size = connection.read(buffer, deadline);
			if (!size) { receiver.eof(); break; }
			auto input = std::span<const char>(buffer.data(), size);
			while (!input.empty()) receiver.feed(input);
		}
		auto response = receiver.take_response();
		response.reusable = response.reusable && !connection.input_error();
		if (!response.reusable) connection.close();
		return response;
	} catch (...) {
		connection.close();
		throw;
	}
}
} // namespace http::client
