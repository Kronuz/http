#include "http_client_posix.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <unistd.h>
#include <exception>
#include <iostream>
#include <thread>
#include <future>
#include "support/response_boundary.h"

namespace client = http::client;
void check(bool value) {
	if (!value) throw std::runtime_error("HTTP POSIX client invariant failed");
}
struct Socket {
	int value;
	~Socket() { if (value != -1) ::close(value); }
};
struct JoiningThread {
	std::thread value;
	~JoiningThread() { if (value.joinable()) value.join(); }
};
client::Endpoint bound_endpoint(int socket) {
	auto endpoint = client::Endpoint::numeric("127.0.0.1", 1);
	auto *address = reinterpret_cast<sockaddr_in *>(&endpoint.address);
	address->sin_port = 0;
	check(::bind(socket, reinterpret_cast<const sockaddr *>(&endpoint.address), endpoint.size) == 0);
	check(::getsockname(socket, reinterpret_cast<sockaddr *>(&endpoint.address), &endpoint.size) == 0);
	return endpoint;
}
void queued_response() {
	const Socket listener{::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)};
	check(listener.value >= 0);
	const auto endpoint = bound_endpoint(listener.value);
	check(::listen(listener.value, 1) == 0);
	const auto wire = http_test::response_at_boundary(client::ResponseParser::read_bytes) + http_test::unsolicited;
	std::promise<void> sent, release;
	auto queued = sent.get_future(), permission = release.get_future();
	std::exception_ptr server_error;
	JoiningThread server{std::thread([&] {
		try {
			pollfd descriptor{listener.value, POLLIN, 0};
			check(::poll(&descriptor, 1, 2000) == 1);
			const Socket peer{::accept(listener.value, nullptr, nullptr)};
			check(peer.value >= 0);
#ifdef SO_NOSIGPIPE
			int one = 1;
			check(::setsockopt(peer.value, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == 0);
#endif
			timeval timeout{2, 0};
			check(::setsockopt(peer.value, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
			for (std::size_t offset = 0; offset < wire.size();) {
				int flags = 0;
#ifdef MSG_NOSIGNAL
				flags = MSG_NOSIGNAL;
#endif
				const auto bytes = ::send(peer.value, wire.data() + offset, wire.size() - offset, flags);
				check(bytes > 0);
				offset += static_cast<std::size_t>(bytes);
			}
			sent.set_value();
			(void)permission.wait_for(std::chrono::seconds(2));
		} catch (...) {
			server_error = std::current_exception();
			sent.set_exception(server_error);
		}
	})};
	client::PosixConnection connection(std::span(&endpoint, 1), client::Deadline(std::chrono::seconds(2)));
	check(queued.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
	queued.get();
	bool rejected = false;
	try {
		auto response = client::exchange(connection, client::PreparedRequest({"GET", "localhost", "/"}),
			client::Deadline(std::chrono::seconds(2)));
		rejected = !response.reusable;
	} catch (const client::Error &error) { rejected = error.kind() == client::ErrorKind::Framing; }
	release.set_value();
	server.value.join();
	if (server_error) std::rethrow_exception(server_error);
	check(rejected);
}
int main() {
	queued_response();
	{
		std::array<client::Endpoint, 1> addresses;
		const client::Deadline deadline(std::chrono::milliseconds(1000));
		check(client::resolve("127.0.0.1", 1234, addresses, deadline) == 1);
		check(addresses.front().address.ss_family == AF_INET);
		for (auto host : {std::string_view{}, std::string_view("bad\r\nhost"), std::string_view("127.0.0.1\0bad", 13)}) {
			bool rejected = false;
			try { (void)client::resolve(host, 1234, addresses, deadline); }
			catch (const client::Error &error) { rejected = error.kind() == client::ErrorKind::Options; }
			check(rejected);
		}
	}
	for (auto address : {"127.0.0.1", "::1"}) check(client::Endpoint::numeric(address, 1234).size > 0);
	for (auto address : {"localhost", "127.0.0.1\r\n", "", "[::1]"}) {
		bool rejected = false;
		try { (void)client::Endpoint::numeric(address, 1234); }
		catch (const client::Error &error) { rejected = error.kind() == client::ErrorKind::Options; }
		check(rejected);
	}
	const Socket listener{::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)};
	check(listener.value >= 0);
	const std::array endpoints{client::Endpoint::numeric("255.255.255.255", 1), bound_endpoint(listener.value)};
	check(::listen(listener.value, 1) == 0);
	const std::string payload(2 * 1024 * 1024, 'p');
	client::PreparedRequest prepared({"POST", "localhost", "/upload", {}, payload, true});
	const std::string expected(prepared.wire());
	const std::string response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc";
	std::exception_ptr server_error;
	JoiningThread server{std::thread([&] {
		try {
			pollfd descriptor{listener.value, POLLIN, 0};
			check(::poll(&descriptor, 1, 5000) == 1);
			const Socket peer{::accept(listener.value, nullptr, nullptr)};
			check(peer.value >= 0);
			timeval timeout{5, 0};
			check(::setsockopt(peer.value, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
			check(::setsockopt(peer.value, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
			std::string received;
			std::array<char, 8192> buffer;
			while (received.size() < expected.size()) {
				const auto bytes = ::recv(peer.value, buffer.data(), buffer.size(), 0);
				check(bytes > 0);
				received.append(buffer.data(), static_cast<std::size_t>(bytes));
			}
			check(received == expected);
			for (std::size_t sent = 0; sent < response.size();) {
				const auto bytes = ::send(peer.value, response.data() + sent, response.size() - sent, 0);
				check(bytes > 0); sent += static_cast<std::size_t>(bytes);
			}
		} catch (...) { server_error = std::current_exception(); }
	})};
	const client::Deadline deadline(std::chrono::milliseconds(5000));
	client::PosixConnection original(endpoints, deadline);
	auto connection = std::move(original);
	const auto wire = prepared.wire();
	connection.write({wire.data(), wire.size()}, deadline);
	std::string received;
	std::array<char, 17> buffer;
	for (;;) {
		const auto bytes = connection.read(buffer, deadline);
		if (!bytes) break;
		received.append(buffer.data(), bytes);
	}
	check(received == response);
	server.value.join();
	if (server_error) std::rethrow_exception(server_error);
	JoiningThread stalled{std::thread([&] {
		try {
			pollfd descriptor{listener.value, POLLIN, 0};
			check(::poll(&descriptor, 1, 5000) == 1);
			const Socket peer{::accept(listener.value, nullptr, nullptr)};
			check(peer.value >= 0);
			timeval timeout{5, 0};
			check(::setsockopt(peer.value, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
			char byte;
			check(::recv(peer.value, &byte, 1, 0) == 0);
		} catch (...) { server_error = std::current_exception(); }
	})};
	bool expired = false;
	{
		const client::Deadline short_deadline(std::chrono::milliseconds(100));
		client::PosixConnection waiting{std::span(endpoints).last(1), short_deadline};
		try { (void)waiting.read(buffer, short_deadline); }
		catch (const client::Error &error) { expired = error.kind() == client::ErrorKind::Deadline; }
	}
	stalled.value.join();
	if (server_error) std::rethrow_exception(server_error);
	check(expired);
	std::cout << "HTTP numeric endpoints, address fallback, owned request and nonblocking transfer passed\n";
}
