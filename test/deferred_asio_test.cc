#include "http_asio.h"
#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
using namespace http;
using namespace std::chrono_literals;
int failures = 0;
void check(bool value, const char *message) {
	if (!value) {
		++failures;
		std::cerr << "FAIL: " << message << '\n';
	}
}
struct Record {
	std::atomic<unsigned> settlements = 0;
	std::atomic<int> reason = -1;
	std::weak_ptr<char> request, reply;
	std::atomic<bool> retained_at_settlement = false, reply_expected = false;
};
struct Pending {
	std::unique_ptr<DeferredReply> reply;
	std::shared_ptr<Record> record;
};
struct App : HttpHandler {
	std::mutex mutex;
	std::condition_variable ready;
	std::deque<Pending> pending;
	std::atomic<unsigned> fast = 0;
	bool should_offload(const Request &) const override { return false; }
	void handle(const Request &request, ResponseWriter &writer) override {
		if (request.path == "/fast") {
			++fast;
			writer.send(200, "sync");
			return;
		}
		auto record = std::make_shared<Record>();
		auto lease = std::make_shared<char>();
		record->request = lease;
		DeferredOptions options;
		options.deadline = std::chrono::steady_clock::now() +
						   (request.path == "/timeout" || request.path == "/large" ? 200ms : 5s);
		options.request_retention = lease;
		options.on_settled = [record](ReplySettlement reason) {
			record->retained_at_settlement =
				!record->request.expired() && (!record->reply_expected || !record->reply.expired());
			record->reason = static_cast<int>(reason);
			++record->settlements;
		};
		auto capability = writer.defer(std::move(options));
		if (!capability) {
			throw std::runtime_error("Asio writer has no owned reply capability");
		}
		if (request.path == "/inline" || request.path == "/throw-ready") {
			Response response;
			response.body = "inline";
			capability->complete(std::move(response));
		}
		{
			std::lock_guard lock(mutex);
			pending.push_back({std::move(capability), record});
		}
		ready.notify_one();
		if (request.path == "/throw" || request.path == "/throw-ready") {
			throw std::runtime_error("after deferral");
		}
	}
	Pending take() {
		std::unique_lock lock(mutex);
		if (!ready.wait_for(lock, 2s, [&] { return !pending.empty(); })) {
			throw std::runtime_error("deferred handler did not enqueue capability");
		}
		auto result = std::move(pending.front());
		pending.pop_front();
		return result;
	}
};
unsigned short available_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in address{};
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address))) {
		::close(fd);
		throw std::runtime_error("probe bind failed");
	}
	socklen_t size = sizeof(address);
	::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &size);
	::close(fd);
	return ntohs(address.sin_port);
}
struct Server {
	std::shared_ptr<App> app;
	std::unique_ptr<HttpAsioService> service;
	unsigned short port;
	explicit Server(std::shared_ptr<App> application = std::make_shared<App>())
		: app(std::move(application)), service(std::make_unique<HttpAsioService>(*app, 1, 0, 64)),
		  port(available_port()) {
		service->start(port);
	}
	~Server() {
		service->stop();
		service.reset();
	}
};
struct Client {
	int fd;
	std::string buffered;
	explicit Client(unsigned short port, bool small = false) : fd(-1) {
		sockaddr_in address{};
		address.sin_family = AF_INET;
		address.sin_port = htons(port);
		address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		for (unsigned attempt = 0; attempt != 200; ++attempt) {
			fd = ::socket(AF_INET, SOCK_STREAM, 0);
			if (fd < 0) {
				throw std::runtime_error("client socket failed");
			}
			timeval timeout{2, 0};
			::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
			if (small) {
				int size = 4096;
				::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
			}
			if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0) {
				return;
			}
			::close(fd);
			fd = -1;
			std::this_thread::sleep_for(5ms);
		}
		throw std::runtime_error("client connect failed");
	}
	~Client() {
		if (fd >= 0) {
			::close(fd);
		}
	}
	void send(std::string_view request) {
		while (!request.empty()) {
			auto count = ::send(fd, request.data(), request.size(), 0);
			if (count <= 0) {
				throw std::runtime_error("client send failed");
			}
			request.remove_prefix(static_cast<std::size_t>(count));
		}
	}
	void request(std::string_view path, bool keep = false) {
		send("GET " + std::string(path) +
			 " HTTP/1.1\r\nHost: localhost\r\nConnection: " + (keep ? "keep-alive" : "close") + "\r\n\r\n");
	}
	Response read() {
		auto receive = [&] {
			char bytes[8192];
			auto count = ::recv(fd, bytes, sizeof(bytes), 0);
			if (count <= 0) {
				return false;
			}
			buffered.append(bytes, static_cast<std::size_t>(count));
			return true;
		};
		while (buffered.find("\r\n\r\n") == std::string::npos) {
			if (!receive()) {
				Response r;
				r.status = 0;
				return r;
			}
		}
		auto end = buffered.find("\r\n\r\n");
		auto length = buffered.find("Content-Length: ");
		if (length == std::string::npos || length > end) {
			throw std::runtime_error("deferred response has no length");
		}
		auto bytes = std::stoull(buffered.substr(length + 16));
		Response result;
		result.status = std::stoi(buffered.substr(9, 3));
		while (buffered.size() < end + 4 + bytes) {
			if (!receive()) {
				throw std::runtime_error("short buffered response");
			}
		}
		result.body.assign(buffered, end + 4, static_cast<std::size_t>(bytes));
		buffered.erase(0, end + 4 + static_cast<std::size_t>(bytes));
		return result;
	}
	void reset() {
		linger value{1, 0};
		::setsockopt(fd, SOL_SOCKET, SO_LINGER, &value, sizeof(value));
		::close(fd);
		fd = -1;
	}
};
void wait_settlement(const std::shared_ptr<Record> &record) {
	auto deadline = std::chrono::steady_clock::now() + 2s;
	while ((!record->settlements || !record->request.expired() ||
			(record->reply_expected && !record->reply.expired())) &&
		   std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(1ms);
	}
	check(record->settlements == 1 && record->request.expired() && record->retained_at_settlement,
		  "transport settles one response and releases its retained request lease");
}
void complete(Pending &pending, std::string body) {
	auto lease = std::make_shared<char>();
	pending.record->reply = lease;
	pending.record->reply_expected = true;
	Response response;
	response.body = std::move(body);
	check(pending.reply->complete(std::move(response), lease) == ReplySubmit::Accepted,
		  "owned response moves into its live transport slot");
}
void normal() {
	Server server;
	for (const char *route : {"/inline", "/throw-ready"}) {
		Client client(server.port);
		client.request(route);
		auto pending = server.app->take();
		auto response = client.read();
		check(response.status == 200 && response.body == "inline",
			  "early owned completion survives handler return and subsequent exception");
		wait_settlement(pending.record);
	}
	{
		Client held(server.port);
		held.request("/late");
		auto pending = server.app->take();
		Client fast(server.port);
		fast.request("/fast");
		check(fast.read().body == "sync" && pending.record->settlements == 0,
			  "deferred handler releases the reactor while response interest remains outstanding");
		std::thread producer([&] { complete(pending, std::string(65536, 'r')); });
		producer.join();
		auto response = held.read();
		check(response.status == 200 && response.body == std::string(65536, 'r'),
			  "cross-thread completion writes exact owned response bytes");
		wait_settlement(pending.record);
		check(pending.record->reply.expired(), "successful original write releases egress retention");
	}
	{
		Client client(server.port);
		client.request("/half-close");
		auto pending = server.app->take();
		::shutdown(client.fd, SHUT_WR);
		complete(pending, "half");
		check(client.read().body == "half", "TCP sending half-close preserves deferred reply interest");
		wait_settlement(pending.record);
	}
	{
		Client client(server.port);
		auto fast_before = server.app->fast.load();
		client.send("GET /pipeline HTTP/1.1\r\nHost: x\r\n\r\nGET /fast HTTP/1.1\r\nHost: x\r\nConnection: "
					"close\r\n\r\n");
		auto pending = server.app->take();
		check(server.app->fast == fast_before,
			  "pipelined handler cannot overtake deferred transport settlement");
		complete(pending, "first");
		check(client.read().body == "first" && client.read().body == "sync",
			  "keep-alive preserves buffered pipeline bytes and ordered handlers");
		wait_settlement(pending.record);
	}
}
void terminal() {
	Server server;
	for (const char *route : {"/timeout", "/abandon", "/throw"}) {
		Client client(server.port);
		client.request(route);
		auto pending = server.app->take();
		if (std::string_view(route) == "/abandon") {
			pending.reply.reset();
		}
		check(client.read().status == 0,
			  "expired, abandoned or throwing deferred request closes without fabricated success");
		wait_settlement(pending.record);
		if (pending.reply) {
			check(pending.reply->complete({}) == ReplySubmit::Closed,
				  "late completion cannot revive a settled connection");
		}
	}
	{
		Client client(server.port);
		client.request("/reset");
		auto pending = server.app->take();
		client.reset();
		wait_settlement(pending.record);
		check(pending.record->reason == static_cast<int>(ReplySettlement::Disconnected) &&
				  pending.reply->complete({}) == ReplySubmit::Closed,
			  "reset before submission closes response interest and rejects late publication");
	}
	{
		Client client(server.port, true);
		client.request("/large");
		auto pending = server.app->take();
		complete(pending, std::string(16 * 1024 * 1024, 's'));
		check(!pending.record->reply.expired(),
			  "slow reader keeps accepted egress lease charged while original write waits");
		wait_settlement(pending.record);
		check(pending.record->reason == static_cast<int>(ReplySettlement::TimedOut) &&
				  pending.record->reply.expired(),
			  "partial-write deadline cancels and reaps before egress settlement");
	}
}
void shutdown() {
	auto app = std::make_shared<App>();
	auto server = std::make_unique<Server>(app);
	Client client(server->port);
	client.request("/escaped");
	auto pending = app->take();
	server.reset();
	wait_settlement(pending.record);
	check(pending.record->reason == static_cast<int>(ReplySettlement::Shutdown) &&
			  pending.reply->complete({}) == ReplySubmit::Closed,
		  "service destruction invalidates escaped capability before runtime release");
	server = std::make_unique<Server>(app);
	Client slow(server->port, true);
	slow.request("/slow-shutdown");
	auto writing = app->take();
	complete(writing, std::string(16 * 1024 * 1024, 'w'));
	char observed[128];
	check(::recv(slow.fd, observed, sizeof(observed), 0) > 0 && writing.record->settlements == 0,
		  "shutdown fixture holds an original partial socket write");
	server.reset();
	wait_settlement(writing.record);
	check(writing.record->reason == static_cast<int>(ReplySettlement::Shutdown) &&
			  writing.record->reply.expired(),
		  "shutdown reaps a partial original write before releasing accepted egress retention");
}
} // namespace
int main() {
	try {
		normal();
		terminal();
		shutdown();
	} catch (const std::exception &error) {
		check(false, error.what());
	}
	std::cout << "deferred Asio failures: " << failures << '\n';
	return failures ? 1 : 0;
}
