#include "http_asio.h"
#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
struct App : http::HttpHandler {
	std::mutex mutex;
	std::condition_variable changed;
	bool entered = false, release = false;
	bool should_offload(const http::Request &request) const override {
		return request.path != "/inline-error";
	}
	void handle(const http::Request &request, http::ResponseWriter &response) override {
		if (request.path == "/inline-error") {
			throw std::runtime_error("inline failure");
		}
		std::unique_lock lock(mutex);
		entered = true;
		changed.notify_all();
		changed.wait_for(lock, std::chrono::seconds(2), [&] { return release; });
		response.send(200, "done");
	}
};
unsigned short available_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in address{};
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address))) {
		throw std::runtime_error("probe bind failed");
	}
	socklen_t size = sizeof(address);
	::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &size);
	::close(fd);
	return ntohs(address.sin_port);
}
int request(unsigned short port, std::string_view path) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in address{};
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = htons(port);
	timeval timeout{3, 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
	bool connected = false;
	for (unsigned attempt = 0; attempt != 100; ++attempt) {
		if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0) {
			connected = true;
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	if (!connected) {
		::close(fd);
		return 0;
	}
	std::string bytes = "GET " + std::string(path) + " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
	::send(fd, bytes.data(), bytes.size(), 0);
	bytes.clear();
	char buffer[1024];
	for (;;) {
		auto count = ::recv(fd, buffer, sizeof(buffer), 0);
		if (count <= 0) {
			break;
		}
		bytes.append(buffer, static_cast<std::size_t>(count));
	}
	::close(fd);
	return bytes.starts_with("HTTP/1.1 ") ? std::stoi(bytes.substr(9, 3)) : 0;
}
} // namespace
int main() {
	App app;
	http::HttpAsioService service(app, 1, 1, 1);
	auto port = available_port();
	service.start(port);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	int inline_status = request(port, "/inline-error");
	std::atomic<int> first = 0, second = 0;
	std::thread held([&] { first = request(port, "/held"); });
	{
		std::unique_lock lock(app.mutex);
		app.changed.wait_for(lock, std::chrono::seconds(2), [&] { return app.entered; });
	}
	std::thread excess([&] { second = request(port, "/excess"); });
	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
	while (!second && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	bool shed_before_release = second == 503;
	{
		std::lock_guard lock(app.mutex);
		app.release = true;
	}
	app.changed.notify_all();
	held.join();
	excess.join();
	service.stop();
	std::cout << "inline=" << inline_status << " first=" << first << " second=" << second
			  << " shed_before_release=" << shed_before_release << '\n';
	return inline_status == 500 && first == 200 && second == 503 && shed_before_release ? 0 : 1;
}
