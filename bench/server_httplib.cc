// http_server_httplib: cpp-httplib answering the same fixed reply as http_server_compare, for the comparison in
// docs/perf/timeouts-cpu.md. cpp-httplib serves each connection on a thread of its pool until the connection ends, so
// the pool is given REACTORS threads, the number of loops the other servers run; the keep-alive request cap is lifted
// so a connection is not closed after 100 requests. The Keep-Alive field is dropped and the two header lines are
// written in the order the other servers use, so the reply is byte for byte theirs.
//
//   http_server_httplib KIND PORT THREADS     KIND is "httplib"
//   http_server_httplib version
//
// SIGUSR2 prints the process's CPU time as http_server_compare does. Built only with -DHTTP_BENCH_HTTPLIB=ON.
#include <httplib.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "bench_common.h"

int main(int argc, char** argv) {
	if (argc > 1 && std::string(argv[1]) == "version") {
		std::printf("cpp-httplib %s\n", CPPHTTPLIB_VERSION);
		return 0;
	}
	int port = argc > 2 ? std::atoi(argv[2]) : 8080;
	std::size_t threads = static_cast<std::size_t>(argc > 3 ? std::atoi(argv[3]) : 1);
	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);
	std::signal(SIGUSR2, on_usr2);

	httplib::Server server;
	server.new_task_queue = [threads] { return new httplib::ThreadPool(threads); };
	server.set_tcp_nodelay(true);
	server.set_keep_alive_max_count(1000000000);
	server.set_post_routing_handler([](const httplib::Request&, httplib::Response& res) { res.headers.erase("Keep-Alive"); });
	server.set_header_writer([](httplib::Stream& strm, httplib::Headers& headers) -> ssize_t {
		std::string out;
		for (const char* name : {"Content-Type", "Content-Length", "Connection"}) {
			auto it = headers.find(name);
			if (it != headers.end()) { out += it->first + ": " + it->second + "\r\n"; }
		}
		out += "\r\n";
		return strm.write(out.data(), out.size());
	});
	server.Get("/", [](const httplib::Request&, httplib::Response& res) {
		res.set_content("Hello, World!\n", "text/plain");
		res.set_header("Connection", "keep-alive");
	});
	if (!server.bind_to_port("0.0.0.0", port)) {
		std::fprintf(stderr, "cannot bind\n");
		return 1;
	}
	std::thread listener([&] { server.listen_after_bind(); });
	server.wait_until_ready();
	std::printf("ready httplib %zu\n", threads);
	std::fflush(stdout);
	while (!g_stop.load()) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
	server.stop();
	listener.join();
	return 0;
}
