/*
 * kv_store — a tiny REST key/value store, served entirely through the HttpHandler
 * seam. The application is just a Router with a few closures: it never sees
 * llhttp, a socket, or the event loop. Swap this handler for Xapiand's search
 * handler and the same engine becomes a search server.
 *
 *   ./kv_store 8080
 *   curl -XPUT  localhost:8080/kv/greeting -d 'hello'
 *   curl        localhost:8080/kv/greeting           # -> hello
 *   curl -XDELETE localhost:8080/kv/greeting
 *   curl        localhost:8080/kv/                   # -> list keys
 */

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "http_handler.h"
#include "http_server.h"

// The application: an in-memory KV store behind a Router. Pure request->response.
class KvApp : public http::HttpHandler {
	std::mutex mutex_;
	std::map<std::string, std::string> store_;
	http::Router router_;

	static std::string key_of(const http::Request& r) { return r.path.substr(4); }  // strip "/kv/"

public:
	KvApp() {
		router_.route("GET", "/kv/", [this](const http::Request& req, http::Response& resp) {
			std::lock_guard<std::mutex> lk(mutex_);
			std::string key = key_of(req);
			if (key.empty()) {                       // GET /kv/ -> list keys
				std::string body;
				for (const auto& [k, v] : store_) { body += k; body += '\n'; }
				resp.set(200, std::move(body));
				return;
			}
			auto it = store_.find(key);
			if (it == store_.end()) { resp.set(404, "not found\n"); return; }
			resp.set(200, it->second);
		});
		router_.route("PUT", "/kv/", [this](const http::Request& req, http::Response& resp) {
			std::lock_guard<std::mutex> lk(mutex_);
			std::string key = key_of(req);
			if (key.empty()) { resp.set(400, "missing key\n"); return; }
			bool created = store_.find(key) == store_.end();
			store_[key] = req.body;
			resp.set(created ? 201 : 204, created ? "created\n" : "");
		});
		router_.route("DELETE", "/kv/", [this](const http::Request& req, http::Response& resp) {
			std::lock_guard<std::mutex> lk(mutex_);
			resp.set(store_.erase(key_of(req)) ? 204 : 404, "");
		});
	}

	void handle(const http::Request& request, http::Response& response) override {
		router_.handle(request, response);
	}
};


static std::weak_ptr<http::HttpService> g_service;
static void on_signal(int) { if (auto s = g_service.lock()) { s->stop(); } }

int main(int argc, char** argv) {
	unsigned int port = (argc > 1) ? static_cast<unsigned int>(std::atoi(argv[1])) : 8080;

	KvApp app;
	auto service = http::HttpService::create(app);
	g_service = service;

	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);

	service->listen(port);
	std::printf("kv_store: REST key/value server on http://127.0.0.1:%u/kv/ (Ctrl-C to stop)\n", port);
	std::fflush(stdout);

	service->run();

	std::printf("kv_store: stopped.\n");
	return 0;
}
