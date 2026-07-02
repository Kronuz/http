/*
 * kv_store — a tiny REST key/value store, served entirely through the HttpHandler
 * seam. The application is just a Router with a few closures: it never sees
 * http-parser, a socket, or the event loop. Swap this handler for Xapiand's search
 * handler and the same engine becomes a search server.
 *
 *   ./kv_store 8080
 *   curl -XPUT  localhost:8080/kv/greeting -d 'hello'
 *   curl        localhost:8080/kv/greeting           # -> hello
 *   curl -XDELETE localhost:8080/kv/greeting
 *   curl        localhost:8080/kv/                   # -> list keys
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "http_handler.h"
#include "http_router.h"
#include "http_asio.h"

// A streaming request-body intake: counts NDJSON lines as the body arrives,
// without ever holding the whole body in memory. The per-request count rides in
// Request::user_data so the handler can read it once the body is fully received.
struct BulkCtx { long lines = 0; };

class LineCountingSink : public http::BodySink {
	std::shared_ptr<BulkCtx> ctx_;
	std::string partial_;
public:
	explicit LineCountingSink(std::shared_ptr<BulkCtx> ctx) : ctx_(std::move(ctx)) {}
	void write(std::string_view chunk) override {
		partial_.append(chunk);
		std::size_t nl;
		while ((nl = partial_.find('\n')) != std::string::npos) {
			++ctx_->lines;
			partial_.erase(0, nl + 1);
		}
	}
	void end() override {
		if (!partial_.empty()) { ++ctx_->lines; }  // trailing line without a newline
	}
};


// The application: an in-memory KV store behind a Router. Pure request->response.
class KvApp : public http::HttpHandler {
	std::mutex mutex_;
	std::map<std::string, std::string> store_;
	http::Router router_;

public:
	KvApp() {
		router_.route("GET", "/kv/", [this](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
			std::lock_guard<std::mutex> lk(mutex_);   // GET /kv/ -> list keys
			std::string body;
			for (const auto& [k, v] : store_) { body += k; body += '\n'; }
			resp.send(200, body);
		});
		router_.route("GET", "/kv/:key", [this](const http::Request&, http::ResponseWriter& resp, const http::Params& p) {
			std::lock_guard<std::mutex> lk(mutex_);
			auto it = store_.find(std::string(p.get("key")));
			if (it == store_.end()) { resp.send(404, "not found\n"); return; }
			resp.send(200, it->second);
		});
		router_.route("PUT", "/kv/:key", [this](const http::Request& req, http::ResponseWriter& resp, const http::Params& p) {
			std::lock_guard<std::mutex> lk(mutex_);
			std::string key(p.get("key"));
			bool created = store_.find(key) == store_.end();
			store_[key] = req.body;
			resp.send(created ? 201 : 204, created ? "created\n" : "");
		});
		router_.route("DELETE", "/kv/:key", [this](const http::Request&, http::ResponseWriter& resp, const http::Params& p) {
			std::lock_guard<std::mutex> lk(mutex_);
			resp.send(store_.erase(std::string(p.get("key"))) ? 204 : 404, "");
		});
		// A streaming response: emit N lines as chunks, never holding them all in
		// memory. The same seam produces a chunked body -- the writer frames it.
		router_.route("GET", "/stream/:n", [](const http::Request&, http::ResponseWriter& resp, const http::Params& p) {
			long n = std::atol(std::string(p.get("n")).c_str());
			resp.status(200);
			resp.content_type("text/plain; charset=utf-8");
			for (long i = 0; i < n; ++i) {
				resp.write("line " + std::to_string(i) + "\n");
			}
			resp.end();
		});
		// A streamed request body: POST NDJSON; the body is counted line-by-line as
		// it arrives (see on_request_body) and never buffered whole.
		router_.route("POST", "/bulk", [](const http::Request& req, http::ResponseWriter& resp, const http::Params&) {
			auto ctx = std::static_pointer_cast<BulkCtx>(req.user_data);
			resp.send(200, "received " + std::to_string(ctx ? ctx->lines : 0) + " lines\n");
		});
	}

	void handle(const http::Request& request, http::ResponseWriter& response) override {
		router_.handle(request, response);
	}

	// Stream the body for POST /bulk instead of buffering it; everything else
	// buffers (the default). The line count is carried in user_data.
	std::unique_ptr<http::BodySink> on_request_body(http::Request& request) override {
		if (request.method == "POST" && request.path == "/bulk") {
			auto ctx = std::make_shared<BulkCtx>();
			request.user_data = ctx;
			return std::make_unique<LineCountingSink>(ctx);
		}
		return nullptr;
	}
};


static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop.store(true); }

int main(int argc, char** argv) {
	unsigned int port = (argc > 1) ? static_cast<unsigned int>(std::atoi(argv[1])) : 8080;

	KvApp app;
	// The Asio transport: N shared-nothing reactors (thread-per-core), each with a
	// small offload pool for the un-stallable path and a bounded offload window. The
	// default bind (no SO_REUSEPORT) uses the portable shared-acceptor, so this runs
	// the same on Linux and macOS.
	std::size_t reactors = std::max(1u, std::thread::hardware_concurrency());
	http::HttpAsioService service(app, reactors, /*workers=*/2, /*queue_limit=*/256);

	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);

	service.start(static_cast<unsigned short>(port));
	std::printf("kv_store: REST key/value server on http://127.0.0.1:%u/kv/ (%zu reactors, Ctrl-C to stop)\n", port, reactors);
	std::fflush(stdout);

	while (!g_stop.load()) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

	service.stop();
	std::printf("kv_store: stopped.\n");
	return 0;
}
