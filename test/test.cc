/*
 * Tests for the HTTP binding of the router and the seam: method dispatch, 404 vs
 * 405, param capture surfaced to a route, query stripping, and that a handler can
 * answer either buffered (send) or streamed (status + multiple write + end). The
 * radix-tree matching itself is covered by radix-router's own suite; here we only
 * exercise what http:: adds on top. Run via ctest (http_test).
 */

#include "http_accept.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_router.h"

#include <cstdio>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

static int g_failures = 0;
static int g_checks = 0;

#define CHECK(cond)                                                                      \
	do {                                                                                 \
		++g_checks;                                                                      \
		if (!(cond)) {                                                                    \
			++g_failures;                                                                 \
			std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);                 \
		}                                                                                \
	} while (0)

// A ResponseWriter that records the logical response a handler produced: the
// status and the concatenation of its body writes (so buffered and streamed
// handlers are both observable). It is above the wire -- HTTP/1.1 framing
// (Content-Length vs chunked) is the connection's job and is smoke-tested
// end-to-end by the kv_store example.
struct RecordingWriter : http::ResponseWriter {
	int status_ = 0;
	std::string body_;
	std::vector<std::pair<std::string, std::string>> headers_;
	bool ended_ = false;
	bool closed_ = false;

	void status(int code) override { status_ = code; }
	void set_header(std::string_view name, std::string_view value) override { headers_.emplace_back(name, value); }
	void write(std::string_view chunk) override { body_.append(chunk); }
	void end() override { ended_ = true; }
	void set_close() override { closed_ = true; }
};

static RecordingWriter run(http::HttpHandler& h, const std::string& method, const std::string& target) {
	http::Request req;
	req.method = method;
	req.target = target;
	auto q = target.find('?');
	req.path = (q == std::string::npos) ? target : target.substr(0, q);
	RecordingWriter w;
	h.handle(req, w);
	return w;
}

static http::Router::Route mark(const char* id, std::vector<std::string> names = {}) {
	std::string sid = id;
	return [sid, names](const http::Request&, http::ResponseWriter& resp, const http::Params& p) {
		std::string body = "id=" + sid;
		for (const auto& n : names) {
			body += " ";
			body += n;
			body += "=";
			body += std::string(p.get(n));
		}
		resp.send(200, body);
	};
}

int main() {
	std::printf("== http::Router + seam tests ==\n");

	http::Router r;
	r.route("GET", "/items", mark("list"));
	r.route("GET", "/items/:id", mark("get", {"id"}));
	r.route("POST", "/items", mark("create"));
	r.route("PUT", "/items/:id", mark("update", {"id"}));
	// a streamed response: status + several writes + end
	r.route("GET", "/multi", [](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
		resp.status(200);
		resp.write("a");
		resp.write("b");
		resp.write("c");
		resp.end();
	});

	// method dispatch + param capture surfaced to the route
	CHECK(run(r, "GET", "/items").body_ == "id=list");
	CHECK(run(r, "GET", "/items/42").body_ == "id=get id=42");
	CHECK(run(r, "POST", "/items").body_ == "id=create");
	CHECK(run(r, "PUT", "/items/42").body_ == "id=update id=42");

	// every successful handler must signal completion
	CHECK(run(r, "GET", "/items").ended_);

	// streamed body is the concatenation of the writes
	{
		RecordingWriter w = run(r, "GET", "/multi");
		CHECK(w.status_ == 200);
		CHECK(w.body_ == "abc");
		CHECK(w.ended_);
	}

	// 404 vs 405
	CHECK(run(r, "GET", "/nope").status_ == 404);       // unknown path
	CHECK(run(r, "DELETE", "/items").status_ == 405);   // known path, unsupported method
	CHECK(run(r, "DELETE", "/items/42").status_ == 405);

	// query string is stripped before matching
	CHECK(run(r, "GET", "/items/42?pretty=1").body_ == "id=get id=42");

	// case-insensitive method
	CHECK(run(r, "get", "/items").body_ == "id=list");

	// malformed pattern is rejected at registration
	bool threw = false;
	try {
		http::Router bad;
		bad.route("GET", "items", mark("x"));   // missing leading '/'
	} catch (const std::invalid_argument&) {
		threw = true;
	}
	CHECK(threw);

	// --- content negotiation (Accept) ---
	{
		std::vector<std::string> produce = {"application/json", "application/x-msgpack", "text/yaml", "text/html"};
		CHECK(http::Accept("application/x-msgpack").best(produce) == "application/x-msgpack");
		CHECK(http::Accept("application/json;q=0.5, application/x-msgpack;q=0.9").best(produce) == "application/x-msgpack");
		CHECK(http::Accept("text/html, */*;q=0.1").best(produce) == "text/html");   // exact beats wildcard
		CHECK(http::Accept("").best(produce) == "application/json");                 // absent Accept -> first available
		CHECK(http::Accept("image/png").best(produce).empty());                      // 406: nothing acceptable
		http::Accept a("text/*;q=0.5, text/html;q=0.9, */*;q=0.1");
		CHECK(a.quality("text/html") == 0.9);       // exact
		CHECK(a.quality("text/plain") == 0.5);      // type/*
		CHECK(a.quality("application/json") == 0.1); // */*
		http::Accept enc("gzip, deflate;q=0.5");     // token-only (Accept-Encoding shape)
		CHECK(enc.best({"deflate", "gzip"}) == "gzip");
		CHECK(enc.quality("br") == 0.0);
	}

	std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
	return g_failures == 0 ? 0 : 1;
}
