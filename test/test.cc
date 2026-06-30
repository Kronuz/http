/*
 * Tests for the HTTP binding of the router: method dispatch, 404 vs 405, param
 * capture surfaced to a route, and that the query string is not part of the
 * match. The radix-tree matching itself is covered by radix-router's own suite;
 * here we only exercise what http::Router adds on top. Run via ctest (http_test).
 */

#include "http_message.h"
#include "http_router.h"

#include <cstdio>
#include <stdexcept>
#include <string>
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

static http::Response run(http::HttpHandler& h, const std::string& method, const std::string& target) {
	http::Request req;
	req.method = method;
	req.target = target;
	auto q = target.find('?');
	req.path = (q == std::string::npos) ? target : target.substr(0, q);
	http::Response resp;
	h.handle(req, resp);
	return resp;
}

static http::Router::Route mark(const char* id, std::vector<std::string> names = {}) {
	std::string sid = id;
	return [sid, names](const http::Request&, http::Response& resp, const http::Params& p) {
		std::string body = "id=" + sid;
		for (const auto& n : names) {
			body += " ";
			body += n;
			body += "=";
			body += std::string(p.get(n));
		}
		resp.set(200, std::move(body));
	};
}

int main() {
	std::printf("== http::Router tests ==\n");

	http::Router r;
	r.route("GET", "/items", mark("list"));
	r.route("GET", "/items/:id", mark("get", {"id"}));
	r.route("POST", "/items", mark("create"));
	r.route("PUT", "/items/:id", mark("update", {"id"}));

	// method dispatch + param capture surfaced to the route
	CHECK(run(r, "GET", "/items").body == "id=list");
	CHECK(run(r, "GET", "/items/42").body == "id=get id=42");
	CHECK(run(r, "POST", "/items").body == "id=create");
	CHECK(run(r, "PUT", "/items/42").body == "id=update id=42");

	// methods are independent: GET and POST on /items are different routes
	CHECK(run(r, "GET", "/items").body == "id=list");
	CHECK(run(r, "POST", "/items").body == "id=create");

	// 404 vs 405
	CHECK(run(r, "GET", "/nope").status == 404);       // unknown path
	CHECK(run(r, "DELETE", "/items").status == 405);   // known path, unsupported method
	CHECK(run(r, "DELETE", "/items/42").status == 405);

	// query string is stripped before matching
	CHECK(run(r, "GET", "/items/42?pretty=1").body == "id=get id=42");

	// case-insensitive method
	CHECK(run(r, "get", "/items").body == "id=list");

	// malformed pattern is rejected at registration
	bool threw = false;
	try {
		http::Router bad;
		bad.route("GET", "items", mark("x"));   // missing leading '/'
	} catch (const std::invalid_argument&) {
		threw = true;
	}
	CHECK(threw);

	std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
	return g_failures == 0 ? 0 : 1;
}
