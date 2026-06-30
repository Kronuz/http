/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The application seam. An HttpHandler turns a request into a response and knows
// nothing about parsing, sockets, or the event loop. The search engine is one
// HttpHandler; the demo is another.
//
// Forward-compatibility note: handle() is synchronous today. Because it is
// value-semantic (request in, response out) rather than a callback into the
// connection, the coroutine upgrade is local and additive -- a `co_await`-able
// variant returning a task<> can be introduced and the single call site in
// HttpConnection switched to `co_await`, without touching any handler's logic.

#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "http_message.h"

namespace http {

class HttpHandler {
public:
	virtual ~HttpHandler() = default;
	virtual void handle(const Request& request, Response& response) = 0;
};


// A convenience HttpHandler that dispatches by method + path prefix to registered
// functions -- the application's routing table. This is what Xapiand's hardcoded
// `prepare()` method-switch becomes: routing as app configuration, not something
// baked into the connection. Matching is longest-prefix; a known path with the
// wrong method yields 405, an unknown path yields 404.
class Router : public HttpHandler {
public:
	using Route = std::function<void(const Request&, Response&)>;

	// `prefix` matches a request whose path starts with it. Register the more
	// specific prefixes; the longest match wins.
	Router& route(std::string method, std::string prefix, Route fn) {
		routes_.push_back({std::move(method), std::move(prefix), std::move(fn)});
		return *this;
	}

	void handle(const Request& request, Response& response) override {
		const Entry* best = nullptr;
		bool path_known = false;
		for (const auto& e : routes_) {
			if (request.path.size() >= e.prefix.size() &&
			    request.path.compare(0, e.prefix.size(), e.prefix) == 0) {
				path_known = true;
				if (iequal(e.method, request.method) &&
				    (best == nullptr || e.prefix.size() > best->prefix.size())) {
					best = &e;
				}
			}
		}
		if (best != nullptr) {
			best->fn(request, response);
			return;
		}
		if (path_known) {
			response.set(405, "Method Not Allowed\n");
		} else {
			response.set(404, "Not Found\n");
		}
	}

private:
	struct Entry { std::string method; std::string prefix; Route fn; };
	std::vector<Entry> routes_;
};

}  // namespace http
