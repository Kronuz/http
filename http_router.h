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

// Router: an HttpHandler that dispatches by method + path -- the application's
// routing table, which is what Xapiand's hardcoded `prepare()` method-switch
// becomes once search is an HttpHandler.
//
// The heavy lifting (the radix tree, parameter capture, zero-copy Params) lives
// in the transport-agnostic Kronuz/radix-router. This is a thin binding over it:
// one radix::Router per HTTP method, plus the HTTP-specific distinction between
// 404 (no method knows this path) and 405 (some method does, just not this one).

#pragma once

#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <radix_router.h>   // radix::Router<T>, radix::Params

#include "http_handler.h"
#include "http_message.h"

namespace http {

// Captured path parameters, re-exported from radix-router so application code
// only needs to name `http::Params`.
using Params = radix::Params;

class Router : public HttpHandler {
public:
	// A route function also receives the captured path parameters; routes that
	// take none can ignore the third argument. Patterns use radix-router syntax:
	// static segments, ":name" params, and a trailing "*name" catch-all.
	using Route = std::function<void(const Request&, Response&, const Params&)>;

	// Register `fn` for `method` + `pattern`. `pattern` must begin with '/'.
	// Throws std::invalid_argument on a malformed pattern or a conflict with an
	// already-registered route (both are programming errors, caught at setup).
	Router& route(std::string_view method, std::string_view pattern, Route fn) {
		if (pattern.empty() || pattern.front() != '/') {
			throw std::invalid_argument("route pattern must begin with '/'");
		}
		tree_for(method).insert(pattern, std::move(fn));
		return *this;
	}

	void handle(const Request& request, Response& response) override {
		Params params;
		for (auto& t : trees_) {
			if (iequal(t.method, request.method)) {
				if (const Route* fn = t.tree.find(request.path, params)) {
					(*fn)(request, response, params);
					return;
				}
				break;  // path not found under the request's method
			}
		}
		// 404 vs 405: a path registered under some other method is "known".
		bool path_known = false;
		for (auto& t : trees_) {
			if (t.tree.find(request.path) != nullptr) { path_known = true; break; }
		}
		response.set(path_known ? 405 : 404, path_known ? "Method Not Allowed\n" : "Not Found\n");
	}

private:
	struct Tree { std::string method; radix::Router<Route> tree; };

	radix::Router<Route>& tree_for(std::string_view method) {
		for (auto& t : trees_) {
			if (iequal(t.method, method)) { return t.tree; }
		}
		trees_.push_back(Tree{std::string(method), {}});
		return trees_.back().tree;
	}

	std::vector<Tree> trees_;
};

}  // namespace http
