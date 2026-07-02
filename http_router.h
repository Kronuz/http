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


// A generic method + path dispatch table: one radix tree per HTTP method, plus the
// HTTP 404-vs-405 distinction (a path some *other* method knows is "known"). The
// stored value type T is the application's route handler. Router below uses it with
// a Route callable to be a drop-in HttpHandler; an application whose handlers have a
// different shape -- e.g. a plain function pointer over its own request type, when
// its URL grammar is richer than a plain path and it derives its own routing key --
// uses MethodRouter<ThatType> directly and invokes the looked-up value itself.
template <typename T>
class MethodRouter {
public:
	// Register `value` for `method` + `pattern`. `pattern` uses radix syntax (static
	// segments, ":name" params, a trailing "*name" catch-all) and must begin with '/'.
	// Throws std::invalid_argument on a malformed pattern or a conflicting route (both
	// are programming errors, caught at setup).
	MethodRouter& add(std::string_view method, std::string_view pattern, T value) {
		if (pattern.empty() || pattern.front() != '/') {
			throw std::invalid_argument("route pattern must begin with '/'");
		}
		tree_for(method).insert(pattern, std::move(value));
		return *this;
	}

	// Look up the value registered for (method, path). Returns nullptr when this
	// method has no match for the path; then, if `path_known` is given, it is set true
	// iff some *other* method matches the path -- the 405-vs-404 distinction. Any
	// captured path parameters fill `params`.
	const T* find(std::string_view method, std::string_view path, Params& params, bool* path_known = nullptr) const {
		const T* hit = nullptr;
		for (const auto& t : trees_) {
			if (iequal(t.method, method)) {
				hit = t.tree.find(path, params);
				break;   // path not found under the request's method
			}
		}
		if (hit == nullptr && path_known != nullptr) {
			*path_known = false;
			for (const auto& t : trees_) {
				if (t.tree.find(path) != nullptr) { *path_known = true; break; }
			}
		}
		return hit;
	}

private:
	struct Tree { std::string method; radix::Router<T> tree; };

	radix::Router<T>& tree_for(std::string_view method) {
		for (auto& t : trees_) {
			if (iequal(t.method, method)) { return t.tree; }
		}
		trees_.push_back(Tree{std::string(method), {}});
		return trees_.back().tree;
	}

	std::vector<Tree> trees_;
};


// The application's routing table as an HttpHandler: it dispatches (method, path) to
// a Route and answers 404/405 itself -- what an app's hardcoded method-switch becomes
// once its endpoints are handlers. A thin binding over MethodRouter<Route>.
class Router : public HttpHandler {
public:
	// A route function also receives the captured path parameters; routes that take
	// none can ignore the third argument. Patterns use radix-router syntax: static
	// segments, ":name" params, and a trailing "*name" catch-all.
	using Route = std::function<void(const Request&, ResponseWriter&, const Params&)>;

	// Register `fn` for `method` + `pattern`. `pattern` must begin with '/'.
	Router& route(std::string_view method, std::string_view pattern, Route fn) {
		routes_.add(method, pattern, std::move(fn));
		return *this;
	}

	void handle(const Request& request, ResponseWriter& response) override {
		Params params;
		bool path_known = false;
		if (const Route* fn = routes_.find(request.method, request.path, params, &path_known)) {
			(*fn)(request, response, params);
			return;
		}
		response.send(path_known ? 405 : 404, path_known ? "Method Not Allowed\n" : "Not Found\n");
	}

private:
	MethodRouter<Route> routes_;
};

}  // namespace http
