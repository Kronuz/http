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
//
// A ready-made HttpHandler that dispatches by method + path -- the application's
// routing table -- lives in http_router.h (the radix-tree Router).

#pragma once

#include "http_message.h"

namespace http {

class HttpHandler {
public:
	virtual ~HttpHandler() = default;
	virtual void handle(const Request& request, Response& response) = 0;
};

}  // namespace http
