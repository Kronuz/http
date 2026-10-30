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

// The HTTP message values that cross the application seam. They are deliberately
// plain data: an application sees a request and fills in a response, and nothing
// here knows how the bytes were parsed (http-parser today), how they arrived (the
// Asio transport today), or which loop the handler runs on. That is what keeps the
// parser / transport choices swappable behind the handler.

#pragma once

#include <algorithm>
#include <cctype>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace http {

using Headers = std::vector<std::pair<std::string, std::string>>;

class BodyStream;   // http_body_stream.h -- the streamed request body (Request::body_stream)

// Why a request was cancelled while its handler ran (Request::on_cancel).
//   Disconnect: the client closed its side (EOF) or the socket failed on a read.
//   Shutdown:   the service is stopping.
// Cancellation stops reads at their next checkpoint; the response is still written.
enum class CancelReason : unsigned char { None, Disconnect, Shutdown };

// The base for an application's typed per-request state. The library owns the
// Request; an application attaches its own working state (a decoded body, resolved
// routes, timings, ...) by subclassing this and returning it from
// HttpHandler::create_extension(). This is how an app EXTENDS the request instead of
// keeping a parallel request object -- the library stays application-agnostic and the
// app never duplicates the HTTP fields. Reused connections reset it between requests.
struct RequestExtension {
	virtual ~RequestExtension() = default;
};

inline bool iequal(std::string_view a, std::string_view b) {
	if (a.size() != b.size()) { return false; }
	for (size_t i = 0; i < a.size(); ++i) {
		if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
			return false;
		}
	}
	return true;
}

// The reason phrase for a status code. A small, common subset; anything else
// gets a generic phrase (the code is what matters on the wire).
inline const char* reason_phrase(int status) {
	switch (status) {
		case 200: return "OK";
		case 201: return "Created";
		case 202: return "Accepted";
		case 204: return "No Content";
		case 206: return "Partial Content";
		case 304: return "Not Modified";
		case 400: return "Bad Request";
		case 404: return "Not Found";
		case 405: return "Method Not Allowed";
		case 409: return "Conflict";
		case 412: return "Precondition Failed";
		case 413: return "Payload Too Large";
		case 416: return "Range Not Satisfiable";
		case 500: return "Internal Server Error";
		case 503: return "Service Unavailable";
		default:  return "Status";
	}
}


struct Request {
	std::string method;        // "GET", "POST", "PUT", ...
	std::string target;        // raw request target, e.g. "/items/42?pretty=1"
	std::string path;          // "/items/42"
	std::string query;         // "pretty=1"
	Headers headers;
	std::string body;
	int http_major = 1;
	int http_minor = 1;
	bool keep_alive = true;
	std::string remote;        // the client's IP address as the transport saw it ("" if unknown)

	// Per-request application state, opaque to the library. An app can stash a
	// context here in on_request_body() and read it back in handle() -- e.g. to
	// carry what a streaming BodySink accumulated.
	std::shared_ptr<void> user_data;

	// Set by the framework when the handler opted into a streamed body
	// (wants_body_stream()); the handler reads raw chunks from it with co_await (see
	// BodyStream) while the connection feeds them flow-controlled. Null otherwise.
	std::shared_ptr<BodyStream> body_stream;

	// Cancellation, delivered on the connection's loop while the handler runs: the
	// transport calls cancel() when the client closes its side (Disconnect) or the
	// service stops (Shutdown), and the handler registers what to do with on_cancel() --
	// typically requesting a task::cancel_source. Only the first cancel() counts, and the
	// callback runs once. A callback registered after cancel() runs at once. Neither call
	// is thread-safe: both run on the connection's loop.
	bool cancelled() const noexcept { return cancel_reason_ != CancelReason::None; }
	CancelReason cancel_reason() const noexcept { return cancel_reason_; }
	// A callback that throws is dropped: the transport's frames must end by completing.
	void on_cancel(std::function<void(CancelReason)> callback) {
		cancel_callback_ = std::move(callback);
		if (cancelled()) { run_cancel_callback(); }
	}
	void cancel(CancelReason reason) {
		if (cancelled()) { return; }
		cancel_reason_ = reason;
		run_cancel_callback();
	}

	// Typed per-request application state (see RequestExtension). Created once at
	// headers-complete by HttpHandler::create_extension(), before on_request_body()
	// and wants_body_stream() so both can use it; reset when the connection is reused.
	// ext<T>() is the app's typed view of it (undefined if the wrong T or none set).
	// It stays mutable even through a const Request: the HTTP fields are immutable
	// during handle(), but the app's extension is its per-request working scratch.
	std::unique_ptr<RequestExtension> extension;

	template <typename T>
	T& ext() const { return static_cast<T&>(*extension); }

	std::string_view header(std::string_view name) const {
		for (const auto& [k, v] : headers) {
			if (iequal(k, name)) { return v; }
		}
		return {};
	}

	// The request's Content-Type (the application decodes the body accordingly);
	// empty if absent.
	std::string_view content_type() const { return header("Content-Type"); }

	void clear() {
		method.clear(); target.clear(); path.clear(); query.clear();
		headers.clear(); body.clear();
		http_major = 1; http_minor = 1; keep_alive = true;
		user_data.reset();
		body_stream.reset();
		extension.reset();
		cancel_reason_ = CancelReason::None;
		cancel_callback_ = nullptr;
	}

private:
	void run_cancel_callback() noexcept {
		if (!cancel_callback_) { return; }
		try { cancel_callback_(cancel_reason_); } catch (...) {}
	}
	CancelReason cancel_reason_ = CancelReason::None;
	std::function<void(CancelReason)> cancel_callback_;
};


struct Response {
	int status = 200;
	std::string content_type = "text/plain; charset=utf-8";
	Headers headers;
	std::string body;
	// When set, the connection closes after this response regardless of the
	// request's keep-alive (e.g. on an error). Otherwise keep-alive is honored.
	bool close = false;

	void set(int status_, std::string body_, std::string content_type_ = "text/plain; charset=utf-8") {
		status = status_;
		body = std::move(body_);
		content_type = std::move(content_type_);
	}

	void add_header(std::string name, std::string value) {
		headers.emplace_back(std::move(name), std::move(value));
	}

	// Serialize to the raw HTTP/1.1 response bytes.
	std::string serialize(bool keep_alive) const {
		std::string out;
		out.reserve(128 + body.size());
		out += "HTTP/1.1 ";
		out += std::to_string(status);
		out += ' ';
		out += reason_phrase(status);
		out += "\r\n";
		out += "Content-Type: ";
		out += content_type;
		out += "\r\n";
		out += "Content-Length: ";
		out += std::to_string(body.size());
		out += "\r\n";
		bool will_close = close || !keep_alive;
		out += will_close ? "Connection: close\r\n" : "Connection: keep-alive\r\n";
		for (const auto& [k, v] : headers) {
			out += k; out += ": "; out += v; out += "\r\n";
		}
		out += "\r\n";
		out += body;
		return out;
	}
};

}  // namespace http
