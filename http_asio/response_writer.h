#pragma once

// AsioResponseWriter: the buffered ResponseWriter, its transforms and HTTP/1.1 framing.

#include "http_asio/options.h"

namespace http {

// The buffered ResponseWriter: the handler sets status/headers and writes the body;
// on serialize() the transforms run and the HTTP/1.1 bytes are framed (Content-Length
// + Connection are the transport's, everything else is the app's).
class AsioResponseWriter : public ResponseWriter, public BufferedResponse {
public:
	AsioResponseWriter(const Request& request, const AsioResponseOptions* options)
		: request_(request), options_(options) {}
	AsioResponseWriter(const Request& request, const AsioResponseOptions* options, const asio::any_io_executor* executor, reactor::Reactor* reactor)
		: request_(request), options_(options), executor_(executor), reactor_(reactor) {}
	~AsioResponseWriter() override {
		if (reactor_) {
			if (stream_) { reactor_->untrack(*stream_); }
		}
		if (stream_) { try { stream_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} stream_->disable(); try { stream_->exchange->settle(); } catch (...) {} }
	}
	OwnedStreamCapability stream(StreamOptions options) override {
		affine();
		if (!executor_)
			return {};
		if (started_ || ended_ || !headers_.empty() || close_ || stream_)
			throw std::logic_error("response authority already used");
		options.allow_keep_alive = options.allow_keep_alive && request_.keep_alive;
		auto resource = options.resource;
		auto control = detail::AsioOwnedStreamControl::create(*executor_, std::move(options));
		if (reactor_)
			reactor_->track(*control);
		auto capability = detail::make_resource_unique<OwnedStreamReply>(resource, control->exchange);
		stream_ = std::move(control);
		return capability;
	}
	const std::shared_ptr<detail::AsioOwnedStreamControl> &owned_stream() const noexcept { return stream_; }
	bool is_streamed() const noexcept { return stream_ != nullptr; }
	void abandon_stream() {
		if (stream_)
			stream_->exchange->close(ReplySettlement::Abandoned);
	}

	void status(int code) override { synchronous(); status_ = code; started_ = true; }
	void set_header(std::string_view name, std::string_view value) override { synchronous(); headers_.emplace_back(name, value); }
	void write(std::string_view chunk) override { synchronous(); started_ = true; body_.append(chunk); }
	void end() override { synchronous(); ended_ = true; }
	void set_close() override { synchronous(); close_ = true; }

	bool ended() const { return ended_; }
	bool wants_close() const { return close_ || (stream_ && stream_->exchange->close_after()); }

	// BufferedResponse: read the app-level response back by reference (the transforms
	// in serialize_head() run later, so this is the pre-transform view a logger wants).
	int response_code() const override { return status_; }
	bool response_started() const override { return started_; }
	bool response_ended() const override { return ended_; }
	const Headers& response_headers() const override { return headers_; }
	std::string_view response_body() const override { return body_; }
	std::string_view response_content_type() const override {
		for (const auto& [k, v] : headers_) { if (iequal(k, "Content-Type")) { return v; } }
		return "text/plain; charset=utf-8";
	}

	// Apply the response transforms and frame the HTTP/1.1 response bytes. Transforms
	// run in order: conditional (may flip to a bodyless 304) -> range (may flip to a
	// 206 slice or 416) -> compression (self-skips 204/206/304 and tiny bodies).
	// Builds only the status line + headers; the body is written separately (scatter
	// -gather) so it is never copied into this string, and appended to `out` (a connection
	// reuses one string for every reply's head). See has_body()/body().
	void serialize_head(std::string& out, bool keep_alive) {
		maybe_conditional(); if (status_ != 304) { maybe_range(); } maybe_compress();
		const char* reason = reason_phrase(status_);
		// The status line, the fields, Content-Length (at most 20 digits), Connection, the blank line.
		std::size_t size = 80 + std::strlen(reason);
		for (const auto& [k, v] : headers_) { size += k.size() + 2 + v.size() + 2; }
		out.reserve(out.size() + size);
		out += "HTTP/1.1 ";
		out += std::to_string(status_);
		out += ' ';
		out += reason;
		out += "\r\n";
		for (const auto& [k, v] : headers_) {
			out += k; out += ": "; out += v; out += "\r\n";
		}
		if (status_ != 304 && !has_header("Content-Length")) {   // 304 is defined bodyless
			out += "Content-Length: ";
			out += std::to_string(body_.size());
			out += "\r\n";
		}
		out += (keep_alive && !close_) ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
		out += "\r\n";
	}

	// The finalised body, valid after serialize_head(). Written as its own buffer.
	bool has_body() const { return status_ != 304 && !body_.empty(); }
	const std::string& body() const { return body_; }

private:
	void synchronous() const { affine(); if (is_streamed()) { throw std::logic_error("response authority was streamed"); } }
	// Debug: the writer is used on its connection's loop thread, whatever scheduler wraps the task
	// running there (a handler body on another thread must come back first).
	void affine() const noexcept { assert(!reactor_ || reactor_->on_loop()); }
	const asio::any_io_executor* executor_ = nullptr;
	reactor::Reactor* reactor_ = nullptr;
	std::shared_ptr<detail::AsioOwnedStreamControl> stream_;
	const Request& request_;
	const AsioResponseOptions* options_;
	int status_ = 200;
	std::vector<std::pair<std::string, std::string>> headers_;
	std::string body_;
	bool ended_ = false;
	bool close_ = false;
	bool started_ = false;

	bool has_header(std::string_view name) const {
		for (const auto& [k, v] : headers_) { if (iequal(k, name)) { return true; } }
		return false;
	}

	// Conditional GET: derive a weak ETag from the (uncompressed) body and, if the
	// client's If-None-Match already holds it, drop the body and answer 304. Runs before
	// range/compression so the ETag covers the resource, not the wire bytes. Buffered
	// GET/HEAD 200s only; a handler that set its own ETag is left alone.
	void maybe_conditional() {
		if (options_ == nullptr || !options_->conditional) { return; }
		if (status_ != 200) { return; }
		const std::string& method = request_.method;
		if (method != "GET" && method != "HEAD") { return; }
		if (has_header("ETag")) { return; }
		std::string etag = weak_etag(body_);
		std::string_view inm = request_.header("If-None-Match");
		if (!inm.empty() && if_none_match(inm, etag)) {
			status_ = 304;
			body_.clear();
		}
		headers_.emplace_back("ETag", std::move(etag));
	}

	// Range request: serve a single byte range of the buffered body as 206 Partial
	// Content (Content-Range), or 416 if unsatisfiable; advertise Accept-Ranges: bytes.
	// Buffered GET 200s only; a multi-range or unrecognized header serves the full 200.
	// A 206 is left uncompressed (range + content-coding is a tar pit; ranges are for
	// already-compressed media).
	void maybe_range() {
		if (options_ == nullptr || !options_->ranges) { return; }
		if (status_ != 200 || request_.method != "GET") { return; }
		if (!has_header("Accept-Ranges")) { headers_.emplace_back("Accept-Ranges", "bytes"); }
		std::string_view spec = request_.header("Range");
		if (spec.empty()) { return; }
		const std::size_t total = body_.size();
		ByteRange r = parse_byte_range(spec, total);
		if (!r.recognized) { return; }
		if (!r.satisfiable) {
			status_ = 416;
			body_.clear();
			headers_.emplace_back("Content-Range", "bytes */" + std::to_string(total));
			return;
		}
		headers_.emplace_back("Content-Range", "bytes " + std::to_string(r.start) + "-" + std::to_string(r.end) + "/" + std::to_string(total));
		body_ = body_.substr(r.start, r.end - r.start + 1);
		status_ = 206;
	}

	// Transparent response compression -- identical policy to the libev Writer.
	void maybe_compress() {
		if (options_ == nullptr || !options_->compress) { return; }
		const CompressionOptions& opt = options_->compression;
		if (body_.size() < opt.min_size) { return; }
		if (has_header("Content-Encoding")) { return; }
		if (status_ == 204 || status_ == 304 || status_ == 206 || (status_ >= 100 && status_ < 200)) { return; }
		std::string coding = negotiate_encoding(request_.header("Accept-Encoding"), opt);
		if (coding.empty()) { return; }
		std::string compressed = encode(coding, body_, opt);
		if (compressed.empty() || compressed.size() >= body_.size()) { return; }
		body_ = std::move(compressed);
		headers_.emplace_back("Content-Encoding", std::move(coding));
		if (!has_header("Vary")) { headers_.emplace_back("Vary", "Accept-Encoding"); }
	}
};

}  // namespace http
