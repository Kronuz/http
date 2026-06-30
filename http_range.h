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

// Range requests (RFC 7233) -- the transparent half. For a buffered response the
// library can serve a single byte range as 206 Partial Content (Content-Range), the
// thing media players and resumable downloads use to seek. Only a single range is
// handled here; a multi-range request (multipart/byteranges) or any unit other than
// bytes is left unrecognized, and the caller serves the full 200 -- always correct,
// just not partial.

#pragma once

#include <charconv>
#include <cstddef>
#include <string_view>

namespace http {

struct ByteRange {
	bool recognized = false;    // a single, well-formed "bytes=" range we handle
	bool satisfiable = false;   // ...and it lies within the representation
	std::size_t start = 0;      // first byte (inclusive)
	std::size_t end = 0;        // last byte (inclusive)
};

// Parse a Range header for a representation of `total` bytes. Forms handled:
//   bytes=first-last   an explicit span (last clamped to the end)
//   bytes=first-       from `first` to the end
//   bytes=-suffix      the final `suffix` bytes
// Unrecognized (multi-range, other units, malformed) -> recognized=false (serve the
// full 200). Recognized but out of range -> satisfiable=false (answer 416).
inline ByteRange parse_byte_range(std::string_view spec, std::size_t total) {
	ByteRange r;
	constexpr std::string_view prefix = "bytes=";
	if (spec.size() < prefix.size() || spec.substr(0, prefix.size()) != prefix) { return r; }
	spec.remove_prefix(prefix.size());
	if (spec.find(',') != std::string_view::npos) { return r; }   // multi-range: not handled here
	auto dash = spec.find('-');
	if (dash == std::string_view::npos) { return r; }
	std::string_view first = spec.substr(0, dash);
	std::string_view last = spec.substr(dash + 1);

	auto to_num = [](std::string_view s, std::size_t& out) {
		if (s.empty()) { return false; }
		auto res = std::from_chars(s.data(), s.data() + s.size(), out);
		return res.ec == std::errc() && res.ptr == s.data() + s.size();
	};

	if (first.empty()) {                          // suffix range: -N (the last N bytes)
		std::size_t n = 0;
		if (!to_num(last, n)) { return r; }       // malformed -> ignore the header
		r.recognized = true;
		if (n == 0 || total == 0) { return r; }   // -0 (or empty representation) -> 416
		if (n > total) { n = total; }
		r.start = total - n;
		r.end = total - 1;
		r.satisfiable = true;
		return r;
	}

	std::size_t s = 0;
	if (!to_num(first, s)) { return r; }          // malformed -> ignore the header
	std::size_t e = 0;
	if (last.empty()) {
		e = (total == 0) ? 0 : total - 1;         // first- (to the end)
	} else if (!to_num(last, e)) {
		return r;                                 // malformed -> ignore the header
	}
	r.recognized = true;
	if (total == 0 || s >= total || e < s) { return r; }   // out of range -> 416
	if (e >= total) { e = total - 1; }
	r.start = s;
	r.end = e;
	r.satisfiable = true;
	return r;
}

}  // namespace http
