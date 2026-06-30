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

// Conditional requests (RFC 7232) -- the transparent half. The library derives an
// ETag from a buffered response body and, when the client's If-None-Match already
// holds it, answers 304 Not Modified with no body. The application does nothing; a
// repeat GET of an unchanged resource costs a hash and a header instead of the body.
//
// The ETag is WEAK (W/"..."): it identifies the resource, not the exact bytes on the
// wire, which is what lets a content-encoded (gzip/zstd) and an identity response
// share a validator, and it is what If-None-Match's weak comparison expects.
// Last-Modified / If-Modified-Since is deliberately not here -- it needs an
// application-supplied modification time the library cannot know.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace http {

// 64-bit FNV-1a over the body. Not cryptographic -- an ETag only has to change when
// the representation changes; collisions just cost a missed 304, never wrong data.
inline std::uint64_t fnv1a64(std::string_view data) {
	std::uint64_t h = 1469598103934665603ull;
	for (unsigned char c : data) {
		h ^= c;
		h *= 1099511628211ull;
	}
	return h;
}

// A weak ETag for a body: W/"<16 hex digits>".
inline std::string weak_etag(std::string_view body) {
	static const char* digits = "0123456789abcdef";
	std::uint64_t h = fnv1a64(body);
	char hex[16];
	for (int i = 15; i >= 0; --i) {
		hex[i] = digits[h & 0xf];
		h >>= 4;
	}
	std::string tag = "W/\"";
	tag.append(hex, 16);
	tag += '"';
	return tag;
}

// The opaque-tag of an ETag: the text between the quotes (drops any W/ prefix), so
// comparison is the weak comparison RFC 7232 §2.3.2 prescribes for If-None-Match.
inline std::string_view etag_opaque(std::string_view etag) {
	auto a = etag.find('"');
	auto b = etag.rfind('"');
	if (a == std::string_view::npos || b <= a) { return {}; }
	return etag.substr(a + 1, b - a - 1);
}

// Does an If-None-Match header select `our_etag`? "*" matches any current
// representation; otherwise any listed tag whose opaque-tag equals ours matches.
inline bool if_none_match(std::string_view header, std::string_view our_etag) {
	while (!header.empty() && (header.front() == ' ' || header.front() == '\t')) { header.remove_prefix(1); }
	while (!header.empty() && (header.back() == ' ' || header.back() == '\t')) { header.remove_suffix(1); }
	if (header == "*") { return true; }
	std::string_view ours = etag_opaque(our_etag);
	if (ours.empty()) { return false; }
	for (std::size_t i = 0;;) {
		auto q1 = header.find('"', i);
		if (q1 == std::string_view::npos) { break; }
		auto q2 = header.find('"', q1 + 1);
		if (q2 == std::string_view::npos) { break; }
		if (header.substr(q1 + 1, q2 - q1 - 1) == ours) { return true; }
		i = q2 + 1;
	}
	return false;
}

}  // namespace http
