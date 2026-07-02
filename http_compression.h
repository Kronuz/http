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

// Transparent response compression -- a generic HTTP knob, not an app concern. The
// library negotiates the response's Content-Encoding from the client's
// Accept-Encoding (reusing http::Accept) and compresses the body with the chosen
// codec (Kronuz/compressors). The application just writes its bytes; if compression
// is enabled on the service and the client advertised a codec we produce, the
// connection swaps the body for the compressed form and sets the headers.
//
// Codings offered: zstd (best ratio at gzip-ish speed; the modern default) and gzip
// (universally supported). HTTP "deflate" is deliberately omitted -- it is
// ambiguously raw-vs-zlib in the wild and our backend emits raw deflate, which some
// clients reject; gzip covers the same backend unambiguously.

#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "compressor_deflate.h"   // Kronuz/compressors: DeflateCompressData (gzip wrapper)
#include "compressor_zstd.h"      // Kronuz/compressors: ZstdCompressData

#include "http_accept.h"

namespace http {

// An application-supplied content-coding encoder: body -> encoded bytes (return ""
// to decline). Lets an app add a coding the library doesn't ship.
using Encoder = std::function<std::string(std::string_view body)>;

// Tunables for response compression. Held by the service; a connection sees a
// pointer to it (null => compression off).
struct CompressionOptions {
	std::size_t min_size = 256;   // bodies smaller than this aren't worth compressing
	int zstd_level = 3;           // libzstd level (default upstream)

	// App-registered content-codings, negotiated AHEAD of the built-in zstd/gzip.
	// This is how an app owns a coding the library doesn't produce (e.g. HTTP
	// "deflate") without reimplementing negotiation -- the transport still picks the
	// coding from Accept-Encoding and applies it; the app only supplies the bytes.
	std::vector<std::pair<std::string, Encoder>> extra_codings;

	CompressionOptions& add_coding(std::string name, Encoder fn) {
		extra_codings.emplace_back(std::move(name), std::move(fn));
		return *this;
	}
};

// The content-codings producible for `opt`, in server-preference order: the app's
// registered codings first (registration order), then the built-ins zstd, gzip.
// Passed to Accept::best(), whose tie-break favours this order.
inline std::vector<std::string> supported_encodings(const CompressionOptions& opt) {
	std::vector<std::string> names;
	names.reserve(opt.extra_codings.size() + 2);
	for (const auto& [name, fn] : opt.extra_codings) { (void)fn; names.push_back(name); }
	names.emplace_back("zstd");
	names.emplace_back("gzip");
	return names;
}

// Compress `body` with the named coding: an app-registered coding if present, else a
// built-in (gzip is DEFLATE with a gzip wrapper; zstd is a standard zstd frame).
// Returns "" for an unknown coding.
inline std::string encode(std::string_view coding, std::string_view body, const CompressionOptions& opt) {
	for (const auto& [name, fn] : opt.extra_codings) {
		if (name == coding) { return fn(body); }
	}
	std::string out;
	if (coding == "zstd") {
		ZstdCompressData c(body.data(), body.size(), opt.zstd_level);
		for (auto it = c.begin(); it; ++it) { out.append(*it); }
	} else if (coding == "gzip") {
		DeflateCompressData c(body.data(), body.size(), /*gzip=*/true);
		for (auto it = c.begin(); it; ++it) { out.append(*it); }
	}
	return out;
}

// Choose a Content-Encoding for the response, or "" for identity (no compression).
// Returns an owning string (the choice is a view into a temporary otherwise). Only
// compresses when the client explicitly advertised a coding we support: an absent
// Accept-Encoding is treated as "do not compress" (conservative -- a client that
// didn't ask may not decode), not as RFC 7231's "anything acceptable".
inline std::string negotiate_encoding(std::string_view accept_encoding, const CompressionOptions& opt) {
	if (accept_encoding.empty()) { return {}; }
	return std::string(Accept(accept_encoding).best(supported_encodings(opt)));
}

}  // namespace http
