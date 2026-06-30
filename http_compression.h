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
#include <string>
#include <string_view>
#include <vector>

#include "compressor_deflate.h"   // Kronuz/compressors: DeflateCompressData (gzip wrapper)
#include "compressor_zstd.h"      // Kronuz/compressors: ZstdCompressData

#include "http_accept.h"

namespace http {

// Tunables for response compression. Held by the service; a connection sees a
// pointer to it (null => compression off).
struct CompressionOptions {
	std::size_t min_size = 256;   // bodies smaller than this aren't worth compressing
	int zstd_level = 3;           // libzstd level (default upstream)
};

// The content-codings the library can produce, in server-preference order: zstd
// first, then gzip. Passed to Accept::best(), whose tie-break favours this order.
inline const std::vector<std::string>& supported_encodings() {
	static const std::vector<std::string> enc = {"zstd", "gzip"};
	return enc;
}

// Compress `body` with the named coding (one of supported_encodings()). gzip is
// DEFLATE with a gzip wrapper; zstd is a standard zstd frame.
inline std::string encode(std::string_view coding, std::string_view body, int zstd_level) {
	std::string out;
	if (coding == "zstd") {
		ZstdCompressData c(body.data(), body.size(), zstd_level);
		for (auto it = c.begin(); it; ++it) { out.append(*it); }
	} else if (coding == "gzip") {
		DeflateCompressData c(body.data(), body.size(), /*gzip=*/true);
		for (auto it = c.begin(); it; ++it) { out.append(*it); }
	}
	return out;
}

// Choose a Content-Encoding for the response, or "" for identity (no compression).
// Only compresses when the client explicitly advertised a coding we support: an
// absent Accept-Encoding is treated as "do not compress" (conservative -- a client
// that didn't ask may not decode), not as RFC 7231's "anything acceptable".
inline std::string_view negotiate_encoding(std::string_view accept_encoding) {
	if (accept_encoding.empty()) { return {}; }
	return Accept(accept_encoding).best(supported_encodings());
}

}  // namespace http
