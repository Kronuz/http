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

// Content negotiation -- the HTTP part of "let the client pick a representation"
// (RFC 7231 §5.3). This is generic: the library parses the client's Accept (or
// Accept-Encoding / Accept-Charset / Accept-Language) header into a quality-ranked
// set and matches it against the *available* representations the application says
// it can produce. The library never learns the application's object model or how
// any media type is serialized -- it only answers "given what the client accepts
// and what you can produce, which should you send?". The application owns the
// serializers (that's its registry); this owns the negotiation.

#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

#include "http_message.h"   // for http::iequal

namespace http {

// One media range from an Accept header: "type/subtype;q=..."; either part may be
// the wildcard "*". Encodings/charsets/languages parse as a bare token in `type`
// with `subtype` left empty.
struct AcceptItem {
	std::string type;        // "application", "text", "*", or a bare token ("gzip")
	std::string subtype;     // "json", "*", or "" for token-only headers
	double q = 1.0;          // quality, 0..1
	int order = 0;           // header position, for stable tie-breaking
};


// A parsed Accept-family header. Use best() to choose what to send.
class Accept {
public:
	Accept() = default;

	explicit Accept(std::string_view header) {
		int order = 0;
		std::size_t i = 0;
		while (i < header.size()) {
			std::size_t comma = header.find(',', i);
			std::string_view part = header.substr(i, comma == std::string_view::npos ? std::string_view::npos : comma - i);
			i = (comma == std::string_view::npos) ? header.size() : comma + 1;

			part = trim(part);
			if (part.empty()) { continue; }

			AcceptItem item;
			item.order = order++;

			// split media range from parameters (";")
			std::string_view range = part;
			std::size_t semi = part.find(';');
			if (semi != std::string_view::npos) {
				range = trim(part.substr(0, semi));
				item.q = parse_q(part.substr(semi + 1));
			}

			std::size_t slash = range.find('/');
			if (slash == std::string_view::npos) {
				item.type = lower(range);        // token-only (encoding/charset/lang)
			} else {
				item.type = lower(trim(range.substr(0, slash)));
				item.subtype = lower(trim(range.substr(slash + 1)));
			}
			if (!item.type.empty()) { items_.push_back(std::move(item)); }
		}
	}

	bool empty() const { return items_.empty(); }

	// The quality (0..1) the client assigns to `media` ("application/json" or a
	// bare token like "gzip"). 0 means "not acceptable". The most specific matching
	// header entry wins (exact > type/* > */*).
	double quality(std::string_view media) const {
		std::string type, subtype;
		auto slash = media.find('/');
		if (slash == std::string_view::npos) {
			type = lower(media);
		} else {
			type = lower(media.substr(0, slash));
			subtype = lower(media.substr(slash + 1));
		}

		double q = -1.0;
		int best_spec = -1;
		for (const auto& it : items_) {
			bool type_ok = (it.type == "*") || it.type == type;
			bool sub_ok = (it.subtype == "*") || it.subtype == subtype || (it.subtype.empty() && subtype.empty());
			if (!type_ok || !sub_ok) { continue; }
			int spec = (it.type != "*" ? 2 : 0) + (!it.subtype.empty() && it.subtype != "*" ? 1 : 0);
			if (spec > best_spec || (spec == best_spec && it.q > q)) {
				best_spec = spec;
				q = it.q;
			}
		}
		return q < 0.0 ? 0.0 : q;
	}

	// Choose the best representation to send. `available` is the application's
	// producible media types, in the application's own preference order (used to
	// break ties). Returns the chosen entry, or {} if the client accepts none.
	// An absent/empty Accept means "anything", so the first available wins.
	std::string_view best(const std::vector<std::string>& available) const {
		if (available.empty()) { return {}; }
		if (items_.empty()) { return available.front(); }

		std::string_view chosen;
		double best_q = 0.0;
		for (const auto& avail : available) {
			double q = quality(avail);
			if (q > best_q) {        // strict: earlier (server-preferred) wins ties
				best_q = q;
				chosen = avail;
			}
		}
		return best_q > 0.0 ? chosen : std::string_view{};
	}

private:
	std::vector<AcceptItem> items_;

	static std::string_view trim(std::string_view s) {
		while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) { s.remove_prefix(1); }
		while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) { s.remove_suffix(1); }
		return s;
	}

	static std::string lower(std::string_view s) {
		std::string out(s);
		std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		return out;
	}

	// Parse the parameter list after the media range, extracting q= (default 1.0).
	static double parse_q(std::string_view params) {
		std::size_t i = 0;
		while (i < params.size()) {
			std::size_t semi = params.find(';', i);
			std::string_view p = trim(params.substr(i, semi == std::string_view::npos ? std::string_view::npos : semi - i));
			i = (semi == std::string_view::npos) ? params.size() : semi + 1;
			std::size_t eq = p.find('=');
			if (eq == std::string_view::npos) { continue; }
			std::string_view key = trim(p.substr(0, eq));
			if (key.size() == 1 && (key[0] == 'q' || key[0] == 'Q')) {
				std::string_view val = trim(p.substr(eq + 1));
				double q = 1.0;
				try {
					q = std::stod(std::string(val));
				} catch (...) {
					q = 1.0;
				}
				return q < 0.0 ? 0.0 : (q > 1.0 ? 1.0 : q);
			}
		}
		return 1.0;
	}
};


// Convenience: negotiate the response media type for a request against the media
// types an application can produce (its preference order). Returns {} if the
// client accepts none of them (the caller should answer 406 Not Acceptable).
inline std::string_view negotiate(const Request& request, const std::vector<std::string>& available) {
	return Accept(request.header("Accept")).best(available);
}

}  // namespace http
