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
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "http_message.h"   // for http::iequal

namespace http {

// One media range from an Accept header: "type/subtype;q=...;param=..."; either part
// may be the wildcard "*". Encodings/charsets/languages parse as a bare token in `type`
// with `subtype` left empty. Any media-range parameters other than q are retained
// (verbatim, lower-cased key) so the application can read a rendering hint it defined --
// e.g. a custom ";indent=4" -- off the negotiated item; the library never interprets
// them.
struct AcceptItem {
	std::string type;        // "application", "text", "*", or a bare token ("gzip")
	std::string subtype;     // "json", "*", or "" for token-only headers
	double q = 1.0;          // quality, 0..1
	int order = 0;           // header position, for stable tie-breaking
	std::vector<std::pair<std::string, std::string>> params;   // non-q params, e.g. indent=4

	// The value of a media-range parameter (case-insensitive name), or {} if absent.
	std::string_view param(std::string_view name) const {
		for (const auto& [k, v] : params) { if (iequal(k, name)) { return v; } }
		return {};
	}
};


// The result of negotiation: the chosen (application-produced) media type and the client
// Accept item it matched -- so the caller can read that item's parameters (a rendering
// hint like indent). `item` is null when nothing matched.
struct AcceptMatch {
	std::string_view media;
	const AcceptItem* item = nullptr;
};


// A parsed Accept-family header. Use best()/negotiate_match() to choose what to send.
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
				parse_params(part.substr(semi + 1), item);
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

	// The parsed items, in header order. Lets an application build its own ranked
	// representation (mapping each media range to its own type model) instead of using
	// best()/negotiate_match() -- e.g. to negotiate against per-resource stored variants.
	const std::vector<AcceptItem>& items() const { return items_; }

	// The quality (0..1) the client assigns to `media` ("application/json" or a
	// bare token like "gzip"). 0 means "not acceptable". The most specific matching
	// header entry wins (exact > type/* > */*).
	double quality(std::string_view media) const {
		const AcceptItem* it = match(media);
		return it != nullptr ? it->q : 0.0;
	}

	// The client Accept item that most-specifically matches `media`, or null if the
	// client would not accept it. Its parameters (e.g. indent) belong to the client.
	// Stateless (no scratch members), so a parsed Accept is safe to share across threads.
	const AcceptItem* match(std::string_view media) const {
		std::string type, subtype;
		auto slash = media.find('/');
		if (slash == std::string_view::npos) {
			type = lower(media);
		} else {
			type = lower(media.substr(0, slash));
			subtype = lower(media.substr(slash + 1));
		}

		const AcceptItem* best_item = nullptr;
		int best_spec = -1;
		double q = -1.0;
		for (const auto& it : items_) {
			bool type_ok = (it.type == "*") || it.type == type;
			bool sub_ok = (it.subtype == "*") || it.subtype == subtype || (it.subtype.empty() && subtype.empty());
			if (!type_ok || !sub_ok) { continue; }
			int spec = (it.type != "*" ? 2 : 0) + (!it.subtype.empty() && it.subtype != "*" ? 1 : 0);
			if (spec > best_spec || (spec == best_spec && it.q > q)) {
				best_spec = spec;
				q = it.q;
				best_item = &it;
			}
		}
		return (best_item != nullptr && best_item->q > 0.0) ? best_item : nullptr;
	}

	// Choose the best representation to send. `available` is the application's
	// producible media types, in the application's own preference order (used to
	// break ties). Returns the chosen entry, or {} if the client accepts none.
	// An absent/empty Accept means "anything", so the first available wins.
	std::string_view best(const std::vector<std::string>& available) const {
		return negotiate_match(available).media;
	}

	// Like best(), but also returns the client Accept item the choice matched, so the
	// caller can read that item's parameters (e.g. an indent hint).
	AcceptMatch negotiate_match(const std::vector<std::string>& available) const {
		if (available.empty()) { return {}; }
		if (items_.empty()) { return AcceptMatch{available.front(), nullptr}; }

		AcceptMatch out;
		double best_q = 0.0;
		for (const auto& avail : available) {
			const AcceptItem* it = match(avail);
			if (it != nullptr && it->q > best_q) {   // strict: earlier (server-preferred) wins ties
				best_q = it->q;
				out.media = avail;
				out.item = it;
			}
		}
		return best_q > 0.0 ? out : AcceptMatch{};
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

	// Parse the parameter list after the media range: q= sets the quality (default 1.0),
	// every other "key=value" is retained on the item (lower-cased key, verbatim value).
	static void parse_params(std::string_view params, AcceptItem& item) {
		std::size_t i = 0;
		while (i < params.size()) {
			std::size_t semi = params.find(';', i);
			std::string_view p = trim(params.substr(i, semi == std::string_view::npos ? std::string_view::npos : semi - i));
			i = (semi == std::string_view::npos) ? params.size() : semi + 1;
			std::size_t eq = p.find('=');
			if (eq == std::string_view::npos) { continue; }
			std::string_view key = trim(p.substr(0, eq));
			std::string_view val = trim(p.substr(eq + 1));
			if (key.size() == 1 && (key[0] == 'q' || key[0] == 'Q')) {
				double q = 1.0;
				try { q = std::stod(std::string(val)); } catch (...) { q = 1.0; }
				item.q = q < 0.0 ? 0.0 : (q > 1.0 ? 1.0 : q);
			} else if (!key.empty()) {
				item.params.emplace_back(lower(key), std::string(val));
			}
		}
	}
};


// Convenience: negotiate the response media type for a request against the media
// types an application can produce (its preference order). Returns {} if the
// client accepts none of them (the caller should answer 406 Not Acceptable).
inline std::string_view negotiate(const Request& request, const std::vector<std::string>& available) {
	return Accept(request.header("Accept")).best(available);
}


// A thread-safe, bounded LRU cache of parsed Accept-family headers. Parsing on every
// request is wasteful when a handful of distinct header strings recur across a client
// population; this parses each once and hands back a shared, immutable Accept (which is
// stateless, hence safe to read concurrently). Generic: the same cache serves Accept,
// Accept-Encoding, Accept-Charset, and Accept-Language. This is the caching layer an
// application used to hand-roll (an "AcceptLRU"); it belongs with the negotiation.
class AcceptCache {
public:
	explicit AcceptCache(std::size_t capacity = 100) : capacity_(capacity != 0 ? capacity : 1) {}

	// The parsed Accept for `header`, from the cache or freshly parsed (and cached).
	std::shared_ptr<const Accept> get(const std::string& header) {
		std::lock_guard<std::mutex> lk(mutex_);
		auto it = map_.find(header);
		if (it != map_.end()) {
			order_.splice(order_.begin(), order_, it->second.second);   // touch: move to MRU
			return it->second.first;
		}
		auto parsed = std::make_shared<const Accept>(header);
		order_.push_front(header);
		map_.emplace(header, std::make_pair(parsed, order_.begin()));
		if (map_.size() > capacity_) {
			map_.erase(order_.back());
			order_.pop_back();
		}
		return parsed;
	}

private:
	std::size_t capacity_;
	std::mutex mutex_;
	std::list<std::string> order_;   // MRU at front ... LRU at back
	std::unordered_map<std::string, std::pair<std::shared_ptr<const Accept>, std::list<std::string>::iterator>> map_;
};

}  // namespace http
