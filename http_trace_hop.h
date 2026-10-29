// http_trace_hop.h -- wall-clock stamps around the offload hop, for one baseline build.
//
// Compiled in only with -DHTTP_TRACE_HOP. Each offloaded request records four steady-clock
// nanosecond stamps: `post` (just before co_spawn onto the pool), `start` (the handler is about to
// run on the pool thread), `end` (it returned) and `resume` (the connection coroutine is running
// again on its loop). The hop's wall time is (start - post) + (resume - end). The stamps are written
// as CSV lines to $HTTP_TRACE_HOP_OUT when the process exits (SIGTERM through exit()).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

namespace http::trace_hop {

struct Stamp {
	std::uint64_t post = 0, start = 0, end = 0, resume = 0;
};

inline std::uint64_t now() noexcept {
	return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

class Log {
public:
	Log() : stamps_(std::size_t{1} << 21) {}
	~Log() { dump(); }
	void record(const Stamp& s) noexcept {
		const auto i = next_.fetch_add(1, std::memory_order_relaxed);
		if (i < stamps_.size()) { stamps_[i] = s; }
	}
	void dump() {
		const char* path = std::getenv("HTTP_TRACE_HOP_OUT");
		if (!path || !*path) { return; }
		std::FILE* f = std::fopen(path, "w");
		if (!f) { return; }
		const auto n = std::min<std::size_t>(next_.load(), stamps_.size());
		for (std::size_t i = 0; i < n; ++i) {
			const auto& s = stamps_[i];
			std::fprintf(f, "%llu,%llu,%llu,%llu\n", (unsigned long long)s.post, (unsigned long long)s.start, (unsigned long long)s.end, (unsigned long long)s.resume);
		}
		std::fclose(f);
	}
private:
	std::vector<Stamp> stamps_;
	std::atomic<std::size_t> next_{0};
};

inline Log& log() { static Log instance; return instance; }

}  // namespace http::trace_hop
