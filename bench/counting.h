// The instrumented build's counters (-DHTTP_BENCH_COUNT_ALLOCS=ON), compiled into the benchmark binary only, never
// the library: every global operator new counts (g_allocs, which includes coroutine frames), and every Asio handler
// invocation counts (g_handlers) through Asio's custom handler tracking. Include it in exactly one translation unit,
// before any Asio header. The counters sit on the hot path, so a counted binary is never the timed one.
// Asio's small-block recycling is off (ASIO_DISABLE_SMALL_BLOCK_RECYCLING), so every awaitable frame and handler
// operation reaches operator new instead of Asio's per-thread cache; a count of frames needs that.
#pragma once
#define ASIO_CUSTOM_HANDLER_TRACKING "counting_tracking.h"
#define ASIO_DISABLE_SMALL_BLOCK_RECYCLING 1
#include <cstdlib>
#include <new>

#include "bench_common.h"

namespace counting {
inline void* take(std::size_t n) {
	g_allocs.fetch_add(1, std::memory_order_relaxed);
	return std::malloc(n ? n : 1);
}
inline void* take(std::size_t n, std::align_val_t a) {
	g_allocs.fetch_add(1, std::memory_order_relaxed);
	void* p = nullptr;
	return posix_memalign(&p, static_cast<std::size_t>(a) < sizeof(void*) ? sizeof(void*) : static_cast<std::size_t>(a), n ? n : 1) ? nullptr : p;
}
}  // namespace counting

void* operator new(std::size_t n) {
	if (void* p = counting::take(n)) return p;
	throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
	if (void* p = counting::take(n)) return p;
	throw std::bad_alloc();
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept { return counting::take(n); }
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept { return counting::take(n); }
void* operator new(std::size_t n, std::align_val_t a) {
	if (void* p = counting::take(n, a)) return p;
	throw std::bad_alloc();
}
void* operator new[](std::size_t n, std::align_val_t a) {
	if (void* p = counting::take(n, a)) return p;
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
