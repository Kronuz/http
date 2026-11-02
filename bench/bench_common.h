// What the comparison servers share: the one reply they all send, and the signals run.sh uses to start and stop
// the CPU clock and the server.
#pragma once
#include <atomic>
#include <cstdio>
#include <string>
#include <sys/resource.h>

// The reply every server writes for every request, byte for byte; bench/verify.py checks it. The http library always
// states `Connection: keep-alive`, so the others state it too.
inline const std::string RESP = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\nHello, World!\n";

// Event counters of the instrumented build (-DHTTP_BENCH_COUNT_ALLOCS, bench/counting.h); SIGUSR2 prints them.
inline std::atomic<unsigned long long> g_allocs{0};
inline std::atomic<unsigned long long> g_handlers{0};

inline std::atomic<bool> g_stop{false};
inline void on_signal(int) { g_stop.store(true); }
inline void on_usr2(int) {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	std::fprintf(stderr, "RUSAGE %lld %lld\n", (long long)ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec,
	             (long long)ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec);
#ifdef HTTP_BENCH_COUNT_ALLOCS
	std::fprintf(stderr, "ALLOCS %llu\nHANDLERS %llu\n", g_allocs.load(std::memory_order_relaxed),
	             g_handlers.load(std::memory_order_relaxed));
#endif
}
