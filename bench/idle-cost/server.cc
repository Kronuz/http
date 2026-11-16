// Idle connection memory: eight one-reactor HTTP services (plain or TLS) hold the connections
// run.py opens, and every line on stdin prints one JSON snapshot of this process's memory.
// HTTP_IDLE_OWNERS builds the profile binary: live heap blocks by size class (operator new) and
// OpenSSL's requested bytes by allocating file, with task frames sent to operator new.
#include "http_asio_tls.h"

#include <openssl/crypto.h>
#include <openssl/opensslv.h>
#include <openssl/x509v3.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <vector>

#include <sys/resource.h>
#ifdef __APPLE__
#include <libproc.h>
#include <mach/mach.h>
#include <malloc/malloc.h>
#include <sys/proc_info.h>
#include <unistd.h>
#else
#include <fstream>
#include <malloc.h>
#include <unistd.h>
#endif

namespace {
std::atomic<std::size_t> ssl_bytes{0}, ssl_blocks{0}, served{0}, failures{0}, release_connections{0}, handshakes{0};
std::atomic<long> ssl_modes{0};
std::atomic<long> bio_write_capacity{-1};

std::size_t usable(void* p) {
#ifdef __APPLE__
	return p ? malloc_size(p) : 0;
#else
	return p ? malloc_usable_size(p) : 0;
#endif
}

#if HTTP_IDLE_OWNERS
// Live operator-new blocks by usable size, in 16-byte classes up to 1 MiB (larger ones share
// the last class, with their bytes).
constexpr std::size_t heap_granularity = 16, heap_classes = (1u << 20) / heap_granularity;
std::atomic<std::int64_t> heap_live[heap_classes + 1];
std::atomic<std::int64_t> heap_large_bytes{0};
void heap_note(void* p, int sign) {
	const auto bytes = usable(p);
	const auto c = (bytes + heap_granularity - 1) / heap_granularity;
	if (c >= heap_classes) {
		heap_live[heap_classes].fetch_add(sign, std::memory_order_relaxed);
		heap_large_bytes.fetch_add(sign * static_cast<std::int64_t>(bytes), std::memory_order_relaxed);
	} else {
		heap_live[c].fetch_add(sign, std::memory_order_relaxed);
	}
}
void* heap_new(std::size_t n, std::size_t align) noexcept {
	void* p = nullptr;
	if (align <= alignof(std::max_align_t)) { p = std::malloc(n ? n : 1); }
	else if (posix_memalign(&p, align, n ? n : 1) != 0) { p = nullptr; }
	if (p) { heap_note(p, 1); }
	return p;
}
void heap_delete(void* p) noexcept {
	if (!p) { return; }
	heap_note(p, -1);
	std::free(p);
}

// OpenSSL's requested bytes by the file that allocated them, behind a header per block.
struct alignas(std::max_align_t) SslAllocation { std::size_t requested, owner; };
struct SslOwner {
	const char* file = nullptr;
	std::atomic<std::size_t> bytes{0}, blocks{0};
};
SslOwner ssl_owners[128];
std::mutex ssl_owner_mutex;
std::size_t ssl_owner_count = 0;
std::size_t ssl_owner(const char* file) {
	if (!file) { file = "unknown"; }
	if (const char* slash = std::strrchr(file, '/')) { file = slash + 1; }
	std::lock_guard lock(ssl_owner_mutex);
	for (std::size_t i = 0; i < ssl_owner_count; ++i) {
		if (!std::strcmp(ssl_owners[i].file, file)) { return i; }
	}
	if (ssl_owner_count == 127) { return 127; }
	ssl_owners[ssl_owner_count].file = file;
	return ssl_owner_count++;
}
void* ssl_malloc(std::size_t bytes, const char* file, int) {
	if (bytes > std::numeric_limits<std::size_t>::max() - sizeof(SslAllocation)) { return nullptr; }
	auto* raw = static_cast<SslAllocation*>(std::malloc(bytes + sizeof(SslAllocation)));
	if (!raw) { return nullptr; }
	const auto owner = ssl_owner(file);
	raw->requested = bytes;
	raw->owner = owner;
	ssl_bytes += bytes;
	++ssl_blocks;
	ssl_owners[owner].bytes += bytes;
	++ssl_owners[owner].blocks;
	return raw + 1;
}
void ssl_free(void* p, const char*, int) {
	if (!p) { return; }
	auto* raw = static_cast<SslAllocation*>(p) - 1;
	ssl_bytes -= raw->requested;
	--ssl_blocks;
	ssl_owners[raw->owner].bytes -= raw->requested;
	--ssl_owners[raw->owner].blocks;
	std::free(raw);
}
void* ssl_realloc(void* p, std::size_t bytes, const char* file, int line) {
	if (!p) { return ssl_malloc(bytes, file, line); }
	if (!bytes) { ssl_free(p, file, line); return nullptr; }
	if (bytes > std::numeric_limits<std::size_t>::max() - sizeof(SslAllocation)) { return nullptr; }
	auto* previous = static_cast<SslAllocation*>(p) - 1;
	const auto old_bytes = previous->requested, owner = previous->owner;
	auto* raw = static_cast<SslAllocation*>(std::realloc(previous, bytes + sizeof(SslAllocation)));
	if (!raw) { return nullptr; }
	raw->requested = bytes;
	ssl_bytes -= old_bytes;
	ssl_bytes += bytes;
	ssl_owners[owner].bytes -= old_bytes;
	ssl_owners[owner].bytes += bytes;
	return raw + 1;
}
#else
// OpenSSL's live usable bytes, without a header.
void* ssl_malloc(std::size_t bytes, const char*, int) {
	auto* p = std::malloc(bytes);
	if (p) { ssl_bytes += usable(p); ++ssl_blocks; }
	return p;
}
void ssl_free(void* p, const char*, int) {
	if (p) { ssl_bytes -= usable(p); --ssl_blocks; }
	std::free(p);
}
void* ssl_realloc(void* p, std::size_t bytes, const char* file, int line) {
	if (!p) { return ssl_malloc(bytes, file, line); }
	if (!bytes) { ssl_free(p, file, line); return nullptr; }
	const auto before = usable(p);
	auto* result = std::realloc(p, bytes);
	if (result) { ssl_bytes -= before; ssl_bytes += usable(result); }
	return result;
}
#endif

std::size_t rss() {
#ifdef __APPLE__
	mach_task_basic_info info{};
	mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
		throw std::runtime_error("task_info failed");
	}
	return info.resident_size;
#else
	std::ifstream input("/proc/self/statm");
	std::size_t total{}, resident{};
	if (!(input >> total >> resident)) { throw std::runtime_error("statm failed"); }
	return resident * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#endif
}

// The charged footprint (macOS phys_footprint; Linux: RSS) and compressed bytes.
std::pair<std::size_t, std::size_t> footprint() {
#ifdef __APPLE__
	task_vm_info_data_t info{};
	mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
	if (task_info(mach_task_self(), TASK_VM_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
		throw std::runtime_error("task_vm_info failed");
	}
	return {info.phys_footprint, info.compressed};
#else
	return {rss(), 0};
#endif
}

void sockets_json() {
#ifdef __APPLE__
	const auto capacity = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, nullptr, 0);
	if (capacity <= 0) { throw std::runtime_error("descriptor list size failed"); }
	std::vector<proc_fdinfo> descriptors(capacity / sizeof(proc_fdinfo) + 32);
	const auto filled = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, descriptors.data(), static_cast<int>(descriptors.size() * sizeof(proc_fdinfo)));
	if (filled <= 0) { throw std::runtime_error("descriptor list failed"); }
	std::size_t sockets = 0, receive = 0, send = 0;
	for (std::size_t i = 0; i < static_cast<std::size_t>(filled) / sizeof(proc_fdinfo); ++i) {
		if (descriptors[i].proc_fdtype != PROX_FDTYPE_SOCKET) { continue; }
		socket_fdinfo info{};
		if (proc_pidfdinfo(getpid(), descriptors[i].proc_fd, PROC_PIDFDSOCKETINFO, &info, sizeof(info)) != sizeof(info)) { continue; }
		if (info.psi.soi_kind != SOCKINFO_TCP) { continue; }
		++sockets;
		receive += info.psi.soi_rcv.sbi_mbcnt;
		send += info.psi.soi_snd.sbi_mbcnt;
	}
	std::cout << ",\"kernel_sockets\":" << sockets << ",\"kernel_receive_mbuf_bytes\":" << receive << ",\"kernel_send_mbuf_bytes\":" << send;
#endif
}

void snapshot(const std::string& stage, const std::vector<std::unique_ptr<http::HttpAsioService>>& services) {
	std::size_t active = 0, rejected = 0;
	for (const auto& service : services) {
		const auto stats = service->connection_stats();
		active += stats.active;
		rejected += stats.rejected;
	}
	std::size_t live{}, reserved{};
#ifdef __APPLE__
	malloc_statistics_t stats{};
	malloc_zone_statistics(nullptr, &stats);
	live = stats.size_in_use;
	reserved = stats.size_allocated;
#else
	const auto stats = mallinfo2();
	live = stats.uordblks + stats.hblkhd;
	reserved = stats.arena + stats.hblkhd;
#endif
	rusage usage{};
	getrusage(RUSAGE_SELF, &usage);
	const auto [physical, compressed] = footprint();
	std::cout << "{\"stage\":\"" << stage << "\",\"served\":" << served.load() << ",\"active\":" << active
			  << ",\"rejected\":" << rejected << ",\"handshakes\":" << handshakes.load()
			  << ",\"bio_write_capacity\":" << bio_write_capacity.load() << ",\"ssl_modes\":" << ssl_modes.load()
			  << ",\"release_connections\":" << release_connections.load() << ",\"tls_failures\":" << failures.load()
			  << ",\"rss\":" << rss() << ",\"footprint\":" << physical << ",\"compressed\":" << compressed
			  << ",\"allocator_live\":" << live << ",\"allocator_reserved\":" << reserved
			  << ",\"openssl_live\":" << ssl_bytes.load() << ",\"openssl_blocks\":" << ssl_blocks.load()
			  << ",\"voluntary_switches\":" << usage.ru_nvcsw << ",\"involuntary_switches\":" << usage.ru_nivcsw
			  << ",\"cpu_us\":" << (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000LL + usage.ru_utime.tv_usec + usage.ru_stime.tv_usec;
#if HTTP_IDLE_OWNERS
	if (stage != "memory-check") {
		std::cout << ",\"task_frames\":" << task::detail::live_frames() << ",\"heap\":[";
		bool comma = false;
		for (std::size_t c = 0; c <= heap_classes; ++c) {
			const auto n = heap_live[c].load(std::memory_order_relaxed);
			if (!n) { continue; }
			std::cout << (comma ? "," : "") << '[' << (c == heap_classes ? -1 : static_cast<long long>(c * heap_granularity)) << ',' << n << ']';
			comma = true;
		}
		std::cout << "],\"heap_large_bytes\":" << heap_large_bytes.load() << ",\"openssl_owners\":[";
		std::lock_guard lock(ssl_owner_mutex);
		comma = false;
		for (std::size_t i = 0; i < 128; ++i) {
			const auto bytes = ssl_owners[i].bytes.load(), blocks = ssl_owners[i].blocks.load();
			if (!bytes && !blocks) { continue; }
			std::cout << (comma ? "," : "") << "{\"file\":\"" << (i == 127 ? "other" : ssl_owners[i].file) << "\",\"bytes\":" << bytes << ",\"blocks\":" << blocks << '}';
			comma = true;
		}
		std::cout << "],\"openssl_profile_headers\":" << ssl_blocks.load() * sizeof(SslAllocation);
		sockets_json();
	}
#endif
	std::cout << "}" << std::endl;
}

// A self-signed P-256 certificate for localhost, installed on `context`.
void self_signed(asio::ssl::context& context) {
	EVP_PKEY* key = EVP_EC_gen("P-256");
	X509* x = X509_new();
	X509_set_version(x, 2);
	ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
	X509_gmtime_adj(X509_getm_notBefore(x), 0);
	X509_gmtime_adj(X509_getm_notAfter(x), 86400);
	X509_set_pubkey(x, key);
	X509_NAME* name = X509_get_subject_name(x);
	X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
	X509_set_issuer_name(x, name);
	X509_sign(x, key, EVP_sha256());
	if (SSL_CTX_use_certificate(context.native_handle(), x) != 1 || SSL_CTX_use_PrivateKey(context.native_handle(), key) != 1) {
		throw std::runtime_error("certificate");
	}
	X509_free(x);
	EVP_PKEY_free(key);
}

struct Handler : http::HttpHandler {
	task::task<void> handle(http::Request& request, http::ResponseWriter& writer) override {
		if (request.path == "/verify") {
			std::uint64_t digest = 14695981039346656037ULL;
			for (unsigned char byte : request.body) { digest ^= byte; digest *= 1099511628211ULL; }
			writer.send(200, std::to_string(request.body.size()) + ":" + std::to_string(digest));
		} else if (request.path.starts_with("/download/")) {
			const auto bytes = std::strtoull(request.path.c_str() + 10, nullptr, 10);
			if (bytes > 16 * 1024 * 1024) {
				writer.send(413, "too large");
			} else {
				std::string body(bytes, '\0');
				for (std::size_t i = 0; i < body.size(); ++i) { body[i] = static_cast<char>(i % 256); }
				writer.send(200, body);
			}
		} else {
			writer.send(200, "ok");
		}
		++served;
		co_return;
	}
};
}  // namespace

#if HTTP_IDLE_OWNERS
void* operator new(std::size_t n) { if (void* p = heap_new(n, 0)) { return p; } throw std::bad_alloc(); }
void* operator new[](std::size_t n) { if (void* p = heap_new(n, 0)) { return p; } throw std::bad_alloc(); }
void* operator new(std::size_t n, const std::nothrow_t&) noexcept { return heap_new(n, 0); }
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept { return heap_new(n, 0); }
void* operator new(std::size_t n, std::align_val_t a) { if (void* p = heap_new(n, static_cast<std::size_t>(a))) { return p; } throw std::bad_alloc(); }
void* operator new[](std::size_t n, std::align_val_t a) { if (void* p = heap_new(n, static_cast<std::size_t>(a))) { return p; } throw std::bad_alloc(); }
void operator delete(void* p) noexcept { heap_delete(p); }
void operator delete[](void* p) noexcept { heap_delete(p); }
void operator delete(void* p, std::size_t) noexcept { heap_delete(p); }
void operator delete[](void* p, std::size_t) noexcept { heap_delete(p); }
void operator delete(void* p, std::align_val_t) noexcept { heap_delete(p); }
void operator delete[](void* p, std::align_val_t) noexcept { heap_delete(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { heap_delete(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { heap_delete(p); }
#endif

int main(int argc, char** argv) {
	if (argc != 2 || (std::string(argv[1]) != "plain" && std::string(argv[1]) != "tls")) {
		std::cerr << "usage: http_idle_cost plain|tls\n";
		return 2;
	}
	if (!CRYPTO_set_mem_functions(ssl_malloc, ssl_realloc, ssl_free)) { return 3; }
	// The headers compiled against must be the library that runs.
	if (OpenSSL_version_major() != OPENSSL_VERSION_MAJOR || OpenSSL_version_minor() != OPENSSL_VERSION_MINOR) {
		std::cerr << "OpenSSL headers " << OPENSSL_VERSION_TEXT << " but library " << OpenSSL_version(OPENSSL_VERSION) << '\n';
		return 4;
	}
	const bool tls = std::string(argv[1]) == "tls";
	std::shared_ptr<asio::ssl::context> context;
	if (tls) {
		context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_server);
		SSL_CTX_set_info_callback(context->native_handle(), [](const SSL* ssl, int where, int) {
			if (where & SSL_CB_HANDSHAKE_DONE) {
				const auto modes = SSL_get_mode(const_cast<SSL*>(ssl));
				ssl_modes.fetch_or(modes);
				bio_write_capacity.store(static_cast<long>(BIO_get_write_buf_size(SSL_get_wbio(ssl), 0)));
				++handshakes;
				if (modes & SSL_MODE_RELEASE_BUFFERS) { ++release_connections; }
			}
		});
		self_signed(*context);
	}
	const char* configured_port = std::getenv("HTTP_IDLE_BASE_PORT");
	const unsigned long base_port = configured_port ? std::strtoul(configured_port, nullptr, 10) : 24000;
	if (base_port < 1024 || base_port > 65527) { return 2; }
	Handler handler;
	std::vector<std::unique_ptr<http::HttpAsioService>> services;
	for (unsigned long port = base_port; port < base_port + 8; ++port) {
		http::HttpAsioService::ConnectionFactory factory;
		if (tls) {
			factory = http::tls_connection_factory(context, std::chrono::seconds(30), 100000, 200000, [](http::TlsFailure) { ++failures; });
		}
		auto service = std::make_unique<http::HttpAsioService>(handler, 1, 0, std::move(factory), 200000);
		service->start(static_cast<unsigned short>(port));
		services.push_back(std::move(service));
	}
	std::cout << "{\"product_commit\":\"" << HTTP_IDLE_PRODUCT_COMMIT << "\",\"bench_commit\":\"" << HTTP_IDLE_BENCH_COMMIT
			  << "\",\"transport\":\"" << argv[1] << "\",\"listener_base\":" << base_port
			  << ",\"openssl\":\"" << OpenSSL_version(OPENSSL_VERSION) << "\",\"owners_profile\":" << HTTP_IDLE_OWNERS
			  << ",\"socket_object\":" << sizeof(asio::ip::tcp::socket) << ",\"parser_object\":" << sizeof(http::RequestParser)
			  << ",\"request_object\":" << sizeof(http::Request) << ",\"timer_object\":" << sizeof(asio::steady_timer) << "}" << std::endl;
	snapshot("baseline", services);
	for (std::string stage; std::getline(std::cin, stage) && stage != "quit";) { snapshot(stage, services); }
	for (auto& service : services) { service->stop(); }
}
