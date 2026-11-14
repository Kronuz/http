#include "http_asio_tls.h"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <limits>
#include <mutex>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#ifdef __APPLE__
#include <mach/mach.h>
#ifdef HTTP_IDLE_SSL_OWNERS
#include <libproc.h>
#include <sys/proc_info.h>
#include <unistd.h>
#endif
#include <malloc/malloc.h>
#else
#include <malloc.h>
#include <unistd.h>
#include <fstream>
#endif

namespace {
std::atomic<std::size_t> ssl_bytes{0}, ssl_blocks{0}, served{0}, failures{0}, release_connections{0};
std::atomic<long> ssl_modes{0};
std::size_t usable(void* p) {
#ifdef __APPLE__
    return p ? malloc_size(p) : 0;
#else
    return p ? malloc_usable_size(p) : 0;
#endif
}
#ifndef HTTP_IDLE_SSL_OWNERS
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
    if (!p) return ssl_malloc(bytes, file, line);
    if (!bytes) { ssl_free(p, file, line); return nullptr; }
    const auto before = usable(p);
    auto* result = std::realloc(p, bytes);
    if (result) { ssl_bytes -= before; ssl_bytes += usable(result); }
    return result;
}
#else
#include "ssl_owners.inc"
#endif
std::size_t rss() {
#ifdef __APPLE__
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
        throw std::runtime_error("task_info failed");
    return info.resident_size;
#else
    std::ifstream input("/proc/self/statm");
    std::size_t total{}, resident{};
    if (!(input >> total >> resident)) throw std::runtime_error("statm failed");
    return resident * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#endif
}
std::pair<std::size_t, std::size_t> footprint() {
#ifdef __APPLE__
    task_vm_info_data_t info{};
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO,
                  reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
        throw std::runtime_error("task_vm_info failed");
    return {info.phys_footprint, info.compressed};
#else
    return {0, 0};
#endif
}
void snapshot(const std::string& stage, const std::vector<std::unique_ptr<http::HttpAsioService>>& services) {
    std::size_t active = 0, rejected = 0;
    for (const auto& service : services) {
        const auto stats = service->connection_stats();
        active += stats.active; rejected += stats.rejected;
    }
    std::size_t live{}, reserved{};
#ifdef __APPLE__
    malloc_statistics_t stats{};
    malloc_zone_statistics(nullptr, &stats);
    live = stats.size_in_use; reserved = stats.size_allocated;
#else
    const auto stats = mallinfo2();
    live = stats.uordblks; reserved = stats.arena + stats.hblkhd;
#endif
    const auto [physical, compressed] = footprint();
    std::cout << "{\"stage\":\"" << stage << "\",\"served\":" << served.load()
              << ",\"active\":" << active << ",\"rejected\":" << rejected
              << ",\"ssl_modes\":" << ssl_modes.load() << ",\"release_connections\":" << release_connections.load()
              << ",\"tls_failures\":" << failures.load() << ",\"rss\":" << rss()
              << ",\"physical_footprint\":" << physical << ",\"compressed\":" << compressed
              << ",\"allocator_live\":" << live << ",\"allocator_reserved\":" << reserved
              << ",\"openssl_live\":" << ssl_bytes.load()
              << ",\"openssl_blocks\":" << ssl_blocks.load();
#ifdef HTTP_IDLE_SSL_OWNERS
    if (stage != "memory-check") {
        ssl_owners_json(); socket_owners_json();
        std::cout << ",\"task_frames\":" << task::detail::live_frames();
    }
#endif
    std::cout << "}" << std::endl;
}
struct Handler : http::HttpHandler {
    task::task<void> handle(http::Request& request, http::ResponseWriter& writer) override {
        if (request.path == "/verify") {
            std::uint64_t digest = 14695981039346656037ULL;
            for (unsigned char byte : request.body) { digest ^= byte; digest *= 1099511628211ULL; }
            writer.send(200, std::to_string(request.body.size()) + ":" + std::to_string(digest));
        } else if (request.path.starts_with("/download/")) {
            const auto bytes = std::strtoull(request.path.c_str() + 10, nullptr, 10);
            if (bytes > 16 * 1024 * 1024) { writer.send(413, "too large"); }
            else {
                std::string body(bytes, '\0');
                for (std::size_t i = 0; i < body.size(); ++i) body[i] = static_cast<char>(i % 256);
                writer.send(200, body);
            }
        } else { writer.send(200, "ok"); }
        ++served; co_return;
    }
};
}
int main(int argc, char** argv) {
    if (argc < 2 || (std::string(argv[1]) != "plain" && std::string(argv[1]) != "tls")) {
        std::cerr << "usage: http_idle_cost plain|tls [certificate key]\n"; return 2;
    }
    if (!CRYPTO_set_mem_functions(ssl_malloc, ssl_realloc, ssl_free)) return 3;
    const bool tls = std::string(argv[1]) == "tls";
    std::shared_ptr<asio::ssl::context> context;
    if (tls) {
        if (argc != 4) return 2;
        context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_server);
        SSL_CTX_set_info_callback(context->native_handle(), [](const SSL* value, int where, int) {
            if (where & SSL_CB_HANDSHAKE_DONE) {
                const auto modes = SSL_get_mode(const_cast<SSL*>(value));
                ssl_modes.fetch_or(modes);
                if (modes & SSL_MODE_RELEASE_BUFFERS) ++release_connections;
            }
        });
        context->use_certificate_chain_file(argv[2]);
        context->use_private_key_file(argv[3], asio::ssl::context::pem);
    }
    const char* configured_port = std::getenv("HTTP_IDLE_BASE_PORT");
    const unsigned long base_port = configured_port ? std::strtoul(configured_port, nullptr, 10) : 24000;
    if (base_port < 1024 || base_port > 65527) return 2;
    Handler handler;
    std::vector<std::unique_ptr<http::HttpAsioService>> services;
    for (unsigned long port = base_port; port < base_port + 8; ++port) {
        http::HttpAsioService::ConnectionFactory factory;
        if (tls) factory = http::tls_connection_factory(context, std::chrono::seconds(30), 100000, 100000,
            [](http::TlsFailure) { ++failures; });
        auto service = std::make_unique<http::HttpAsioService>(handler, 1, 0, std::move(factory), 100000);
        service->start(static_cast<unsigned short>(port));
        services.push_back(std::move(service));
    }
    std::cout << "{\"listener_base\":" << base_port << ",\"socket_object\":" << sizeof(asio::ip::tcp::socket)
              << ",\"parser_object\":" << sizeof(http::RequestParser)
              << ",\"plain_input_object\":" << sizeof(http::detail::ConnectionInput<asio::ip::tcp::socket>)
              << ",\"timer_object\":" << sizeof(asio::steady_timer)
              << ",\"request_object\":" << sizeof(http::Request) << "}" << std::endl;
    snapshot("baseline", services);
    for (std::string stage; std::getline(std::cin, stage) && stage != "quit";) snapshot(stage, services);
    for (auto& service : services) service->stop();
}
