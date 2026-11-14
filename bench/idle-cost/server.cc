#include "http_asio_tls.h"
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#ifdef __APPLE__
#include <mach/mach.h>
#include <malloc/malloc.h>
#else
#include <malloc.h>
#include <unistd.h>
#include <fstream>
#endif

namespace {
std::atomic<std::size_t> ssl_bytes{0}, ssl_blocks{0}, served{0}, failures{0};
std::size_t usable(void* p) {
#ifdef __APPLE__
    return p ? malloc_size(p) : 0;
#else
    return p ? malloc_usable_size(p) : 0;
#endif
}
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
    std::cout << "{\"stage\":\"" << stage << "\",\"served\":" << served.load()
              << ",\"active\":" << active << ",\"rejected\":" << rejected
              << ",\"tls_failures\":" << failures.load() << ",\"rss\":" << rss()
              << ",\"allocator_live\":" << live << ",\"allocator_reserved\":" << reserved
              << ",\"openssl_live\":" << ssl_bytes.load()
              << ",\"openssl_blocks\":" << ssl_blocks.load() << "}" << std::endl;
}
struct Handler : http::HttpHandler {
    task::task<void> handle(http::Request&, http::ResponseWriter& writer) override {
        writer.send(200, "ok"); ++served; co_return;
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
        context->use_certificate_chain_file(argv[2]);
        context->use_private_key_file(argv[3], asio::ssl::context::pem);
    }
    Handler handler;
    std::vector<std::unique_ptr<http::HttpAsioService>> services;
    for (unsigned port = 18920; port < 18928; ++port) {
        http::HttpAsioService::ConnectionFactory factory;
        if (tls) factory = http::tls_connection_factory(context, std::chrono::seconds(30), 100000, 100000,
            [](http::TlsFailure) { ++failures; });
        auto service = std::make_unique<http::HttpAsioService>(handler, 1, 0, std::move(factory), 100000);
        service->start(static_cast<unsigned short>(port));
        services.push_back(std::move(service));
    }
    std::cout << "{\"socket_object\":" << sizeof(asio::ip::tcp::socket)
              << ",\"parser_object\":" << sizeof(http::RequestParser)
              << ",\"plain_input_object\":" << sizeof(http::detail::ConnectionInput<asio::ip::tcp::socket>)
              << ",\"timer_object\":" << sizeof(asio::steady_timer)
              << ",\"request_object\":" << sizeof(http::Request) << "}" << std::endl;
    snapshot("baseline", services);
    for (std::string stage; std::getline(std::cin, stage) && stage != "quit";) snapshot(stage, services);
    for (auto& service : services) service->stop();
}
