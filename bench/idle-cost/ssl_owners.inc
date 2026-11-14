struct alignas(std::max_align_t) SslAllocation {
    std::size_t requested, owner;
};
struct SslOwner {
    const char* file = nullptr;
    std::atomic<std::size_t> bytes{0}, blocks{0};
};
SslOwner ssl_owners[128];
std::mutex ssl_owner_mutex;
std::size_t ssl_owner_count = 0;
std::size_t ssl_owner(const char* file) {
    if (!file) file = "unknown";
    if (const char* slash = std::strrchr(file, '/')) file = slash + 1;
    std::lock_guard lock(ssl_owner_mutex);
    for (std::size_t i = 0; i < ssl_owner_count; ++i)
        if (!std::strcmp(ssl_owners[i].file, file)) return i;
    if (ssl_owner_count == 127) return 127;
    ssl_owners[ssl_owner_count].file = file;
    return ssl_owner_count++;
}
void* ssl_malloc(std::size_t bytes, const char* file, int) {
    if (bytes > std::numeric_limits<std::size_t>::max() - sizeof(SslAllocation)) return nullptr;
    auto* raw = static_cast<SslAllocation*>(std::malloc(bytes + sizeof(SslAllocation)));
    if (!raw) return nullptr;
    const auto owner = ssl_owner(file);
    raw->requested = bytes; raw->owner = owner;
    ssl_bytes += bytes; ++ssl_blocks;
    ssl_owners[owner].bytes += bytes; ++ssl_owners[owner].blocks;
    return raw + 1;
}
void ssl_free(void* p, const char*, int) {
    if (!p) return;
    auto* raw = static_cast<SslAllocation*>(p) - 1;
    ssl_bytes -= raw->requested; --ssl_blocks;
    ssl_owners[raw->owner].bytes -= raw->requested;
    --ssl_owners[raw->owner].blocks;
    std::free(raw);
}
void* ssl_realloc(void* p, std::size_t bytes, const char* file, int line) {
    if (!p) return ssl_malloc(bytes, file, line);
    if (!bytes) { ssl_free(p, file, line); return nullptr; }
    if (bytes > std::numeric_limits<std::size_t>::max() - sizeof(SslAllocation)) return nullptr;
    auto* previous = static_cast<SslAllocation*>(p) - 1;
    const auto old_bytes = previous->requested, owner = previous->owner;
    auto* raw = static_cast<SslAllocation*>(std::realloc(previous, bytes + sizeof(SslAllocation)));
    if (!raw) return nullptr;
    raw->requested = bytes;
    ssl_bytes -= old_bytes; ssl_bytes += bytes;
    ssl_owners[owner].bytes -= old_bytes; ssl_owners[owner].bytes += bytes;
    return raw + 1;
}
void ssl_owners_json() {
    std::lock_guard lock(ssl_owner_mutex);
    std::cout << ",\"openssl_accounting\":\"requested-profile\",\"openssl_profile_headers\":"
              << ssl_blocks.load() * sizeof(SslAllocation) << ",\"openssl_owners\":[";
    bool comma = false;
    for (std::size_t i = 0; i < 128; ++i) {
        const auto bytes = ssl_owners[i].bytes.load(), blocks = ssl_owners[i].blocks.load();
        if (!bytes && !blocks) continue;
        if (comma) std::cout << ',';
        comma = true;
        std::cout << "{\"file\":\"" << (i == 127 ? "other" : ssl_owners[i].file)
                  << "\",\"bytes\":" << bytes << ",\"blocks\":" << blocks << '}';
    }
    std::cout << ']';
}
