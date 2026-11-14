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
void socket_owners_json() {
#ifdef __APPLE__
    const auto capacity = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, nullptr, 0);
    if (capacity <= 0) throw std::runtime_error("socket owner list size failed");
    std::vector<proc_fdinfo> descriptors(capacity / sizeof(proc_fdinfo) + 32);
    const auto filled = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, descriptors.data(),
                                    static_cast<int>(descriptors.size() * sizeof(proc_fdinfo)));
    if (filled <= 0) throw std::runtime_error("socket owner list failed");
    std::size_t sockets = 0, receive = 0, send = 0, receive_capacity = 0, send_capacity = 0;
    for (std::size_t i = 0; i < static_cast<std::size_t>(filled) / sizeof(proc_fdinfo); ++i) {
        const auto& descriptor = descriptors[i];
        if (descriptor.proc_fdtype != PROX_FDTYPE_SOCKET) continue;
        socket_fdinfo info{};
        if (proc_pidfdinfo(getpid(), descriptor.proc_fd, PROC_PIDFDSOCKETINFO, &info, sizeof(info))
            != sizeof(info)) throw std::runtime_error("socket owner read failed");
        if (info.psi.soi_kind != SOCKINFO_TCP) continue;
        ++sockets;
        receive += info.psi.soi_rcv.sbi_mbcnt; send += info.psi.soi_snd.sbi_mbcnt;
        receive_capacity += info.psi.soi_rcv.sbi_hiwat; send_capacity += info.psi.soi_snd.sbi_hiwat;
    }
    std::cout << ",\"kernel_sockets\":" << sockets << ",\"kernel_receive_mbuf_bytes\":" << receive
              << ",\"kernel_send_mbuf_bytes\":" << send
              << ",\"kernel_receive_capacity\":" << receive_capacity
              << ",\"kernel_send_capacity\":" << send_capacity;
#endif
}
