// Counts the server's socket and reactor syscalls, and how many would block, when inserted with
// DYLD_INSERT_LIBRARIES (macOS interposing); idle_syscount() hands the counters to server.cc.
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

enum { kRecv, kRecvfrom, kRecvmsg, kRead, kReadv, kSend, kSendto, kSendmsg, kWrite, kWritev, kKevent, kCalls };
static const char* const names[kCalls] = {"recv", "recvfrom", "recvmsg", "read", "readv", "send", "sendto", "sendmsg", "write", "writev", "kevent"};
static _Atomic uint64_t calls[kCalls], blocked[kCalls];

static void note(int which, long result) {
	atomic_fetch_add_explicit(&calls[which], 1, memory_order_relaxed);
	if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { atomic_fetch_add_explicit(&blocked[which], 1, memory_order_relaxed); }
}

// The counters: names, calls and would-block results, kCalls of each.
int idle_syscount(const char* const** out_names, uint64_t* out_calls, uint64_t* out_blocked) {
	*out_names = names;
	for (int i = 0; i < kCalls; ++i) {
		out_calls[i] = atomic_load_explicit(&calls[i], memory_order_relaxed);
		out_blocked[i] = atomic_load_explicit(&blocked[i], memory_order_relaxed);
	}
	return kCalls;
}

static ssize_t my_recv(int s, void* b, size_t n, int f) { ssize_t r = recv(s, b, n, f); note(kRecv, r); return r; }
static ssize_t my_recvfrom(int s, void* b, size_t n, int f, struct sockaddr* a, socklen_t* l) { ssize_t r = recvfrom(s, b, n, f, a, l); note(kRecvfrom, r); return r; }
static ssize_t my_recvmsg(int s, struct msghdr* m, int f) { ssize_t r = recvmsg(s, m, f); note(kRecvmsg, r); return r; }
static ssize_t my_read(int d, void* b, size_t n) { ssize_t r = read(d, b, n); note(kRead, r); return r; }
static ssize_t my_readv(int d, const struct iovec* v, int c) { ssize_t r = readv(d, v, c); note(kReadv, r); return r; }
static ssize_t my_send(int s, const void* b, size_t n, int f) { ssize_t r = send(s, b, n, f); note(kSend, r); return r; }
static ssize_t my_sendto(int s, const void* b, size_t n, int f, const struct sockaddr* a, socklen_t l) { ssize_t r = sendto(s, b, n, f, a, l); note(kSendto, r); return r; }
static ssize_t my_sendmsg(int s, const struct msghdr* m, int f) { ssize_t r = sendmsg(s, m, f); note(kSendmsg, r); return r; }
static ssize_t my_write(int d, const void* b, size_t n) { ssize_t r = write(d, b, n); note(kWrite, r); return r; }
static ssize_t my_writev(int d, const struct iovec* v, int c) { ssize_t r = writev(d, v, c); note(kWritev, r); return r; }
static int my_kevent(int kq, const struct kevent* c, int nc, struct kevent* e, int ne, const struct timespec* t) {
	int r = kevent(kq, c, nc, e, ne, t); note(kKevent, r); return r;
}

#define INTERPOSE(mine, theirs) \
	__attribute__((used)) static const struct { const void* replacement; const void* replacee; } interpose_##theirs \
		__attribute__((section("__DATA,__interpose"))) = {(const void*)(mine), (const void*)(theirs)}
INTERPOSE(my_recv, recv);
INTERPOSE(my_recvfrom, recvfrom);
INTERPOSE(my_recvmsg, recvmsg);
INTERPOSE(my_read, read);
INTERPOSE(my_readv, readv);
INTERPOSE(my_send, send);
INTERPOSE(my_sendto, sendto);
INTERPOSE(my_sendmsg, sendmsg);
INTERPOSE(my_write, write);
INTERPOSE(my_writev, writev);
INTERPOSE(my_kevent, kevent);
