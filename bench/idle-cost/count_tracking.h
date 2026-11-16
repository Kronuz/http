// Asio handler tracking that counts instead of printing (ASIO_CUSTOM_HANDLER_TRACKING): handlers
// created by object type and operation (posts, socket waits, receives, sends), handlers invoked, and
// reactor operations by name and whether they would block. The http_idle_count binary only.
#ifndef HTTP_IDLE_COUNT_TRACKING_H
#define HTTP_IDLE_COUNT_TRACKING_H

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <system_error>

#define ASIO_INHERIT_TRACKED_HANDLER : public ::count_tracking::tracked_handler
#define ASIO_ALSO_INHERIT_TRACKED_HANDLER , public ::count_tracking::tracked_handler
#define ASIO_HANDLER_TRACKING_INIT ::count_tracking::init()
#define ASIO_HANDLER_LOCATION(args) (void)0
#define ASIO_HANDLER_CREATION(args) ::count_tracking::creation args
#define ASIO_HANDLER_COMPLETION(args) ::count_tracking::completion tracked_completion args
#define ASIO_HANDLER_INVOCATION_BEGIN(args) tracked_completion.invocation_begin args
#define ASIO_HANDLER_INVOCATION_END tracked_completion.invocation_end()
#define ASIO_HANDLER_OPERATION(args) (void)0
#define ASIO_HANDLER_REACTOR_REGISTRATION(args) (void)0
#define ASIO_HANDLER_REACTOR_DEREGISTRATION(args) (void)0
#define ASIO_HANDLER_REACTOR_READ_EVENT 1
#define ASIO_HANDLER_REACTOR_WRITE_EVENT 2
#define ASIO_HANDLER_REACTOR_ERROR_EVENT 4
#define ASIO_HANDLER_REACTOR_EVENTS(args) ::count_tracking::reactor_events args
#define ASIO_HANDLER_REACTOR_OPERATION(args) ::count_tracking::reactor_operation args

namespace asio { class execution_context; }

struct count_tracking {
	struct tracked_handler {
		const char* object_type_ = "";
	};
	// A counter per (kind, object type, name); names are string literals, so pointers identify them.
	struct Counter {
		const char* kind = nullptr;
		const char* object = nullptr;
		const char* name = nullptr;
		std::atomic<std::uint64_t> n{0};
	};
	static constexpr std::size_t kCounters = 128;
	static Counter* counters() {
		static Counter table[kCounters];
		return table;
	}
	static void bump(const char* kind, const char* object, const char* name) {
		static std::mutex insert;
		Counter* t = counters();
		for (std::size_t i = 0; i < kCounters; ++i) {
			const char* k = std::atomic_ref(t[i].kind).load(std::memory_order_acquire);
			if (k == kind && t[i].object == object && t[i].name == name) {
				t[i].n.fetch_add(1, std::memory_order_relaxed);
				return;
			}
			if (!k) {
				std::lock_guard lock(insert);
				if (!std::atomic_ref(t[i].kind).load(std::memory_order_acquire)) {
					t[i].object = object;
					t[i].name = name;
					std::atomic_ref(t[i].kind).store(kind, std::memory_order_release);
					t[i].n.fetch_add(1, std::memory_order_relaxed);
					return;
				}
				--i;   // taken meanwhile: look at it again
			}
		}
	}
	static void init() {}
	static void creation(asio::execution_context&, tracked_handler& h, const char* object_type, void*, std::uintmax_t, const char* op_name) {
		h.object_type_ = object_type;
		bump("created", object_type, op_name);
	}
	struct completion {
		explicit completion(const tracked_handler& h) : object(h.object_type_) {}
		completion(const completion&) = delete;
		completion& operator=(const completion&) = delete;
		template<class... Args>
		void invocation_begin(Args&&...) { bump("invoked", object, ""); }
		void invocation_end() {}
		const char* object;
	};
	static void reactor_events(asio::execution_context&, std::uintmax_t, unsigned events) {
		bump("reactor_event", "", (events & ASIO_HANDLER_REACTOR_READ_EVENT) ? "read" : (events & ASIO_HANDLER_REACTOR_WRITE_EVENT) ? "write" : "error");
	}
	static const char* outcome(const std::error_code& ec) {
		if (!ec) { return "done"; }
		if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) { return "would_block"; }
		return "error";
	}
	static void reactor_operation(const tracked_handler& h, const char* op_name, const std::error_code& ec) {
		bump(outcome(ec), h.object_type_, op_name);
	}
	static void reactor_operation(const tracked_handler& h, const char* op_name, const std::error_code& ec, std::size_t) {
		bump(outcome(ec), h.object_type_, op_name);
	}
};

#endif
