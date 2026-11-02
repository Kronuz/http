// Asio's custom handler tracking (ASIO_CUSTOM_HANDLER_TRACKING): every macro is empty except the one that runs when
// a handler is invoked, which counts it. Included by Asio through bench/counting.h.
#pragma once
#include "bench_common.h"

#define ASIO_INHERIT_TRACKED_HANDLER
#define ASIO_ALSO_INHERIT_TRACKED_HANDLER
#define ASIO_HANDLER_TRACKING_INIT (void)0
#define ASIO_HANDLER_LOCATION(loc) (void)0
#define ASIO_HANDLER_CREATION(args) (void)0
#define ASIO_HANDLER_COMPLETION(args) (void)0
#define ASIO_HANDLER_INVOCATION_BEGIN(args) ::g_handlers.fetch_add(1, std::memory_order_relaxed)
#define ASIO_HANDLER_INVOCATION_END (void)0
#define ASIO_HANDLER_OPERATION(args) (void)0
#define ASIO_HANDLER_REACTOR_REGISTRATION(args) (void)0
#define ASIO_HANDLER_REACTOR_DEREGISTRATION(args) (void)0
#define ASIO_HANDLER_REACTOR_READ_EVENT 0
#define ASIO_HANDLER_REACTOR_WRITE_EVENT 0
#define ASIO_HANDLER_REACTOR_ERROR_EVENT 0
#define ASIO_HANDLER_REACTOR_EVENTS(args) (void)0
#define ASIO_HANDLER_REACTOR_OPERATION(args) (void)0
