# HTTP audit

## Worker offload executor

The offload path these two sections audit was removed in Phase 7c L3 (handlers are coroutines on the connection's loop; see `docs/mechanisms/ coroutine-handlers.md`). They stay as the record of what the audit found.

The buffered transport awaited `asio::post(pool_executor, use_awaitable)` and then called the handler. That await retained the parent coroutine's executor, so blocking handlers still ran on the reactor. The untouched revision reproduces four failures in the existing real TCP transport suite, including serialized slow work and ineffective load shedding.

The corrected path explicitly spawns a handler coroutine on the worker executor and awaits its result on the connection executor. Handler exceptions return to the connection's existing error path. One offload reservation still covers the complete handler call.

On the same native macOS fixture, 40 fast requests under four 500 ms slow requests observe a maximum of 1,931.2 ms before and 2.8 ms afterward. Sixteen slow requests at a four-slot limit change from sixteen accepted requests to four accepted and twelve 503 responses. The existing suite changes from four failures to zero. These are one paired fixture run, not production tail, throughput, cost or power measurements.

Qualification artifacts remain in the Detent workspace's ignored `.scratch/http-offload-before-tests.log` and `.scratch/http-offload-after-tests.log`. The isolated comparison changes only executor selection in archived baseline headers, so the result does not depend on the deferred-response implementation.

## Inline exception admission

The buffered exception path released an offload reservation whenever a pool existed, including inline handlers that never acquired one. This underflows the gate and permits excess worker work. The transport now records successful acquisition and releases exactly that reservation on success or exception.

A real TCP regression uses one worker and one admission slot. Against the isolated previous revision, an inline exception returns 500 but both subsequent held requests are accepted; the excess request is not shed. With the reservation guard, the held request returns 200 and the excess request receives 503 before the held request is released. The regression is retained as `offload_test`; baseline comparison logs are in Detent `.scratch/http-offload-gate-before.log` and `.scratch/http-offload-gate-after.log`.

## Deferred reply footprint

The owned deferred capability adds 32 bytes to the scoped Asio writer on this Intel macOS build: 96 bytes before and 128 bytes afterward. The Response value remains 88 bytes. Deferred exchange, timer and cancellation control are allocated only when `defer()` is called; body buffers and admission leases are separately bounded. The synchronous transport keeps its direct write coroutine path. These object-size measurements establish no latency, throughput, power or cost improvement.
