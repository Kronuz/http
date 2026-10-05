# HTTP audit

## Worker offload executor

The buffered transport awaited `asio::post(pool_executor, use_awaitable)` and then called the handler. That await retained the parent coroutine's executor, so blocking handlers still ran on the reactor. The untouched revision reproduces four failures in the existing real TCP transport suite, including serialized slow work and ineffective load shedding.

The corrected path explicitly spawns a handler coroutine on the worker executor and awaits its result on the connection executor. Handler exceptions return to the connection's existing error path. One offload reservation still covers the complete handler call.

On the same native macOS fixture, 40 fast requests under four 500 ms slow requests observe a maximum of 1,931.2 ms before and 2.8 ms afterward. Sixteen slow requests at a four-slot limit change from sixteen accepted requests to four accepted and twelve 503 responses. The existing suite changes from four failures to zero. These are one paired fixture run, not production tail, throughput, cost or power measurements.

Qualification artifacts remain in the Detent workspace's ignored `.scratch/http-offload-before-tests.log` and `.scratch/http-offload-after-tests.log`. The isolated comparison changes only executor selection in archived baseline headers, so the result does not depend on the deferred-response implementation.
