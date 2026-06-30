/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// StallWatchdog: makes the never-stalling guarantee observable instead of silent.
//
// In the un-stallable model handlers run on workers, so the reactor should never
// block. This catches the cases where it does anyway -- a handler misclassified as
// cheap (run inline) that turns out to block, or a bug in the engine. A periodic
// ev::timer on the loop "pets" a timestamp every tick; a separate monitor thread
// checks that the last pet is recent. Because the timer forces the loop to wake
// even when it is idle, a stale timestamp means the loop is genuinely stuck in
// something, not merely waiting for I/O. On the healthy->stalled edge the monitor
// invokes on_stall (default: a line to stderr); wire it to a metric/alert in prod.
// (cf. Envoy's watchdog, Seastar's stall detector.)

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <thread>
#include <utility>

#include "ev/ev++.h"

namespace http {

class StallWatchdog {
public:
	using clock = std::chrono::steady_clock;
	using Callback = std::function<void(std::chrono::milliseconds)>;

	// `threshold`: how long the loop may go without ticking before it counts as
	// stalled. `on_stall`: invoked from the monitor thread on each healthy->stalled
	// transition (default logs to stderr).
	StallWatchdog(ev::loop_ref& loop, std::chrono::milliseconds threshold, Callback on_stall = {})
		: pet_timer_(loop),
		  threshold_ms_(threshold.count() > 0 ? threshold.count() : 1),
		  on_stall_(on_stall ? std::move(on_stall) : default_callback()) {
		pet_timer_.set<StallWatchdog, &StallWatchdog::pet_cb>(this);
	}

	~StallWatchdog() {
		stop();
		pet_timer_.stop();   // safe: the loop is no longer running by destruction time
	}

	StallWatchdog(const StallWatchdog&) = delete;
	StallWatchdog& operator=(const StallWatchdog&) = delete;

	// Start petting + monitoring. The timer touches the loop, so this must run on
	// the loop thread (before the loop runs is fine); the monitor is its own thread.
	void start() {
		if (running_.exchange(true)) { return; }
		last_pet_ns_.store(now_ns(), std::memory_order_relaxed);
		double interval = (static_cast<double>(threshold_ms_) / 4.0) / 1000.0;   // pet ~4x per window
		pet_timer_.start(interval, interval);
		monitor_ = std::thread([this] { monitor_loop(); });
	}

	// Stop monitoring (joins the monitor thread). Thread-safe: it does not touch the
	// loop, so it may be called from any thread (e.g. alongside break_loop). The
	// timer is stopped in the destructor, once the loop has stopped running.
	void stop() {
		if (!running_.exchange(false)) { return; }
		if (monitor_.joinable()) { monitor_.join(); }
	}

private:
	static int64_t now_ns() {
		return std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch()).count();
	}

	// On the loop: a tick. If the loop is stuck this never runs, so the timestamp
	// goes stale and the monitor notices.
	void pet_cb(ev::timer&, int) {
		last_pet_ns_.store(now_ns(), std::memory_order_relaxed);
	}

	void monitor_loop() {
		auto check = std::chrono::milliseconds(std::max<int64_t>(1, threshold_ms_ / 4));
		bool stalled = false;
		while (running_.load(std::memory_order_relaxed)) {
			std::this_thread::sleep_for(check);
			if (!running_.load(std::memory_order_relaxed)) { break; }
			int64_t elapsed_ms = (now_ns() - last_pet_ns_.load(std::memory_order_relaxed)) / 1'000'000;
			if (elapsed_ms > threshold_ms_) {
				if (!stalled) { stalled = true; on_stall_(std::chrono::milliseconds(elapsed_ms)); }   // edge: fire once per episode
			} else {
				stalled = false;
			}
		}
	}

	static Callback default_callback() {
		return [](std::chrono::milliseconds elapsed) {
			std::fprintf(stderr, "[stall-watchdog] reactor loop has not ticked in %lld ms\n",
			             static_cast<long long>(elapsed.count()));
		};
	}

	ev::timer pet_timer_;
	std::thread monitor_;
	std::atomic<int64_t> last_pet_ns_{0};
	std::atomic<bool> running_{false};
	int64_t threshold_ms_;
	Callback on_stall_;
};

}  // namespace http
