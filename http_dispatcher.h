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

// Dispatcher: a bounded worker pool for running heavy handler work *off* the
// reactor thread, so a slow or blocking handler never stalls the event loop.
//
// This is the core of the un-stallable model. Two properties make it safe under
// load: the work runs on workers (the loop keeps accepting and reading), and the
// queue is *bounded* -- submit() returns false when it is full, so overload sheds
// load (the caller answers 503 Service Unavailable) instead of growing an
// unbounded backlog that blows latency and memory. Response bytes are still
// written through the connection's thread-safe write path (Kronuz/server enqueues
// them and wakes the reactor), so a worker can drive a response without touching
// the loop directly.

#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <thread>
#include <vector>

#include "queue.h"   // Kronuz/queue: thread-safe, bounded, blocking MPMC queue

namespace http {

class Dispatcher {
	queue::Queue<std::function<void()>> queue_;
	std::vector<std::thread> workers_;

public:
	// `threads` worker threads draining a queue bounded at `queue_limit` tasks.
	Dispatcher(std::size_t threads, std::size_t queue_limit)
		: queue_(queue_limit) {
		workers_.reserve(threads);
		for (std::size_t i = 0; i < threads; ++i) {
			workers_.emplace_back([this] {
				std::function<void()> task;
				while (queue_.pop_front(task, -1.0)) {   // blocks; false once finished + drained
					task();
				}
			});
		}
	}

	~Dispatcher() { stop(); }

	Dispatcher(const Dispatcher&) = delete;
	Dispatcher& operator=(const Dispatcher&) = delete;

	// Try to enqueue work. Returns false if the bounded queue is full -- that is
	// the backpressure signal; the caller should answer 503 rather than block the
	// reactor. Non-blocking (timeout 0).
	bool submit(std::function<void()> task) {
		return queue_.push_back(std::move(task), 0.0);
	}

	// Number of tasks queued but not yet started (for metrics / load shedding).
	std::size_t pending() const { return queue_.size(); }

	// Drain queued work, let in-flight tasks finish, then join the workers.
	void stop() {
		while (!queue_.empty()) {                       // let queued tasks be picked up
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		queue_.finish();                                // wake idle workers -> pop returns false
		for (auto& t : workers_) {
			if (t.joinable()) { t.join(); }
		}
		workers_.clear();
	}
};

}  // namespace http
