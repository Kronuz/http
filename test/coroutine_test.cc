// coroutine_test -- Phase 7c L3: handlers are coroutines on the connection's loop.
// Each section proves one claim of docs/mechanisms/coroutine-handlers.md over real TCP:
//   [A] a suspended handler does not stall the loop, and a throw after a suspension is mapped
//   [B] BodyStream semantics (order, eof, error, bound, cancel)
//   [C] a reset cancels reads but never writes; a half-close (EOF) cancels nothing; a pipelined byte ends the watch
//   [D] the in-flight gate answers 503 + Retry-After and counts the refusals
//   [E] the buffered-body cap answers 413 (Content-Length and chunked), streams are not capped
//   [F] shutdown: cancel, the response is still written, frames outlive the cancel, bounded by the grace
//   [G] pipelined and keep-alive requests are unaffected by the per-request watch
//   [H] a body cut short (FIN, half-close, RST, chunked) fails the reader, never eof, with the request cancelled
//   [I] a throwing cancel callback is dropped: the handler's frames outlive it, streamed and buffered
//   [J] a half-closed client keeps its response (D126): a plain handler, a long poll held on its
//       token to its own bound; a reset or a failed write cancels, a failed write included after
//       the peer half-closed and then left
//   [S] the streamed join: a hang-up after the body cancels the handler; a handler that answers early stops the feed
//   [T] the task loop (design T4): (b) budget demotion, (c) affinity after a std::thread resume,
//       (d) stop() with suspended handlers leaves no task frame, (e) task::cancelled after a
//       cancellation is 503 + close, (f) no checkpoint passes after a hang-up, (g) an owned
//       stream abandoned by task::cancelled writes no terminal chunk, (h) writer affinity
//   [X] the teardown phases across the service and an application's own scheduler

#include "coroutine/sections.h"

int main() {
	std::printf("== coroutine handler tests ==\n");
	check(affinity_death_test(get("/affine-body", "Connection: close\r\n")), "[T] (h) a handler body that moved away and then writes fails the writer's affinity check (Debug)");
	check(affinity_death_test(get("/affine-thread", "Connection: close\r\n")), "[T] (h) a raw thread (no task, off the loop) that writes fails the writer's affinity check (Debug)");
	check(affinity_death_test(post("/affine-read", "abc", "Connection: close\r\n")), "[T] (h) a handler body that moved away and then reads its BodyStream fails the read's affinity check (Debug)");
	App app;

	section_A(app);
	body_stream_tests();
	section_C(app);
	section_J(app);
	section_D(app);
	section_S(app);
	section_E(app);
	section_F(app);
	section_G(app);
	section_H(app);
	section_I(app);
	section_T(app);
	section_X(app);

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
