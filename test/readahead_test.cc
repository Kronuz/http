// readahead_test -- the connection's disconnect watch reads ahead (ConnectionInput) and the
// reply is written in slices that span its buffers (write_with_progress). Both change how bytes
// move between the kernel and the parser, so every semantic that rides on those bytes is pinned
// here over real TCP:
//   [U] ConnectionInput on its own: the watch holds at most kReadAhead bytes outside the kernel,
//       hands them back in order whatever the output size, arms nothing until they are consumed,
//       and keeps the disconnect rules (a reset fires; EOF, a half-close, and data end the watch)
//   [L] the lazy watch: it reads nothing until the handler asks (CancelWatch::engage), and the
//       first question decides what happened meanwhile as the read would have; the Request hook
//       engages it once, from the first cancel_token(); a read that finds the watch's read
//       pending is resumed by it, its token ends the wait, and drain() waits for it
//   [H] a heavy reader's read is resumed through its scheduler, a light one's inline
//   [Q] the same over real TCP: a reset before the first question is seen by it, a half-close is not, pipelined input
//       before it ends the watch, a handler that never asks has its pipelined successors answered
//   [N] a buffered reply sent at once as far as the socket takes it: replies to 4 MiB arrive whole
//   [P] a pipelined request that arrives inside the read-ahead (while the handler of the one
//       before runs) is parsed and answered, at every alignment of the 4096-byte window against
//       the request line, the headers, the CRLF CRLF, the body and the next request
//   [F] a flood of pipelined requests is answered in order, none lost or repeated
//   [D] disconnect: the watch's data ends it for that request; a half-close after the pipelined
//       requests cancels none of them, all are answered in order and no read follows the EOF; a
//       reset is seen by the request in flight
//   [B] a client that disconnects in the middle of a buffered body never reaches the handler,
//       and the request it held is released
//   [S] request smuggling: ambiguous framing and a request hidden in a body are never executed
//       as a request, as the first request and as one that arrives inside the read-ahead; trailer
//       fields never become headers; nothing after a CONNECT, or after an HTTP/1.0 message with
//       Transfer-Encoding, is read as a request
//   [W] the reply's inactivity slices: every byte arrives, at every alignment of head and body
//       against the 64 KiB slice
//   [A] read_ahead(): the idle read goes through the watch's own buffer, appends at most
//       kReadAhead bytes, and a token requested before it starts arms nothing
//   [I] idle: the body scratch released at each idle wait comes back for the next body, after a
//       long idle, for headers split across reads and headers larger than the read-ahead
#include "readahead/input.h"
#include "readahead/sections.h"

int main() {
	std::signal(SIGPIPE, SIG_IGN);
	std::printf("== read-ahead watch and sliced write tests ==\n");
#ifndef READAHEAD_TEST_INTEGRATION_ONLY
	input_tests();
	read_ahead_tests();
#endif

	section_I();
	section_P();
	section_F();
	section_D();
	section_B();
	section_S();
	section_Q();
	section_N();
	section_W();

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
