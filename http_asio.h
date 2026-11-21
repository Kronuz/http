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

// The Asio connection + HTTP service for Kronuz/http -- the transport layer. The generic
// server runtime (the shared-nothing reactor pool, the accept/bind strategies, the
// admission gate, and the graceful abort->stop->join shutdown) lives in Kronuz/reactor;
// this file is what makes that runtime speak HTTP. Everything above it is unchanged: the
// HttpHandler seam, the radix router, http_message, http-parser (via RequestParser), the
// codings.
//
// A connection is a task::task (the reactor::Session detail::serve_connection) and so is a
// handler: handle() is awaited on the connection's loop as a child task, nothing runs off it,
// and a handler that must wait suspends. The connection is the single owner of its
// request/response, so the ordering is structural. While a handler runs, the connection
// watches the socket for a hang-up and tracks the request for shutdown; both cancel it
// (Request::cancel) without destroying its frame. The session's token (SessionContext::stop)
// reaches only the waits shutdown should end: the reads of the next request and the TLS
// handshake, never the handler or a reply's write.
//
// Scaling is the runtime's shared-nothing / thread-per-core model: N io_contexts on N
// threads, one port (SO_REUSEPORT or a portable shared acceptor), and a bounded in-flight
// admission gate per reactor. HttpAsioService is a thin adapter over reactor::TcpServer.
// Response compression / conditional / range transforms are applied on the buffered
// response (the http_* helpers). Request-body intake has three modes, chosen per endpoint:
// buffered into Request::body up to a cap (then 413), pushed to a BodySink, or streamed to
// a BodyStream the handler reads with co_await -- a multi-gigabyte RESTORE/bulk load flows
// through in O(read-buffer) memory, never held whole.

#pragma once
// The pieces, in dependency order: http_asio/options.h (options, Limits, TLS),
// response_writer.h, connection_io.h, connection_state.h, serve_connection.h, service.h.
#include "http_asio/options.h"
#include "http_asio/response_writer.h"
#include "http_asio/connection_io.h"
#include "http_asio/connection_state.h"
#include "http_asio/serve_connection.h"
#include "http_asio/service.h"
