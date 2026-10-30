# http: a generic, extensible HTTP/1.1 application framework on standalone Asio.
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
#   ./build/kv_store 8080      # a REST app served through the HttpHandler seam
#
# An application implements one interface -- HttpHandler (Request in, response
# written out) -- and the framework does the rest: an Asio C++20-coroutine
# connection parses with http-parser, routes on a radix tree, negotiates content
# and encodings, streams or buffers the body, runs the handler (inline or offloaded
# so a slow handler never stalls the loop), and frames the response. The search
# engine becomes one HttpHandler; the demo is another.

cmake_minimum_required(VERSION 3.16)
project(http LANGUAGES C CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# --- the radix-tree path router (transport-agnostic) ---
FetchContent_Declare(radix_router GIT_REPOSITORY https://github.com/Kronuz/radix-router.git GIT_TAG d03574406e76f7a5dc7d7d4d11eb8bd6c20c6aed)
FetchContent_MakeAvailable(radix_router)

# --- http-parser (the HTTP parser) ---
# The Joyent http_parser fork, chosen over stricter parsers (e.g. llhttp) because
# it accepts arbitrary request methods: Xapiand's REST API uses custom verbs
# (COUNT, INFO, DUMP, RESTORE, ...) that a method-validating parser rejects. The
# seam stays parser-agnostic, so this is contained inside the connection.
FetchContent_Declare(http_parser GIT_REPOSITORY https://github.com/Kronuz/http-parser.git GIT_TAG 178f873450364b67f0abca9c96fbbc735589d435)
FetchContent_MakeAvailable(http_parser)

# --- compressors (the deflate/gzip + zstd codecs) ---
# Response compression (http_compression.h) negotiates Accept-Encoding and applies a
# coding. The codecs are Kronuz/compressors (has compiled units + lz4/zstd/zlib
# wiring). Previously reached transitively via Kronuz/server; now a direct dependency.
FetchContent_Declare(compressors GIT_REPOSITORY https://github.com/Kronuz/compressors.git GIT_TAG ed590c87201afd270afbfbb2d58d5d467c6d23b6)
FetchContent_MakeAvailable(compressors)

# --- standalone Asio (the runtime + transport substrate) ---
# The reactor + TCP + C++20-coroutine transport (http_asio.h) rides on standalone
# Asio. Fetched rather than taken from a system/brew prefix (dependency policy);
# header-only, so ASIO_STANDALONE + the include path is all it needs -- the
# coroutine support comes from -std=c++20. Set ASIO_INCLUDE_DIR to reuse an
# existing checkout instead of fetching.
set(ASIO_INCLUDE_DIR "" CACHE PATH "use an existing standalone Asio include dir instead of fetching")
if(ASIO_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(asio GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git GIT_TAG asio-1-36-0)
	FetchContent_MakeAvailable(asio)
	set(ASIO_INCLUDE_DIR "${asio_SOURCE_DIR}/asio/include")
endif()

# --- reactor: the generic Asio server runtime (shared reactor pool + accept + shutdown) ---
# The reactor pool, the two accept/bind strategies, the admission gate, the abortable
# list, and the abort->grace->force->stop->join shutdown were extracted from http_asio.h into
# Kronuz/reactor so other Kronuz network services share one runtime. Header-only; rides
# on the same standalone Asio (so no second Asio fetch -- just its include dir). Set
# REACTOR_INCLUDE_DIR to use a local checkout instead of fetching (dev iteration).
set(REACTOR_INCLUDE_DIR "" CACHE PATH "use an existing Kronuz/reactor dir instead of fetching")
if(REACTOR_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(reactor GIT_REPOSITORY https://github.com/Kronuz/reactor.git GIT_TAG 67e22a66877e6bf4934273eae0b292b0d7daed5f)
	FetchContent_MakeAvailable(reactor)
	set(REACTOR_INCLUDE_DIR "${reactor_SOURCE_DIR}")
endif()

# --- the http framework (header-only) ---
add_library(http INTERFACE)
target_include_directories(http INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}" "${ASIO_INCLUDE_DIR}" "${REACTOR_INCLUDE_DIR}")
target_compile_features(http INTERFACE cxx_std_20)
target_compile_definitions(http INTERFACE ASIO_STANDALONE)
target_link_libraries(http INTERFACE http_parser::http_parser radix_router::radix_router compressors::compressors Threads::Threads)
add_library(http::http ALIAS http)

# --- demo + benchmarks + tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	add_executable(kv_store examples/kv_store.cc)
	target_link_libraries(kv_store PRIVATE http)

	# Raw-Asio throughput ceiling (no framework): the reference the framework is
	# measured against. SO_REUSEPORT multi-reactor; run on Linux for real numbers.
	add_executable(bench_asio examples/bench_asio.cc)
	target_link_libraries(bench_asio PRIVATE http)

	# Framework throughput: the same fixed response served through HttpAsioService +
	# a trivial HttpHandler -- how close the framework gets to the raw ceiling.
	add_executable(bench_http examples/bench_http.cc)
	target_link_libraries(bench_http PRIVATE http)

	enable_testing()
	add_executable(http_owned_stream_test test/owned_stream_test.cc)
	target_include_directories(http_owned_stream_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
	target_link_libraries(http_owned_stream_test PRIVATE Threads::Threads)
	add_test(NAME owned_stream COMMAND http_owned_stream_test)
	set_tests_properties(owned_stream PROPERTIES TIMEOUT 30)
	add_executable(http_owned_stream_asio_test test/owned_stream_asio_test.cc)
	target_link_libraries(http_owned_stream_asio_test PRIVATE http)
	add_test(NAME owned_stream_asio COMMAND http_owned_stream_asio_test)
	set_tests_properties(owned_stream_asio PROPERTIES TIMEOUT 30)
	add_executable(coroutine_test test/coroutine_test.cc)
	target_link_libraries(coroutine_test PRIVATE http)
	add_test(NAME coroutine COMMAND coroutine_test)
	set_tests_properties(coroutine PROPERTIES TIMEOUT 120)
	add_executable(http_deferred_test test/deferred_test.cc)
	target_include_directories(http_deferred_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
	target_link_libraries(http_deferred_test PRIVATE Threads::Threads)
	add_test(NAME deferred COMMAND http_deferred_test)
	set_tests_properties(deferred PROPERTIES TIMEOUT 60)

	# Transport-agnostic unit tests: method dispatch, 404 vs 405, param capture.
	add_executable(http_test test/test.cc)
	target_link_libraries(http_test PRIVATE http)
	add_test(NAME http COMMAND http_test)

	add_executable(deferred_asio_test test/deferred_asio_test.cc)
	target_link_libraries(deferred_asio_test PRIVATE http)
	add_test(NAME deferred_asio COMMAND deferred_asio_test)
	set_tests_properties(deferred_asio PROPERTIES TIMEOUT 60)

	# The service's reactor lag probe.
	add_executable(lag_asio_test test/lag_asio_test.cc)
	target_link_libraries(lag_asio_test PRIVATE http)
	add_test(NAME lag_asio COMMAND lag_asio_test)
	set_tests_properties(lag_asio PROPERTIES TIMEOUT 30)

	# The Asio transport (http_asio.h) end-to-end over real TCP: the un-stallable
	# model, 503 backpressure, N reactors (both bind modes), keep-alive, compression,
	# streaming request-body intake, and error mapping.
	add_executable(asio_test test/asio_test.cc)
	target_link_libraries(asio_test PRIVATE http)
	add_test(NAME asio COMMAND asio_test)
	set_tests_properties(asio PROPERTIES TIMEOUT 60)

	# The TLS connection factory and its failure hook, when reactor built its TLS layer.
	if(TARGET reactor::tls)
		add_executable(tls_asio_test test/tls_asio_test.cc)
		target_link_libraries(tls_asio_test PRIVATE http reactor::tls)
		add_test(NAME tls_asio COMMAND tls_asio_test)
		set_tests_properties(tls_asio PROPERTIES TIMEOUT 60)
	endif()

	# The idle, header, and inactivity timeouts over real TCP.
	add_executable(timeouts_test test/timeouts_test.cc)
	target_link_libraries(timeouts_test PRIVATE http)
	add_test(NAME timeouts COMMAND timeouts_test)
	set_tests_properties(timeouts PROPERTIES TIMEOUT 60)
endif()
