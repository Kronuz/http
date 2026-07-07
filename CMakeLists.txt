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
FetchContent_Declare(radix_router GIT_REPOSITORY https://github.com/Kronuz/radix-router.git GIT_TAG main)
FetchContent_MakeAvailable(radix_router)

# --- http-parser (the HTTP parser) ---
# The Joyent http_parser fork, chosen over stricter parsers (e.g. llhttp) because
# it accepts arbitrary request methods: Xapiand's REST API uses custom verbs
# (COUNT, INFO, DUMP, RESTORE, ...) that a method-validating parser rejects. The
# seam stays parser-agnostic, so this is contained inside the connection.
FetchContent_Declare(http_parser GIT_REPOSITORY https://github.com/Kronuz/http-parser.git GIT_TAG main)
FetchContent_MakeAvailable(http_parser)

# --- compressors (the deflate/gzip + zstd codecs) ---
# Response compression (http_compression.h) negotiates Accept-Encoding and applies a
# coding. The codecs are Kronuz/compressors (has compiled units + lz4/zstd/zlib
# wiring). Previously reached transitively via Kronuz/server; now a direct dependency.
FetchContent_Declare(compressors GIT_REPOSITORY https://github.com/Kronuz/compressors.git GIT_TAG main)
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
# The reactor pool, the two accept/bind strategies, the offload gate, the abortable
# registry, and the abort->stop->join shutdown were extracted from http_asio.h into
# Kronuz/reactor so other Kronuz network services share one runtime. Header-only; rides
# on the same standalone Asio (so no second Asio fetch -- just its include dir). Set
# REACTOR_INCLUDE_DIR to use a local checkout instead of fetching (dev iteration).
set(REACTOR_INCLUDE_DIR "" CACHE PATH "use an existing Kronuz/reactor dir instead of fetching")
if(REACTOR_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(reactor GIT_REPOSITORY https://github.com/Kronuz/reactor.git GIT_TAG main)
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

	# Transport-agnostic unit tests: method dispatch, 404 vs 405, param capture.
	add_executable(http_test test/test.cc)
	target_link_libraries(http_test PRIVATE http)
	add_test(NAME http COMMAND http_test)

	# The Asio transport (http_asio.h) end-to-end over real TCP: the un-stallable
	# model, 503 backpressure, N reactors (both bind modes), keep-alive, compression,
	# streaming request-body intake, and error mapping.
	add_executable(asio_test test/asio_test.cc)
	target_link_libraries(asio_test PRIVATE http)
	add_test(NAME asio COMMAND asio_test)
	set_tests_properties(asio PROPERTIES TIMEOUT 60)
endif()
