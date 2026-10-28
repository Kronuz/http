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

option(HTTP_BUILD_SERVER "Provide the existing HTTP server framework" ON)
option(HTTP_BUILD_CLIENTS "Provide bounded HTTP client mechanisms" OFF)
option(HTTP_BUILD_CLIENT_ASIO "Provide the optional Asio client driver" OFF)
option(HTTP_BUILD_CLIENT_TLS "Provide the optional TLS client driver" OFF)
if(NOT HTTP_BUILD_SERVER AND NOT HTTP_BUILD_CLIENTS)
	return()
endif()

# --- the radix-tree path router (transport-agnostic) ---
if(HTTP_BUILD_SERVER)
find_package(Threads REQUIRED)
FetchContent_Declare(radix_router GIT_REPOSITORY https://github.com/Kronuz/radix-router.git GIT_TAG d03574406e76f7a5dc7d7d4d11eb8bd6c20c6aed)
FetchContent_MakeAvailable(radix_router)
endif()

# --- http-parser (the HTTP parser) ---
# The Joyent http_parser fork, chosen over stricter parsers (e.g. llhttp) because
# it accepts arbitrary request methods: Xapiand's REST API uses custom verbs
# (COUNT, INFO, DUMP, RESTORE, ...) that a method-validating parser rejects. The
# seam stays parser-agnostic, so this is contained inside the connection.
FetchContent_Declare(http_parser GIT_REPOSITORY https://github.com/Kronuz/http-parser.git GIT_TAG 87ae8cf1e20c9e2bac5b503b0869ac2d8b03be90)
FetchContent_MakeAvailable(http_parser)

# Owning allocation contexts, independent of legacy allocation interception.
FetchContent_Declare(allocators GIT_REPOSITORY https://github.com/Kronuz/allocators.git GIT_TAG 4b3981936112e45e06d54cdc922ebd6906787a4f)
FetchContent_MakeAvailable(allocators)
if(HTTP_BUILD_CLIENTS)
	add_library(http_client_core INTERFACE)
	add_library(http::client_core ALIAS http_client_core)
	target_include_directories(http_client_core INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
	target_compile_features(http_client_core INTERFACE cxx_std_20)
	target_link_libraries(http_client_core INTERFACE http_parser::http_parser allocators::resources)
	if(UNIX)
		add_library(http_client http_client_posix.cc)
		add_library(http::client ALIAS http_client)
		target_link_libraries(http_client PUBLIC http::client_core)
	endif()
endif()

if(HTTP_BUILD_SERVER)
# --- compressors (the deflate/gzip + zstd codecs) ---
# Response compression (http_compression.h) negotiates Accept-Encoding and applies a
# coding. The codecs are Kronuz/compressors (has compiled units + lz4/zstd/zlib
# wiring). Previously reached transitively via Kronuz/server; now a direct dependency.
FetchContent_Declare(compressors GIT_REPOSITORY https://github.com/Kronuz/compressors.git GIT_TAG 86ff17d171524977723f070f08d3867d6fdb7364)
FetchContent_MakeAvailable(compressors)
endif()

# --- standalone Asio (the runtime + transport substrate) ---
# The reactor + TCP + C++20-coroutine transport (http_asio.h) rides on standalone
# Asio. Fetched rather than taken from a system/brew prefix (dependency policy);
# header-only, so ASIO_STANDALONE + the include path is all it needs -- the
# coroutine support comes from -std=c++20. Set ASIO_INCLUDE_DIR to reuse an
# existing checkout instead of fetching.
if(HTTP_BUILD_SERVER OR (HTTP_BUILD_CLIENTS AND (HTTP_BUILD_CLIENT_ASIO OR HTTP_BUILD_CLIENT_TLS)))
find_package(Threads REQUIRED)
set(ASIO_INCLUDE_DIR "" CACHE PATH "use an existing standalone Asio include dir instead of fetching")
if(ASIO_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(asio GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git GIT_TAG asio-1-36-0)
	FetchContent_MakeAvailable(asio)
	set(ASIO_INCLUDE_DIR "${asio_SOURCE_DIR}/asio/include")
endif()
if(HTTP_BUILD_CLIENTS AND (HTTP_BUILD_CLIENT_ASIO OR HTTP_BUILD_CLIENT_TLS))
	add_library(http_client_asio INTERFACE)
	add_library(http::client_asio ALIAS http_client_asio)
	target_include_directories(http_client_asio INTERFACE "${ASIO_INCLUDE_DIR}")
	target_compile_definitions(http_client_asio INTERFACE ASIO_STANDALONE)
	target_link_libraries(http_client_asio INTERFACE http::client_core Threads::Threads)
endif()
endif()

# --- reactor: the generic Asio server runtime (shared reactor pool + accept + shutdown) ---
# The reactor pool, the two accept/bind strategies, the offload gate, the abortable
# registry, and the abort->stop->join shutdown were extracted from http_asio.h into
# Kronuz/reactor so other Kronuz network services share one runtime. Header-only; rides
# on the same standalone Asio (so no second Asio fetch -- just its include dir). Set
# REACTOR_INCLUDE_DIR to use a local checkout instead of fetching (dev iteration).
if(HTTP_BUILD_SERVER OR (HTTP_BUILD_CLIENTS AND HTTP_BUILD_CLIENT_TLS))
set(REACTOR_INCLUDE_DIR "" CACHE PATH "use an existing Kronuz/reactor dir instead of fetching")
if(REACTOR_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(reactor GIT_REPOSITORY https://github.com/Kronuz/reactor.git GIT_TAG e19afd966d5cf18e377649398096be66fb40e800)
	FetchContent_MakeAvailable(reactor)
	set(REACTOR_INCLUDE_DIR "${reactor_SOURCE_DIR}")
endif()
endif()

# --- the http framework (header-only) ---
if(HTTP_BUILD_CLIENTS AND HTTP_BUILD_CLIENT_TLS)
	find_package(OpenSSL 3.0 REQUIRED)
	add_library(http_client_tls http_client_tls.cc)
	add_library(http::client_tls ALIAS http_client_tls)
	target_include_directories(http_client_tls PUBLIC "${REACTOR_INCLUDE_DIR}")
	target_link_libraries(http_client_tls PUBLIC http::client_asio OpenSSL::SSL OpenSSL::Crypto)
endif()
if(HTTP_BUILD_SERVER)
add_library(http INTERFACE)
target_include_directories(http INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}" "${ASIO_INCLUDE_DIR}" "${REACTOR_INCLUDE_DIR}")
target_compile_features(http INTERFACE cxx_std_20)
target_compile_definitions(http INTERFACE ASIO_STANDALONE)
target_link_libraries(http INTERFACE http_parser::http_parser radix_router::radix_router compressors::compressors Threads::Threads allocators::resources)
add_library(http::http ALIAS http)
endif()

# --- demo + benchmarks + tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND HTTP_BUILD_SERVER)
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
	target_link_libraries(http_owned_stream_test PRIVATE Threads::Threads allocators::resources)
	add_test(NAME owned_stream COMMAND http_owned_stream_test)
	set_tests_properties(owned_stream PROPERTIES TIMEOUT 30)
	if(HTTP_BUILD_CLIENTS)
		add_executable(http_held_response_test test/held_response_test.cc)
		target_link_libraries(http_held_response_test PRIVATE http::http)
		add_test(NAME held_response COMMAND http_held_response_test)
		set_tests_properties(held_response PROPERTIES TIMEOUT 30)
	endif()
	add_executable(http_owned_stream_asio_test test/owned_stream_asio_test.cc)
	target_link_libraries(http_owned_stream_asio_test PRIVATE http)
	add_test(NAME owned_stream_asio COMMAND http_owned_stream_asio_test)
	set_tests_properties(owned_stream_asio PROPERTIES TIMEOUT 30)
	add_executable(offload_test test/offload_test.cc)
	target_link_libraries(offload_test PRIVATE http)
	add_test(NAME offload COMMAND offload_test)
	set_tests_properties(offload PROPERTIES TIMEOUT 15)
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

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND HTTP_BUILD_CLIENTS)
	enable_testing()
	add_executable(http_client_core_test test/client_core_test.cc)
	target_link_libraries(http_client_core_test PRIVATE http::client_core)
	add_test(NAME client_core COMMAND http_client_core_test)
	if(TARGET http::client_tls)
		add_executable(http_client_tls_test test/client_tls_test.cc)
		target_link_libraries(http_client_tls_test PRIVATE http::client_tls)
		add_test(NAME client_tls COMMAND http_client_tls_test)
		set_tests_properties(client_tls PROPERTIES TIMEOUT 30)
	endif()
	if(TARGET http::client_asio)
		add_executable(http_client_asio_test test/client_asio_test.cc)
		target_link_libraries(http_client_asio_test PRIVATE http::client_asio)
		add_test(NAME client_asio COMMAND http_client_asio_test)
		set_tests_properties(client_asio PROPERTIES TIMEOUT 30)
	endif()
	if(UNIX)
		add_executable(http_client_get examples/client_get.cc)
		target_link_libraries(http_client_get PRIVATE http::client)
		find_package(Threads REQUIRED)
		add_executable(http_client_posix_test test/client_posix_test.cc)
		target_link_libraries(http_client_posix_test PRIVATE http::client Threads::Threads)
		add_test(NAME client_posix COMMAND http_client_posix_test)
		set_tests_properties(client_posix PROPERTIES TIMEOUT 30)
	endif()
	set_tests_properties(client_core PROPERTIES TIMEOUT 30)
endif()
