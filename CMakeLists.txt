# http: a generic HTTP/1.1 application layer on top of Kronuz/server.
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
#   ./build/kv_store 8080      # a REST app served through the HttpHandler seam
#
# Leg 2 of the "invert Xapiand" work: the application seam (HttpHandler: Request
# -> Response) sits above a generic HttpConnection that parses with http-parser
# and rides Kronuz/server's BaseClient. Search becomes one HttpHandler; the parser
# (http-parser), the transport (Kronuz/server's reactor), and the handler's
# concurrency model are all swappable behind that seam.

cmake_minimum_required(VERSION 3.16)
project(http LANGUAGES C CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

# --- the generic server engine (reactor + TCP + connection FSM) ---
FetchContent_Declare(server GIT_REPOSITORY https://github.com/Kronuz/server.git GIT_TAG main)
FetchContent_MakeAvailable(server)

# --- the radix-tree path router (transport-agnostic) ---
FetchContent_Declare(radix_router GIT_REPOSITORY https://github.com/Kronuz/radix-router.git GIT_TAG main)
FetchContent_MakeAvailable(radix_router)

# --- http-parser (the HTTP parser) ---
# The Joyent http_parser fork, chosen over stricter parsers (e.g. llhttp) because
# it accepts arbitrary request methods: Xapiand's REST API uses custom verbs
# (COUNT, INFO, DUMP, RESTORE, ...) that a method-validating parser rejects. The
# seam stays parser-agnostic, so this is contained inside HttpConnection.
FetchContent_Declare(http_parser GIT_REPOSITORY https://github.com/Kronuz/http-parser.git GIT_TAG main)
FetchContent_MakeAvailable(http_parser)

# --- the http layer (header-only) ---
add_library(http INTERFACE)
target_include_directories(http INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(http INTERFACE cxx_std_20)
target_link_libraries(http INTERFACE server::server http_parser::http_parser radix_router::radix_router)
add_library(http::http ALIAS http)

# --- demo + tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	add_executable(kv_store examples/kv_store.cc)
	target_link_libraries(kv_store PRIVATE http)

	# A throughput benchmark for the Dispatcher's queue (single-queue vs sharded),
	# to decide whether the per-reactor pool needs per-worker run queues.
	add_executable(dispatcher_bench examples/dispatcher_bench.cc)
	target_link_libraries(dispatcher_bench PRIVATE http)

	enable_testing()
	add_executable(http_test test/test.cc)
	target_link_libraries(http_test PRIVATE http)
	add_test(NAME http COMMAND http_test)

	# Drives a real server over TCP to prove the un-stallable model (a slow handler
	# must not stall the loop) + 503 backpressure. Slower (~2s); still a ctest.
	add_executable(http_loadtest test/loadtest.cc)
	target_link_libraries(http_loadtest PRIVATE http)
	add_test(NAME loadtest COMMAND http_loadtest)
	set_tests_properties(loadtest PROPERTIES TIMEOUT 60)
endif()
