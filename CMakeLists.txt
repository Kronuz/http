# http: a generic HTTP/1.1 application layer on top of Kronuz/server.
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
#   ./build/kv_store 8080      # a REST app served through the HttpHandler seam
#
# Leg 2 of the "invert Xapiand" work: the application seam (HttpHandler: Request
# -> Response) sits above a generic HttpConnection that parses with llhttp and
# rides Kronuz/server's BaseClient. Search becomes one HttpHandler; the parser
# (llhttp), the transport (Kronuz/server's reactor), and the handler's
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

# --- llhttp (the HTTP parser) ---
# FetchContent, not a system/brew install, to stay self-contained and version-
# pinned like the rest of the family. The release/* tags ship the pre-generated C
# (llhttp.c) + a CMakeLists, so this builds with no Node toolchain.
FetchContent_Declare(llhttp GIT_REPOSITORY https://github.com/nodejs/llhttp.git GIT_TAG release/v9.2.1)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON  CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(llhttp)

# --- the http layer (header-only) ---
add_library(http INTERFACE)
target_include_directories(http INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(http INTERFACE cxx_std_20)
target_link_libraries(http INTERFACE server::server llhttp::llhttp)
add_library(http::http ALIAS http)

# --- demo (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	add_executable(kv_store examples/kv_store.cc)
	target_link_libraries(kv_store PRIVATE http)
endif()
