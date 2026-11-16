cmake_minimum_required(VERSION 3.20)
project(http_idle_cost LANGUAGES C CXX)
# The product headers under test: this checkout by default, or another (a baseline) with the
# same bench sources.
set(HTTP_IDLE_PRODUCT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "the http checkout whose headers are measured")
get_filename_component(product "${HTTP_IDLE_PRODUCT_DIR}" ABSOLUTE)
add_subdirectory("${product}" http-product)
find_package(OpenSSL 3 REQUIRED)
foreach(dir product bench)
  if(dir STREQUAL "product")
    set(source "${product}")
  else()
    set(source "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()
  execute_process(COMMAND git -C "${source}" rev-parse HEAD OUTPUT_VARIABLE ${dir}_commit
    OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
endforeach()
# OpenSSL's headers first, so they match the library that links: Apple clang searches
# /usr/local/include before -isystem directories (the imported targets' include), so the
# libraries link by path and the headers come in as the first -I.
get_filename_component(openssl_include "${OPENSSL_INCLUDE_DIR}" REALPATH)
foreach(target http_idle_cost http_idle_owners)
  add_executable(${target} server.cc)
  target_compile_options(${target} BEFORE PRIVATE "-I${openssl_include}")
  target_link_libraries(${target} PRIVATE http "${OPENSSL_SSL_LIBRARY}" "${OPENSSL_CRYPTO_LIBRARY}")
  target_compile_definitions(${target} PRIVATE HTTP_IDLE_PRODUCT_COMMIT="${product_commit}" HTTP_IDLE_BENCH_COMMIT="${bench_commit}")
endforeach()
target_compile_definitions(http_idle_cost PRIVATE HTTP_IDLE_OWNERS=0)
target_compile_definitions(http_idle_owners PRIVATE HTTP_IDLE_OWNERS=1 TASK_COUNT_FRAMES=1 TASK_FRAME_POOL=0)
if(APPLE)
  target_link_libraries(http_idle_owners PRIVATE proc)
endif()
