cmake_minimum_required(VERSION 3.16)
project(rdma)

set(CMAKE_CXX_STANDARD 14)
include(FindThreads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(IB REQUIRED libibverbs)
pkg_check_modules(GFLAGS REQUIRED gflags)
pkg_check_modules(LIBPMEM REQUIRED libpmem)

# Test for server socket
add_executable(server "")
target_sources(server PUBLIC
        "sock.cc"
        "server.cc")
target_include_directories(server PUBLIC .)

# test for client socket
add_executable(client "")
target_sources(client PUBLIC
        "sock.cc"
        "client.cc")
target_include_directories(client PUBLIC .)

add_executable(rdma_server "")

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    target_compile_definitions(rdma_server PUBLIC -DDEBUG)
endif ()

target_sources(rdma_server PUBLIC
        "main.cpp"
        "rdma_server.cc"
        "sock.cc"
        "pmem.cc"
        "util.h")
target_include_directories(rdma_server PUBLIC
        .
        ${IB_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_DIRS}
        ${LIBPMEM_INCLUDE_DIRS})

target_link_libraries(rdma_server
        ${IB_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${LIBPMEM_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT})


