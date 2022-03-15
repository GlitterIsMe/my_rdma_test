cmake_minimum_required(VERSION 3.16)
project(rdma)

set(CMAKE_CXX_STANDARD 14)
find_package(PkgConfig REQUIRED)
pkg_check_modules(IB REQUIRED libibverbs)
pkg_check_modules(GFLAGS REQUIRED gflags)

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
target_sources(rdma_server PUBLIC
        "main.cpp"
        "rdma_server.cc"
        "sock.cc")
target_include_directories(rdma_server PUBLIC . ${IB_INCLUDE_DIRS} ${GFLAGS_INCLUDE_DIRS})
target_link_libraries(rdma_server ${IB_LIBRARIES} ${GFLAGS_LIBRARIES})


