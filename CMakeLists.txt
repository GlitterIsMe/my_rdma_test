cmake_minimum_required(VERSION 3.16)
project(rdma)

set(CMAKE_CXX_STANDARD 14)

add_executable(server "")
add_executable(client "")

target_sources(server PUBLIC
        "sock.cc"
        "server.cc")
target_include_directories(server PUBLIC .)

target_sources(client PUBLIC
        "sock.cc"
        "client.cc")
target_include_directories(client PUBLIC .)


