#include <iostream>

#include <gflags/gflags.h>

#include "rdma_server.h"

DEFINE_bool(is_server, true, "Is this instance a server or a client");

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    rdma::Server server(FLAGS_is_server);
    server.InitConnection();
    char buf[1024];
    char read_buf[1024];
    memset(read_buf, 0, 1024);
    sprintf(buf, "hello world");
    if (!FLAGS_is_server) {
        server.Write(buf, strlen(buf));
        server.Read(read_buf, strlen(buf));
        printf("read from server: %s\n", read_buf);
    }
    getchar();
    return 0;
}
