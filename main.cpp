#include <iostream>

#include "rdma_server.h"

int main() {
    rdma::Server server;
    server.InitConnection();
    return 0;
}
