#include <iostream>

#include <gflags/gflags.h>

#include "rdma_server.h"
#include "pmem.h"

DEFINE_bool(is_server, true, "Is this instance a server or a client");

DEFINE_string(pmem_path, "/mnt/pmem/rdma", "The path to the pmem");

DEFINE_uint64(pmem_size, 1UL * 1024 * 1024 * 1024, "The size of the pmem file");

DEFINE_uint64(ops, 10000, "Ops");

DEFINE_uint64(block_size, 256, "Block size");

DEFINE_uint64(max_batch_signal, 16, "The number of WRs for one signal");

DEFINE_uint64(max_post_list, 16, "The max number of WRs for each post send");

DEFINE_bool(random, false, "Access pattern");

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    char* buf;
    if (FLAGS_is_server) {
        //rdma::pmem_size = FLAGS_pmem_size;
        //buf = rdma::map_pmem_file(FLAGS_pmem_path.c_str());
        buf = new char[1UL * 1024 * 1024 * 1024];
    } else {
        buf = new char[1UL * 1024 * 1024];
    }
    rdma::Server server(FLAGS_is_server, buf);
    server.InitConnection();

    /*
     * Test for RDMA read and write
    char buf[1024];
    char read_buf[1024];
    memset(read_buf, 0, 1024);
    sprintf(buf, "hello world");
    if (!FLAGS_is_server) {
        server.Write(buf, strlen(buf));
        server.Read(read_buf, strlen(buf));
        printf("read from server: %s\n", read_buf);
    }*/

    if (!FLAGS_is_server) {
        server.WriteThroughputBench(FLAGS_ops, FLAGS_block_size,
                                    FLAGS_max_batch_signal, FLAGS_max_post_list,
                                    FLAGS_random);
    }

    getchar();
    return 0;
}
