#include <iostream>

#include <gflags/gflags.h>
#include <thread>
#include <chrono>

#include "rdma_server.h"
#include "sock.h"
#include "pmem.h"

// Common parameters
DEFINE_string(addr, "192.168.200.101", "The address of the target");
DEFINE_bool(is_server, true, "Is this instance a server or a client");
DEFINE_int32(num_qp, 1, "The number of qps");

// Parameters for Server
DEFINE_bool(use_pmem, false, "Use pmem");
DEFINE_string(pmem_path, "/mnt/pmem/rdma", "The path to the pmem");
DEFINE_uint64(pmem_size, 1024, "The size of the pmem file in MB");

// Parameters for Client
DEFINE_int32(num_threads, 1, "The number of threads for benchmarks");
DEFINE_uint64(ops, 10000, "Ops");
DEFINE_uint64(block_size, 256, "Block size");
DEFINE_uint64(max_batch_signal, 16, "The number of WRs for one signal");
DEFINE_uint64(max_post_list, 16, "The max number of WRs for each post send");
DEFINE_bool(random, false, "Access pattern");

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    char* buf;
    if (FLAGS_is_server) {
        if (FLAGS_use_pmem) {
            rdma::pmem_size = FLAGS_pmem_size * 1024 * 1024UL;
            buf = rdma::map_pmem_file(FLAGS_pmem_path.c_str());
        } else {
            buf = new char[FLAGS_pmem_size * 1024 * 1024];
        }
    } else {
        rdma::pmem_size = 64UL * 1024 * 1024 * 1024;
        buf = new char[1UL * 1024 * 1024 * 1024];
    }

    rdma::RDMA_Context ctx;
    if (FLAGS_use_pmem) {
        std::cout << "use PMEM\n";
        size_t buf_size = FLAGS_is_server ? rdma::mapped_size : 1UL * 1024 * 1024 * 1024;
        rdma::InitRDMAContext(&ctx, FLAGS_num_qp, buf, buf_size);
    } else {
        std::cout << "use DRAM\n";
        size_t buf_size = FLAGS_is_server ? FLAGS_pmem_size * 1024 * 1024 : 1UL * 1024 * 1024 * 1024;
        rdma::InitRDMAContext(&ctx, FLAGS_num_qp, buf, buf_size);
    }

    rdma::Server server(&ctx, FLAGS_is_server);
    sock::connect_sock(FLAGS_is_server, FLAGS_addr, 34602);
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
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> thrds;
        for (int i = 0; i < FLAGS_num_threads; ++i) {
            int qp_idx = i;
            thrds.emplace_back(std::thread{rdma::Server::WriteThroughputBench, &ctx, FLAGS_num_threads, qp_idx,
                                           FLAGS_ops / FLAGS_num_threads, FLAGS_block_size,
                                           FLAGS_max_batch_signal,FLAGS_max_post_list,
                                           FLAGS_random, true});
        }
        for (int i = 0; i < FLAGS_num_threads; ++i) {
            thrds[i].join();
        }
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "Run " << FLAGS_ops << " ops, time: " << us << " us\n";
        std::cout << "IOPS: " << (double)FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
        std::cout << "Throughput: " << (FLAGS_ops * FLAGS_block_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
    }

    char tmp;
    char sync = 's';
    sock::exchange_message(FLAGS_is_server, &sync, 1, &tmp, 1);
    sock::disconnect_sock(FLAGS_is_server);

    if (FLAGS_is_server) {
        if (FLAGS_use_pmem) {
            rdma::unmap_file(buf);
        } else {
            delete[] buf;
        }
    } else {
        buf = new char[1UL * 1024 * 1024];
        delete[] buf;
    }

    return 0;
}
