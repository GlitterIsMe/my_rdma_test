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
DEFINE_int32(num_threads, 1, "The number of threads for benchmarks, also the numbers of qps");

// Parameters for Server
DEFINE_bool(use_pmem, false, "Use pmem");
DEFINE_string(pmem_path, "/mnt/pmem/rdma", "The path to the pmem");
DEFINE_uint64(pmem_size, 1024, "The size of the pmem file in MB");

// Parameters for Client
DEFINE_uint64(ops, 10000, "Ops");
DEFINE_uint64(block_size, 256, "Block size");
DEFINE_uint64(max_batch_signal, 16, "The number of WRs for one signal");
DEFINE_uint64(max_post_list, 16, "The max number of WRs for each post send");
DEFINE_bool(random, false, "Access pattern");
DEFINE_bool(persist, true, "Whether persist the RDMA write");

const int kMaxThreadsNum = 33;

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    char* buf;
    if (FLAGS_is_server) {
        // For server
        if (FLAGS_use_pmem) {
            // Using PM, mmap the pmem, the mapped size is stored in rdma::mapped_size;
            rdma::pmem_size = FLAGS_pmem_size * 1024 * 1024UL;
            buf = rdma::map_pmem_file(FLAGS_pmem_path.c_str());
        } else {
            // Using DRAM, new buffer;
            buf = new char[FLAGS_pmem_size * 1024 * 1024];
        }
    } else {
        // For client, new a 1GB buffer, and set the size of remote buffer
        rdma::pmem_size = 64UL * 1024 * 1024 * 1024;
        buf = new char[rdma::CLIENT_BUF_SIZE];
    }

    // Init RDMA resource
    rdma::RDMA_Context ctx;
    int qp_num = FLAGS_num_threads;
    if (FLAGS_num_threads == 0) {
        qp_num = kMaxThreadsNum;
    }
    if (FLAGS_use_pmem) {
        std::cout << "use PMEM\n";
        size_t buf_size = FLAGS_is_server ? rdma::mapped_size : rdma::CLIENT_BUF_SIZE;
        rdma::InitRDMAContext(&ctx, qp_num, buf, buf_size);
    } else {
        std::cout << "use DRAM\n";
        size_t buf_size = FLAGS_is_server ? FLAGS_pmem_size * 1024 * 1024 : rdma::CLIENT_BUF_SIZE;
        rdma::InitRDMAContext(&ctx, qp_num, buf, buf_size);
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
        if (FLAGS_block_size == 0) {
            // iterator on block size
            if (FLAGS_num_threads == 0) {
                fprintf(stderr, "Not support iterating the block size and thread num simultaneously\n");
            } else {
                for (int blk = 8; blk < 1024 * 16; blk *= 2) {
                    std::cout << "\n[Running benchmark] Block size: " << blk << "\n";
                    std::vector<std::thread> thrds;
                    auto start = std::chrono::high_resolution_clock::now();
                    for (int i = 0; i < FLAGS_num_threads; ++i) {
                        int qp_idx = i;
                        thrds.emplace_back(std::thread{rdma::Server::WriteThroughputBench, &ctx, FLAGS_num_threads, qp_idx,
                                                       FLAGS_ops / FLAGS_num_threads, blk,
                                                       FLAGS_max_batch_signal,FLAGS_max_post_list,
                                                       FLAGS_random, FLAGS_persist});
                    }
                    for (int i = 0; i < FLAGS_num_threads; ++i) {
                        thrds[i].join();
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                    std::cout << "[Finish] Block size: " << blk << " bytes\n";
                    std::cout << "[Finish] Run " << FLAGS_ops << " ops, time: " << us << " us\n";
                    std::cout << "[Finish] IOPS: " << (double)FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
                    std::cout << "[Finish] Throughput: " << (FLAGS_ops * blk / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
                }
            }
        } else {
            if (FLAGS_num_threads == 0) {
                // Iterator on thread numbers
                for (int thrd_n = 1; thrd_n < kMaxThreadsNum; thrd_n *= 2) {
                    std::cout << "\n[Running benchmark] Thread: " << thrd_n << "\n";
                    std::vector<std::thread> thrds;
                    auto start = std::chrono::high_resolution_clock::now();
                    for (int i = 0; i < thrd_n; ++i) {
                        int qp_idx = i;
                        thrds.emplace_back(std::thread{rdma::Server::WriteThroughputBench, &ctx, thrd_n, qp_idx,
                                                       FLAGS_ops / thrd_n, FLAGS_block_size,
                                                       FLAGS_max_batch_signal,FLAGS_max_post_list,
                                                       FLAGS_random, FLAGS_persist});
                    }
                    for (int i = 0; i < thrd_n; ++i) {
                        thrds[i].join();
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                    std::cout << "[Finish] Block size: " << FLAGS_block_size << " bytes\n";
                    std::cout << "[Finish] Run " << FLAGS_ops << " ops, time: " << us << " us\n";
                    std::cout << "[Finish] IOPS: " << (double)FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
                    std::cout << "[Finish] Throughput: " << (FLAGS_ops * FLAGS_block_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
                }
            } else {
                // use exactly thread number and block size
                std::vector<std::thread> thrds;
                std::cout << "\n[Running benchmark] Thread: " << FLAGS_num_threads << ", Block size: " << FLAGS_block_size << "B\n";
                auto start = std::chrono::high_resolution_clock::now();
                for (int i = 0; i < FLAGS_num_threads; ++i) {
                    int qp_idx = i;
                    thrds.emplace_back(std::thread{rdma::Server::WriteThroughputBench, &ctx, FLAGS_num_threads, qp_idx,
                                                   FLAGS_ops / FLAGS_num_threads, FLAGS_block_size,
                                                   FLAGS_max_batch_signal,FLAGS_max_post_list,
                                                   FLAGS_random, FLAGS_persist});
                }
                for (int i = 0; i < FLAGS_num_threads; ++i) {
                    thrds[i].join();
                }
                auto end = std::chrono::high_resolution_clock::now();
                uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                std::cout << "[Finish] Block size: " << FLAGS_block_size << " bytes\n";
                std::cout << "[Finish] Run " << FLAGS_ops << " ops, time: " << us << " us\n";
                std::cout << "[Finish] IOPS: " << (double)FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
                std::cout << "[Finish] Throughput: " << (FLAGS_ops * FLAGS_block_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
            }
        }
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
        delete[] buf;
    }

    return 0;
}
