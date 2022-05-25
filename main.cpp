#include <iostream>

#include <gflags/gflags.h>
#include <thread>
#include <chrono>

#include "rdma_server.h"
#include "sock.h"
#include "pmem.h"

// Common parameters
DEFINE_string(addr, "192.168.1.88", "The address of the target");
DEFINE_bool(is_server, true, "Is this instance a server or a client");
DEFINE_int32(num_threads, 1, "The number of threads for benchmarks, also the numbers of qps");

// Parameters for Server
DEFINE_bool(use_pmem, true, "Use pmem");
DEFINE_string(pmem_path, "/dev/dax0.3", "The path to the pmem");
DEFINE_uint64(pmem_size, 32UL * 1024, "The size of the pmem file in MB");

// Parameters for Client
DEFINE_string(benchmark, "write", "the name of benchmarks, write or atomic");
DEFINE_uint64(ops, 10000, "Ops");
DEFINE_uint64(block_size, 256, "Block size");
DEFINE_uint64(max_batch_signal, 16, "The number of WRs for one signal");
DEFINE_uint64(max_post_list, 16, "The max number of WRs for each post send");
DEFINE_bool(random, false, "Access pattern");
DEFINE_bool(persist, true, "Whether persist the RDMA write");

const int kMaxThreadsNum = 33;

void RunPwriteLatencyBench(rdma::Server* server, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Pwrite Latency Test\n";
    std::vector<std::thread> thrds;
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        /*thrds.emplace_back(std::thread{rdma::Server::PwriteLatency, server, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size});*/
        rdma::Server::LatencyBench(server, rdma::PwriteLat, threads_num, qp_idx, FLAGS_ops / threads_num, block_size);
    }
    std::cout << "Finishing RDMA Pwrite Latency Test\n";
}

void RunReadLatencyBench(rdma::Server* server, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Read Latency Test\n";
    std::vector<std::thread> thrds;
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        /*thrds.emplace_back(std::thread{rdma::Server::PwriteLatency, server, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size});*/
        rdma::Server::LatencyBench(server, rdma::ReadLat, threads_num, qp_idx, FLAGS_ops / threads_num, block_size);
    }
    std::cout << "Finishing RDMA Read Latency Test\n";
}

void RunWriteLatencyBench(rdma::Server* server, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Write Latency Test\n";
    std::vector<std::thread> thrds;
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        /*thrds.emplace_back(std::thread{rdma::Server::PwriteLatency, server, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size});*/
        rdma::Server::LatencyBench(server, rdma::WriteLat, threads_num, qp_idx, FLAGS_ops / threads_num, block_size);
    }
    std::cout << "Finishing RDMA Write Latency Test\n";
}

void RunNoopLatencyBench(rdma::Server* server, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Noop Latency Test\n";
    std::vector<std::thread> thrds;
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        /*thrds.emplace_back(std::thread{rdma::Server::PwriteLatency, server, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size});*/
        rdma::Server::LatencyBench(server, rdma::NoopLat, threads_num, qp_idx, FLAGS_ops / threads_num, block_size);
    }
    std::cout << "Finishing RDMA Noop Latency Test\n";
}

void RunCASLatencyBench(rdma::Server* server, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA CAS Latency Test\n";
    std::vector<std::thread> thrds;
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        /*thrds.emplace_back(std::thread{rdma::Server::PwriteLatency, server, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size});*/
        rdma::Server::LatencyBench(server, rdma::CASLat, threads_num, qp_idx, FLAGS_ops / threads_num, block_size);
    }
    std::cout << "Finishing RDMA CAS Latency Test\n";
}

void RunWrite(rdma::RDMA_Context *ctx, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Write\n";
    std::vector<std::thread> thrds;
    leveldb::Histogram hist;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        thrds.emplace_back(std::thread{rdma::Server::WriteThroughputBench, ctx, threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size,
                                       FLAGS_max_batch_signal, FLAGS_max_post_list,
                                       FLAGS_random, FLAGS_persist, &hist});
    }
    for (int i = 0; i < threads_num; ++i) {
        thrds[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    printf("%s\n", hist.ToString().c_str());
    std::cout << "[Finish] Block size: " << block_size << " bytes\n";
    std::cout << "[Finish] Run RDMA write" << FLAGS_ops << " ops, time: " << us << " us\n";
    std::cout << "[Finish] Write IOPS: " << (double) FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
    std::cout << "[Finish] Write Throughput: " << (FLAGS_ops * block_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
}

void RunCAS(rdma::RDMA_Context *ctx, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA CAS\n";
    std::vector<std::thread> thrds;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        thrds.emplace_back(std::thread{rdma::Server::CASThroughputBench, ctx, threads_num, qp_idx,
                                       FLAGS_ops / threads_num,
                                       FLAGS_max_batch_signal, FLAGS_max_post_list,
                                       FLAGS_random, FLAGS_persist});
    }
    for (int i = 0; i < threads_num; ++i) {
        thrds[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    //std::cout << "[Finish] Block size: " << block_size << " bytes\n";
    std::cout << "[Finish] Run RDMA CAS " << FLAGS_ops << " ops, time: " << us << " us\n";
    std::cout << "[Finish] CAS OPS: " << (double) FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
    //std::cout << "[Finish] CAS Throughput: " << (FLAGS_ops * block_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
}

void RunSend(rdma::RDMA_Context *ctx, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Send\n";
    std::vector<std::thread> thrds;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        thrds.emplace_back(std::thread{rdma::Server::EchoThroughputBench_Client, ctx,
                                       threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size, FLAGS_max_post_list});
    }
    for (int i = 0; i < threads_num; ++i) {
        thrds[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::cout << "[Finish] Run RDMA Send " << FLAGS_ops << " ops, time: " << us << " us\n";
    std::cout << "[Finish] Send OPS: " << (double) FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
}

void RunRecv(rdma::RDMA_Context *ctx, uint64_t block_size, uint64_t threads_num) {
    std::cout << "Running RDMA Recv\n";
    std::vector<std::thread> thrds;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < threads_num; ++i) {
        int qp_idx = i;
        thrds.emplace_back(std::thread{rdma::Server::EchoThroughputBench_Server, ctx,
                                       threads_num, qp_idx,
                                       FLAGS_ops / threads_num, block_size, FLAGS_max_post_list});
    }
    for (int i = 0; i < threads_num; ++i) {
        thrds[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::cout << "[Finish] Run RDMA Recv " << FLAGS_ops << " ops, time: " << us << " us\n";
    std::cout << "[Finish] Recv OPS: " << (double) FLAGS_ops / us * 1000000 / 1000 << "KOPS\n";
}

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    char *buf;
    if (FLAGS_is_server) {
        // For server
        if (FLAGS_use_pmem) {
            // Using PM, mmap the pmem, the mapped size is stored in rdma::mapped_size;
            rdma::pmem_size = 0;
            buf = rdma::map_pmem_file(FLAGS_pmem_path.c_str());
            rdma::pmem_size = FLAGS_pmem_size * 1024 * 1024;
            if (FLAGS_benchmark == "cas" | FLAGS_benchmark == "cas_latency") {
                rdma::zero_mapped_file(buf);
            }
        } else {
            // Using DRAM, new buffer;
            buf = new char[FLAGS_pmem_size * 1024 * 1024];
            if (FLAGS_benchmark == "cas" | FLAGS_benchmark == "cas_latency") {
                memset(buf, 0, FLAGS_pmem_size * 1024 * 1024);
            }
            memset(buf, '1', FLAGS_pmem_size * 1024 * 1024);
        }
    } else {
        // For client, new a 1GB buffer, and set the size of remote buffer
        // Requiring modifying manually;
        rdma::pmem_size = FLAGS_pmem_size * 1024 * 1024;;
        buf = new char[rdma::CLIENT_BUF_SIZE];
        memset(buf, 0, rdma::CLIENT_BUF_SIZE);
    }

    // Init RDMA resource
    rdma::RDMA_Context ctx;
    int qp_num = FLAGS_num_threads;
    if (FLAGS_num_threads == 0) {
        qp_num = kMaxThreadsNum;
    }
    if (FLAGS_use_pmem) {
        std::cout << "use PMEM\n";
        size_t buf_size = FLAGS_is_server ? rdma::pmem_size : rdma::CLIENT_BUF_SIZE;
        rdma::InitRDMAContext(&ctx, qp_num, buf, buf_size);
    } else {
        std::cout << "use DRAM\n";
        size_t buf_size = FLAGS_is_server ? FLAGS_pmem_size * 1024 * 1024 : rdma::CLIENT_BUF_SIZE;
        rdma::InitRDMAContext(&ctx, qp_num, buf, buf_size);
    }

    rdma::Server server(&ctx, FLAGS_is_server);
    sock::connect_sock(FLAGS_is_server, FLAGS_addr, 35700);
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

    void (*bench_func)(rdma::Server *, uint64_t, uint64_t);
    if (FLAGS_benchmark == "write") {
        //bench_func = &RunWrite;
    } else if (FLAGS_benchmark == "cas") {
        //bench_func = &RunCAS;
    } else if (FLAGS_benchmark == "echo") {
        if (FLAGS_is_server) {
            //bench_func = &RunRecv;
        } else {
            //bench_func = &RunSend;
        }
    } else if (FLAGS_benchmark == "pwrite_latency") {
        bench_func = &RunPwriteLatencyBench;
    } else if (FLAGS_benchmark == "read_latency") {
        bench_func = &RunReadLatencyBench;
    } else if (FLAGS_benchmark == "write_latency") {
        bench_func = &RunWriteLatencyBench;
    } else if (FLAGS_benchmark == "cas_latency") {
        bench_func = &RunCASLatencyBench;
    } else if (FLAGS_benchmark == "noop_latency") {
        bench_func = &RunNoopLatencyBench;
    }

    if (!FLAGS_is_server) {
        if (FLAGS_block_size == 0) {
            // iterator on block size
            if (FLAGS_num_threads == 0) {
                fprintf(stderr, "Not support iterating the block size and thread num simultaneously\n");
            } else {
                for (int blk = 8; blk < 1024 * 16; blk *= 2) {
                    std::cout << "\n[Running benchmark] Block size: " << blk << " B\n";
                    bench_func(&server, blk, FLAGS_num_threads);
                }
            }
        } else {
            if (FLAGS_num_threads == 0) {
                // Iterator on thread numbers
                for (int thrd_n = 1; thrd_n < kMaxThreadsNum; thrd_n *= 2) {
                    std::cout << "\n[Running benchmark] Threads num: " << thrd_n << "\n";
                    bench_func(&server, FLAGS_block_size, thrd_n);
                }
            } else {
                // use exactly thread number and block size
                //printf("use exactly thread number and block size\n");
                std::vector<std::thread> thrds;
                std::cout << "\n[Running benchmark] Thread: " << FLAGS_num_threads << ", Block size: "
                          << FLAGS_block_size << "B\n";
                bench_func(&server, FLAGS_block_size, FLAGS_num_threads);
            }
        }
    } else {
        if (FLAGS_benchmark == "echo") {
            //printf("server run recv\n");
            server.PrePostRQ(FLAGS_block_size, FLAGS_max_post_list);
            char tmp;
            char sync = 'c';
            sock::exchange_message(FLAGS_is_server, &sync, 1, &tmp, 1);
            printf("[Sync Point] Server before run benchmark [%c]\n", tmp);

            bench_func(&server, FLAGS_block_size, FLAGS_num_threads);

            /*char tmp;
            char sync = 'c';
            sock::exchange_message(FLAGS_is_server, &sync, 1, &tmp, 1);
            sock::disconnect_sock(FLAGS_is_server);
            printf("send sync signal[%c]\n", sync);*/

        }
    }

    char tmp;
    char sync = 't';
    sock::exchange_message(FLAGS_is_server, &sync, 1, &tmp, 1);
    sock::disconnect_sock(FLAGS_is_server);
    printf("[Sync Point] Disconnect [%c]\n", tmp);

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
