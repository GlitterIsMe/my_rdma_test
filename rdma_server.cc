//
// Created by YiwenZhang on 2022/3/14.
//

#include<infiniband/verbs.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>
#include <unistd.h>

#include "rdma_server.h"
#include "histogram.h"
#include "sock.h"
#include "util.h"
#include "pmem.h"

#define likely(x) __builtin_expect(!!(x), 1)

#define PERSIST_EACH_WRITE

namespace rdma {

    void InitRDMAContext(rdma::RDMA_Context* ctx, int num_qp, char* buf, size_t buf_size) {
        int dev_num;
        struct ibv_device** dev_list = ibv_get_device_list(&dev_num);
        std::cout << "Find " << dev_num << " RDMA devices" << std::endl;

        int dev = 0;
        ctx->ib_ctx = ibv_open_device(dev_list[dev]);
        std::cout << "Use RDMA device [" << dev << "]\n";


            struct ibv_device_attr_ex ex_attr;
            ibv_query_device_ex(ctx->ib_ctx, nullptr, &ex_attr);
            printf("atomic: %lu\n", ex_attr.pci_atomic_caps.compare_swap);


        ibv_free_device_list(dev_list);

        ctx->pd = ibv_alloc_pd(ctx->ib_ctx);

        ibv_query_port(ctx->ib_ctx, 1, &ctx->port_attr);

        ctx->buf = buf;
        //memset(ctx->buf, 0, buf_size);
        std::cout << "Register memory region [" << buf_size / 1024.0 / 1024 / 1024 << "] GB\n";
        ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, buf_size, IBV_ACCESS_LOCAL_WRITE |
                                                      IBV_ACCESS_REMOTE_READ |
                                                      IBV_ACCESS_REMOTE_WRITE |
                                                      IBV_ACCESS_REMOTE_ATOMIC);
        if (ctx->mr == nullptr) {
            fprintf(stderr, "Register memory failed [%s]\n", strerror(errno));
        } else {
            printf("register memory success\n");
        }

        struct ibv_device_attr dev_attr;
        ibv_query_device(ctx->ib_ctx, &dev_attr);

        //struct ibv_srq_init_attr srq_init_attr;
        ctx->num_qp = num_qp;
        ctx->cqs = new struct ibv_cq*[num_qp];
        ctx->qps = new struct ibv_qp*[num_qp];

        for (int i = 0; i < num_qp; ++i) {
            ctx->cqs[i] = ibv_create_cq(ctx->ib_ctx, 512,
                                        nullptr, nullptr,
                                        0);
            struct ibv_qp_init_attr qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(ibv_qp_init_attr));
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.send_cq = ctx->cqs[i];
            qp_init_attr.recv_cq = ctx->cqs[i];
            qp_init_attr.cap.max_send_wr = 1024;
            qp_init_attr.cap.max_recv_wr = 1024;
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.sq_sig_all = 0;
            ctx->qps[i] = ibv_create_qp(ctx->pd, &qp_init_attr);
            //connect_qp(cqs[i], qps[i]);
        }
        ctx->remote_conn = new rdma::con_data_t[num_qp];
    }

    void Server::InitConnection() {
        /*int dev_num;
        struct ibv_device** dev_list = ibv_get_device_list(&dev_num);
        std::cout << dev_num << std::endl;

        ib_ctx = ibv_open_device(dev_list[1]);

        ibv_free_device_list(dev_list);

        pd = ibv_alloc_pd(ib_ctx);

        ibv_query_port(ib_ctx, 1, &port_attr);

        buf_ = new char[1024];
        memset(buf_, 0, 1024);

        mr = ibv_reg_mr(pd, buf_, 1024, IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC);

        struct ibv_device_attr dev_attr;
        ibv_query_device(ib_ctx, &dev_attr);

        //struct ibv_srq_init_attr srq_init_attr;
        cqs = new struct ibv_cq*[num_qp_];
        qps = new struct ibv_qp*[num_qp_];*/

        for (int i = 0; i < ctx_->num_qp; ++i) {
            connect_qp(ctx_->cqs[i], ctx_->qps[i], &ctx_->remote_conn[i]);
        }
    }

    void Server::connect_qp(struct ibv_cq* cq, struct ibv_qp* qp, con_data_t* remote_conn) {
        union ibv_gid lgid;
        ibv_query_gid(ctx_->ib_ctx, 1, 1, &lgid);

        con_data_t local_con;
        local_con.addr = (uintptr_t)ctx_->buf;
        local_con.rkey = ctx_->mr->rkey;
        local_con.qp_num = qp->qp_num;
        local_con.lid = ctx_->port_attr.lid;
        memcpy(local_con.gid, (char*)&lgid, 16);

        std::cout << "Exchange QP info and connect\n";
        sock::exchange_message(is_server_, (char*)&local_con, sizeof(con_data_t), (char*)remote_conn, sizeof(con_data_t));

#ifdef DEBUG
        std::cout << "local qpn:" << local_con.qp_num << "\n"
                  << "lkey: " << local_con.rkey << "\n"
                  << "local addr: " << local_con.addr << "\n"
                  << "local lid: " << local_con.lid << "\n";

        std::cout << "remote qpn:" << remote_conn->qp_num << "\n"
                  << "rkey: " << remote_conn->rkey << "\n"
                  << "remote addr: " << remote_conn->addr << "\n"
                  << "remote lid: " << remote_conn->lid << "\n";
#endif
        modify_qp_to_init(qp);
        modify_qp_to_rtr(qp, remote_conn->qp_num, remote_conn->lid, remote_conn->gid);
        modify_qp_to_rts(qp);

        // sync
        char tmp;
        char sync = 'I';
        sock::exchange_message(is_server_, &sync, 1, &tmp, 1);
        printf("[Sync Point] Exchange QP information [%c]\n", tmp);
    }

    void Server::modify_qp_to_init(struct ibv_qp* qp) {
        struct ibv_qp_attr attr;
        int flags;
        memset(&attr, 0, sizeof(ibv_qp_attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = 1;
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

        flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
        int res = ibv_modify_qp(qp, &attr, flags);
        printf("Transfer QP to init[%d]\n", res);
    }

    void Server::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* gid) {
        struct ibv_qp_attr attr;
        int flags;
        memset(&attr, 0, sizeof(ibv_qp_attr));

        attr.qp_state = IBV_QPS_RTR; // IBV_QP_STATE
        attr.path_mtu = IBV_MTU_256; // IBV_QP_PATH_MTU
        attr.dest_qp_num = remote_qpn; // IBV_QP_DEST_QPN
        attr.rq_psn = 0; // IBV_QP_RQ_PSN
        attr.max_dest_rd_atomic = 16; // IBV_QP_MAX_DEST_RD_ATOMIC
        attr.min_rnr_timer = 31; // IBV_QP_MIN_RNR_TIMER

        attr.ah_attr.is_global = 1;
        attr.ah_attr.dlid = dlid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = 1;
        attr.ah_attr.grh.traffic_class = 0;

        flags = IBV_QP_STATE | IBV_QP_AV |
                IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;

        int res = ibv_modify_qp(qp, &attr, flags);
        printf("Transfer QP to rtr[%d]\n", res);
    }

    void Server::modify_qp_to_rts(struct ibv_qp* qp) {
        struct ibv_qp_attr attr;
        int flags;
        memset(&attr, 0, sizeof(ibv_qp_attr));

        attr.qp_state = IBV_QPS_RTS;
        attr.timeout = 0x30;
        attr.retry_cnt = 6;
        attr.rnr_retry = 6;
        attr.sq_psn = 0;
        attr.max_rd_atomic = 16;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT |
                IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

        int res = ibv_modify_qp(qp, &attr, flags);
        printf("Transfer QP to rts[%d]\n", res);
    }

    void Server::Write(int qp_idx, char* src, size_t src_len, char* dest) {
        struct ibv_send_wr write_wr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t) src;
        sge.length = src_len;
        sge.lkey = ctx_->mr->lkey;

        write_wr.wr_id = 0;
        write_wr.sg_list = &sge;
        write_wr.num_sge = 1;
        write_wr.opcode = IBV_WR_RDMA_WRITE;
        write_wr.send_flags = IBV_SEND_SIGNALED;

        write_wr.wr.rdma.remote_addr = (uintptr_t)dest;
        write_wr.wr.rdma.rkey = ctx_->remote_conn[qp_idx].rkey;

        write_wr.next = nullptr;

        int ret = ibv_post_send(ctx_->qps[qp_idx], &write_wr, &bad);
        if (ret) {
            fprintf(stdout, "post send failed [%d] [%s]\n", ret, strerror(errno));
            fprintf(stdout, "wr id [%d]\n", bad->wr_id);
            fprintf(stdout, "sge offset: %lu\n", sge.addr);
            exit(-1);
        }
        poll_cq(ctx_->cqs[qp_idx], 1);
    }

    void Server::Read(int qp_idx, char* src, size_t src_len, char* dest) {
        struct ibv_send_wr write_wr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t) src;
        sge.length = src_len;
        sge.lkey = ctx_->mr->lkey;

        write_wr.wr_id = 0;
        write_wr.sg_list = &sge;
        write_wr.num_sge = 1;
        write_wr.opcode = IBV_WR_RDMA_READ;
        write_wr.send_flags = IBV_SEND_SIGNALED;

        write_wr.wr.rdma.remote_addr = (uintptr_t)dest;
        write_wr.wr.rdma.rkey = ctx_->remote_conn[qp_idx].rkey;

        write_wr.next = nullptr;

        int ret = ibv_post_send(ctx_->qps[qp_idx], &write_wr, &bad);
        if (ret) {
            fprintf(stdout, "post send failed [%d] [%s]\n", ret, strerror(errno));
            fprintf(stdout, "wr id [%d]\n", bad->wr_id);
            fprintf(stdout, "sge offset: %lu\n", sge.addr);
            exit(-1);
        }
        poll_cq(ctx_->cqs[qp_idx], 1);
    }

    void Server::Pwrite(int qp_idx, char *src, size_t src_len, char *dest) {
        struct ibv_send_wr write_wr, read_wr;
        struct ibv_sge sge, read_sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t) src;
        sge.length = src_len;
        sge.lkey = ctx_->mr->lkey;

        read_sge.addr = (uintptr_t) src;
        read_sge.length = 0;
        read_sge.lkey = ctx_->mr->lkey;

        write_wr.wr_id = 0;
        write_wr.sg_list = &sge;
        write_wr.num_sge = 1;
        write_wr.opcode = IBV_WR_RDMA_WRITE;
        //write_wr.send_flags = IBV_SEND_SIGNALED;

        write_wr.wr.rdma.remote_addr = (uintptr_t)dest;
        write_wr.wr.rdma.rkey = ctx_->remote_conn[qp_idx].rkey;

        read_wr.opcode = IBV_WR_RDMA_READ;
        read_wr.wr_id = 1;
        read_wr.num_sge = 1;
        read_wr.sg_list = &read_sge;
        read_wr.next = nullptr;
        read_wr.send_flags = IBV_SEND_SIGNALED;
        read_wr.wr.rdma.remote_addr = (uintptr_t)dest;
        read_wr.wr.rdma.rkey = ctx_->remote_conn[qp_idx].rkey;

        write_wr.next = &read_wr;
        //write_wr.next = nullptr;

        int ret = ibv_post_send(ctx_->qps[qp_idx], &write_wr, &bad);
        if (ret) {
            fprintf(stdout, "post send failed [%d] [%s]\n", ret, strerror(errno));
            fprintf(stdout, "wr id [%d]\n", bad->wr_id);
            fprintf(stdout, "sge offset: %lu\n", sge.addr);
            exit(-1);
        }
        poll_cq(ctx_->cqs[qp_idx], 1);
    }

    bool Server::CAS(int qp_idx, char *src, char *dest, uint64_t old_v, uint64_t new_v) {
        struct ibv_send_wr write_wr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t) src;
        sge.length = 8;
        sge.lkey = ctx_->mr->lkey;

        write_wr.wr_id = 0;
        write_wr.sg_list = &sge;
        write_wr.num_sge = 1;
        write_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        write_wr.send_flags = IBV_SEND_SIGNALED;

        write_wr.wr.atomic.remote_addr = (uintptr_t)dest;
        write_wr.wr.atomic.compare_add = old_v;
        write_wr.wr.atomic.swap = new_v;
        write_wr.wr.atomic.rkey = ctx_->remote_conn[qp_idx].rkey;

        write_wr.next = nullptr;

        int ret = ibv_post_send(ctx_->qps[qp_idx], &write_wr, &bad);
        if (ret) {
            fprintf(stdout, "post send failed [%d] [%s]\n", ret, strerror(errno));
            fprintf(stdout, "wr id [%d]\n", bad->wr_id);
            fprintf(stdout, "sge offset: %lu\n", sge.addr);
            exit(-1);
        }
        poll_cq(ctx_->cqs[qp_idx], 1);
        uint64_t swapped = *(uint64_t*)sge.addr;
        //printf("old %llu new %llu swapped %llu\n", old_v, new_v, swapped);
        return swapped == old_v;
    }

    void Server::LatencyBench(Server* server, BenchType type, int threads, int qp_idx, size_t total_ops, size_t blk_size) {
        size_t local_pm_region_size = pmem_size / threads;
        size_t local_dram_region_size = CLIENT_BUF_SIZE / threads;
        char* pm_region_start = (char*)server->ctx_->remote_conn[qp_idx].addr + qp_idx * local_pm_region_size;
        size_t dram_region_start = qp_idx * local_dram_region_size;
        size_t local_pm_block_nums = local_pm_region_size / blk_size;

        uint64_t seed = 0xdeadbeef;

        switch (type) {
            case ReadLat: {
                for (int i = 0; i < total_ops; ++i) {
                    size_t blk_no = hrd_fastrand(&seed) % (local_pm_block_nums - 1);
                    printf("cur: %lu, total: %lu\n", blk_no, local_pm_block_nums);
                    auto start = std::chrono::high_resolution_clock::now();
                    server->Read(qp_idx, server->ctx_->buf + dram_region_start, blk_size,
                                   pm_region_start + blk_no * blk_size);
                    auto end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double, std::micro> elapse = end - start;
                    /*if (i % 1000 == 0) {
                        fprintf(stdout, "finished %ld\r", i);
                        fflush(stdout);
                    }*/
                    server->histogram.Add(elapse.count());
                }
                break;
            }

            case WriteLat: {
                for (int i = 0; i < total_ops; ++i) {
                    size_t blk_no = hrd_fastrand(&seed) % (local_pm_block_nums - 1);
                    printf("cur: %lu, total: %lu\n", blk_no, local_pm_block_nums);
                    auto start = std::chrono::high_resolution_clock::now();
                    server->Write(qp_idx, server->ctx_->buf + dram_region_start, blk_size,
                                   pm_region_start + blk_no * blk_size);
                    auto end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double, std::micro> elapse = end - start;
                    /*if (i % 1000 == 0) {
                        fprintf(stdout, "finished %ld\r", i);
                        fflush(stdout);
                    }*/
                    server->histogram.Add(elapse.count());
                }
                break;
            }

            case PwriteLat: {
                for (int i = 0; i < total_ops; ++i) {
                    size_t blk_no = hrd_fastrand(&seed) % (local_pm_block_nums - 1);
                    printf("cur: %lu, total: %lu\n", blk_no, local_pm_block_nums);
                    auto start = std::chrono::high_resolution_clock::now();
                    server->Pwrite(qp_idx, server->ctx_->buf + dram_region_start, blk_size,
                                   pm_region_start + blk_no * blk_size);
                    auto end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double, std::micro> elapse = end - start;
                    /*if (i % 1000 == 0) {
                        fprintf(stdout, "finished %ld\r", i);
                        fflush(stdout);
                    }*/
                    server->histogram.Add(elapse.count());
                }
                break;
            }

            case CASLat: {
                for (int i = 0; i < total_ops; ++i) {
                    size_t blk_no = hrd_fastrand(&seed) % (local_pm_block_nums - 1);
                    //printf("cur: %lu, total: %lu\n", blk_no, local_pm_block_nums);
                    auto start = std::chrono::high_resolution_clock::now();
                    server->CAS(qp_idx, server->ctx_->buf + dram_region_start,
                                   pm_region_start + blk_no * blk_size, 0, i);
                    auto end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double, std::micro> elapse = end - start;
                    /*if (i % 1000 == 0) {
                        fprintf(stdout, "finished %ld\r", i);
                        fflush(stdout);
                    }*/
                    server->histogram.Add(elapse.count());
                }
                break;
            }
        }


        printf("%s\n", server->histogram.ToString().c_str());
    }

    void Server::poll_cq(struct ibv_cq* cq, int num_comps) {
#ifdef DEBUG
        fprintf(stdout, "target poll %d comps\n", num_comps);
#endif
        //struct ibv_wc wc[num_comps];
        struct ibv_wc wc;
        int res = 0, total_comp = 0;
        while (total_comp < num_comps) {
            res = ibv_poll_cq(cq, 1, &wc);
            assert(res <= 1);
            if (res > 0) {
//#ifdef DEBUG
                //for (int i = 0; i < res; ++i) {
                    if (wc.status != IBV_WC_SUCCESS) {
                        fprintf(stdout, "failed request [%ld] [0x%x]\n", wc.wr_id, wc.status);
                        exit(-1);
                    } /*else {
                        //printf("wr id %d, op %d\n", wc.wr_id, wc.opcode);
                    }*/
                //}
//#endif
                total_comp += res;
            }
/*#ifdef DEBUG
            fprintf(stdout, "polled %d comps\n", total_comp);
#endif*/
        }
#ifdef DEBUG
        fprintf(stdout, "finished total poll %d comps\n", total_comp);
#endif
    }
    /*void Server::WriteThroughputBench(size_t total_ops, size_t blk_size, size_t max_bacth, size_t max_post, bool random) {
        std::cout << "Run RDMA Write Benchmark: \n"
                    << "Total ops: " << total_ops << "\n"
                    << "Block size: " << blk_size << "\n"
                    << "Max batch: " << max_bacth << "\n"
                    << "Max post list: " << max_post << "\n"
                    << "Random: " << random << "\n";
        ibv_send_wr wr[max_post], *bad;
        ibv_sge sge;
        size_t posted_wr;
        size_t offset = 0;
        size_t finished = 0;
        uint64_t seed = 0xdeadbeef;

        sge.lkey = mr->lkey;
        sge.addr = (uintptr_t)buf_;
        sge.length = blk_size;

        int qp_idx = 0;
        auto start = std::chrono::high_resolution_clock::now();
        for(; finished < total_ops; finished += max_post) {
            // post max_post WR to the QP;
            for(int i = 0; i < max_post; i++) {
                wr[i].opcode = IBV_WR_RDMA_WRITE;
                wr[i].wr_id = 0;
                wr[i].num_sge = 1;
                wr[i].sg_list = &sge;

                if (i < max_post - 1) {
                    wr[i].next = &wr[i+1];
                } else {
                    wr[i].next = nullptr;
                }

                if (posted_wr % max_bacth == 0 && posted_wr > 0) {
                    wr[i].send_flags = IBV_SEND_SIGNALED;
                    ibv_wc wc;
                    ibv_poll_cq(cqs[qp_idx], 1, &wc);
                }

                wr[i].wr.rdma.rkey = remote_con.rkey;
                if (random) {
                    size_t rd_off  = hrd_fastrand(&seed) % pmem_size;
                    if (pmem_size - rd_off < blk_size) rd_off -= blk_size;
                    wr[i].wr.rdma.remote_addr = remote_con.addr + rd_off;
                } else {
                    wr[i].wr.rdma.remote_addr = remote_con.addr + offset;
                    offset += blk_size;
                }
            }
            ibv_post_send(qps[qp_idx], wr, &bad);
            qp_idx = (qp_idx + 1 ) % num_qp_;
        }

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "Run time: " << us << " us\n";
        std::cout << "IOPS: " << (double)total_ops / us * 1000000 / 1000 << "KOPS\n";
        std::cout << "Throughput: " << (total_ops * blk_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
    }*/

        /*
         * parameter:
         * - max_batch: signal frequency
         * - max_post: the number of posted wr for each post_send
         * */
    void Server::WriteThroughputBench(RDMA_Context* ctx, int threads, int qp_idx,
                                      size_t total_ops, size_t blk_size, size_t max_batch,
                                      size_t max_post, bool random, bool persist, leveldb::Histogram* hist) {
#ifdef DEBUG
        std::cout << "thread [" << std::this_thread::get_id() << "], idx [" << qp_idx << "] start process\n";
#endif
        ibv_send_wr wr[max_post], *bad = nullptr;
        ibv_sge sge[max_post];
#ifdef PERSIST_EACH_WRITE
        ibv_send_wr read_wr[max_post];
        ibv_sge read_sge[max_post];
#else
        ibv_send_wr read_wr;
        ibv_sge read_sge;
#endif
        size_t local_pm_size = pmem_size / threads;
        size_t local_dram_size = CLIENT_BUF_SIZE / threads;
        size_t pm_start = qp_idx * local_pm_size;
        size_t dram_start = qp_idx * local_dram_size;
#ifdef DEBUG
        std::cout << "PM start " << pm_start << "\n";
        std::cout << "DRAM start " << dram_start << "\n";
#endif
        size_t posted_wr = 0;
        size_t offset = 0;
        size_t finished = 0;
        uint64_t seed = 0xdeadbeef;

        size_t remote_block_num = 0;
        if (random) {
            remote_block_num = pmem_size / blk_size;
        }

        //auto start = std::chrono::high_resolution_clock::now();
        for(; finished < total_ops; finished += max_post) {
            auto start = std::chrono::high_resolution_clock::now();
            // post max_post WR to the QP;
            for(int i = 0; i < max_post; i++) {

                sge[i].lkey = ctx->mr->lkey;
                sge[i].addr = (uintptr_t)ctx->buf + dram_start;
                sge[i].length = blk_size;

                wr[i].opcode = IBV_WR_RDMA_WRITE;
                wr[i].wr_id = finished + i;
                wr[i].num_sge = 1;
                wr[i].sg_list = &sge[i];
#ifdef PERSIST_EACH_WRITE
                wr[i].next = &read_wr[i];
#else
                if (i < max_post - 1) {
                    wr[i].next = &wr[i+1];
                } else {
                    wr[i].next = nullptr;
                }
#endif

#ifndef PERSIST_EACH_WRITE
                if (posted_wr % max_batch == 0) {
                    wr[i].send_flags = IBV_SEND_SIGNALED;
                    if (likely(finished > 0)) {
                        poll_cq(ctx->cqs[qp_idx], 1);
                    }
                }
#endif

                wr[i].wr.rdma.rkey = ctx->remote_conn[qp_idx].rkey;
                if (random) {
                    // random write
                    /*
                     * 1. divide the total PM space into N blocks with granularity of block size;
                     * 2. randomly choose a block in [0, N]
                     * */
                    assert(remote_block_num != 0);
                    size_t rd_blk  = hrd_fastrand(&seed) % remote_block_num;
                    wr[i].wr.rdma.remote_addr = ctx->remote_conn[qp_idx].addr + rd_blk * blk_size;
/*#ifdef DEBUG
                    fprintf(stdout, "[Random Write] : [%d] write offset [%p] = (base)[%p] + (rd_block)[%p] * blk_size[%lu]\n",wr[i].wr_id,
                            ctx->remote_conn[qp_idx].addr + rd_blk * blk_size,
                            ctx->remote_conn[qp_idx].addr, rd_blk, blk_size);
#endif*/
                } else {
                    wr[i].wr.rdma.remote_addr = ctx->remote_conn[qp_idx].addr + pm_start + offset;
                    offset += blk_size;
#ifdef DEBUG
                    fprintf(stdout, "[Sequrntial Write] : [%d] write offset [%lu] = (base)[%p] + (start)[%lu] + offset[%lu]\n",wr[i].wr_id,
                            ctx->remote_conn[qp_idx].addr + pm_start + offset,
                            ctx->remote_conn[qp_idx].addr, pm_start, offset);
#endif
                }
/*#ifdef DEBUG
                fprintf(stdout, "wr[%d] id [%d], offset [%p], size [%d], next[%p]\n", i, wr[i].wr_id, wr[i].wr.rdma.remote_addr, blk_size, wr[i].next);
#endif*/

#ifdef PERSIST_EACH_WRITE
                if (persist) {
                    read_sge[i].lkey = ctx->mr->lkey;
                    read_sge[i].addr = (uintptr_t)ctx->buf + dram_start;
                    read_sge[i].length = 0;

                    read_wr[i].opcode = IBV_WR_RDMA_READ;
                    read_wr[i].wr_id = 1;
                    read_wr[i].num_sge = 1;
                    read_wr[i].sg_list = &read_sge[i];
                    if (i < max_post - 1) {
                        read_wr[i].next = &wr[i+1];
                    } else {
                        read_wr[i].next = nullptr;
                    }
                    //read_wr[i].next = nullptr;
                    read_wr[i].send_flags = IBV_SEND_SIGNALED;

                    /*if (likely(finished > 0)) {
                        poll_cq(ctx->cqs[qp_idx], max_post);
                    }*/

                    read_wr[i].wr.rdma.rkey = ctx->remote_conn[qp_idx].rkey;
                    read_wr[i].wr.rdma.remote_addr = wr[max_post - 1].wr.rdma.remote_addr;

                    wr[max_post - 1].next = &read_wr[i];
                }
#endif

            }
#ifndef PERSIST_EACH_WRITE
            if (persist) {
                read_sge.lkey = ctx->mr->lkey;
                read_sge.addr = (uintptr_t)ctx->buf + dram_start;
                read_sge.length = 0;

                read_wr.opcode = IBV_WR_RDMA_READ;
                read_wr.wr_id = 1;
                read_wr.num_sge = 1;
                read_wr.sg_list = &read_sge;
                read_wr.next = nullptr;
                read_wr.send_flags = IBV_SEND_SIGNALED;
                read_wr.wr.rdma.rkey = ctx->remote_conn[qp_idx].rkey;
                read_wr.wr.rdma.remote_addr = wr[max_post - 1].wr.rdma.remote_addr;

                wr[max_post - 1].next = &read_wr;
            }
#endif
#ifdef DEBUG
            int wr_num = 0;
            for (ibv_send_wr* first = &wr[0]; first != nullptr; first = first->next) {
                wr_num++;
            }
            assert(wr_num == max_post * 2);
#endif

            int ret = ibv_post_send(ctx->qps[qp_idx], wr, &bad);
#ifdef DEBUG
            if (ret) {
                fprintf(stdout, "post send[%ld] failed [%d] [%s]\n",finished, ret, strerror(errno));
                exit(-1);
            }
            if (bad != nullptr)  {
                fprintf(stdout, "bad wr [%ld] failed [%d] [%s]\n",finished, ret, strerror(errno));
            }
#endif
            posted_wr += max_post;

#ifndef PERSIST_EACH_WRITE
            if (persist) {
                poll_cq(ctx->cqs[qp_idx], 1);
            }
#endif

            poll_cq(ctx->cqs[qp_idx], max_post);
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::micro> elapse = end - start;
            hist->Add(elapse.count());

            if (finished % 10000 == 0) {
                fprintf(stdout, "finished %ld\r", finished);
                fflush(stdout);
            }
        }
        //std::cout << "thread " << qp_idx << " end process\n";
    }

    void Server::CASThroughputBench(RDMA_Context *ctx, int threads, int qp_idx, size_t total_ops, size_t max_batch,
                                    size_t max_post, bool random, bool persist) {
        int blk_size = 8;
        std::cout << "thread [" << std::this_thread::get_id() << "], idx [" << qp_idx << "] start process\n";
        ibv_send_wr wr[max_post], *bad = nullptr;
        ibv_send_wr read_wr;
        ibv_sge sge, read_sge;
        size_t local_pm_size = pmem_size / threads;
        size_t local_dram_size = CLIENT_BUF_SIZE / threads;
        size_t pm_start = qp_idx * local_pm_size;
        size_t dram_start = qp_idx * local_dram_size;
        std::cout << "PM start " << pm_start << "\n";
        std::cout << "DRAM start " << dram_start << "\n";
        size_t posted_wr = 0;
        size_t offset = 0;
        size_t finished = 0;
        uint64_t seed = 0xdeadbeef;

        size_t remote_block_num = 0;
        if (random) {
            remote_block_num = pmem_size / blk_size;
        }

        sge.lkey = ctx->mr->lkey;
        sge.addr = (uintptr_t)ctx->buf + dram_start;
        sge.length = blk_size;

        //auto start = std::chrono::high_resolution_clock::now();
        for(; finished < total_ops; finished += max_post) {
            // post max_post WR to the QP;
            for(int i = 0; i < max_post; i++) {
                wr[i].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
                wr[i].wr_id = qp_idx;
                wr[i].num_sge = 1;
                wr[i].sg_list = &sge;
                if (i < max_post - 1) {
                    wr[i].next = &wr[i+1];
                } else {
                    wr[i].next = nullptr;
                }
                /*if (!persist && posted_wr % max_batch == 0) {
                    wr[i].send_flags = IBV_SEND_SIGNALED;
                }*/
                wr[i].send_flags = IBV_SEND_SIGNALED;

                wr[i].wr.atomic.rkey = ctx->remote_conn[qp_idx].rkey;
                if (random) {
                    // random write
                    /*
                     * 1. divide the total PM space into N blocks with granularity of block size;
                     * 2. randomly choose a block in [0, N]
                     * */
                    assert(remote_block_num != 0);
                    size_t rd_blk  = hrd_fastrand(&seed) % remote_block_num;
                    wr[i].wr.atomic.remote_addr = ctx->remote_conn[qp_idx].addr + rd_blk * blk_size;
#ifdef DEBUG
                    fprintf(stdout, "[Random Write]: qp [%d] write offset [%lu] = (base)[%lu] + (rd_block)[%lu] * blk_size[%lu]\n",qp_idx,
                            ctx->remote_conn[qp_idx].addr + pm_start + blk_size,
                            ctx->remote_conn[qp_idx].addr, rd_blk, blk_size);
#endif
                } else {
                    wr[i].wr.atomic.remote_addr = ctx->remote_conn[qp_idx].addr + pm_start + offset;
                    offset += blk_size;
#ifdef DEBUG
                    fprintf(stdout, "[%d] write offset [%lu] = (base)[%lu] + (start)[%lu] + offset[%lu]\n",qp_idx,
                            ctx->remote_conn[qp_idx].addr + pm_start + offset,
                            ctx->remote_conn[qp_idx].addr, pm_start, offset);
#endif
                }
                wr[i].wr.atomic.compare_add = 0;
                wr[i].wr.atomic.swap = 0;
            }
            /*if (persist) {
                read_sge.lkey = ctx->mr->lkey;
                read_sge.addr = (uintptr_t)ctx->buf + dram_start;
                read_sge.length = 0;

                read_wr.opcode = IBV_WR_RDMA_READ;
                read_wr.wr_id = 1;
                read_wr.num_sge = 1;
                read_wr.sg_list = &read_sge;
                read_wr.next = nullptr;
                read_wr.send_flags = IBV_SEND_SIGNALED;
                read_wr.wr.rdma.rkey = ctx->remote_conn[qp_idx].rkey;
                read_wr.wr.rdma.remote_addr = ctx->remote_conn[qp_idx].addr;

                wr[max_post - 1].next = &read_wr;
            }*/
            int ret = ibv_post_send(ctx->qps[qp_idx], wr, &bad);
#ifdef DEBUG
            if (ret) {
                fprintf(stdout, "post send[%ld] failed [%d] [%s]\n",finished, ret, strerror(errno));
            }
            if (bad != nullptr)  {
                fprintf(stdout, "bad wr [%ld] failed [%d] [%s]\n",finished, ret, strerror(errno));
            }
#endif
            posted_wr += max_post;
            //if (posted_wr % max_batch == 0 && posted_wr > 0) {
            poll_cq(ctx->cqs[qp_idx], max_post);
            //}

            if (finished % 10000 == 0) {
                fprintf(stdout, "finished %ld\r", finished);
                fflush(stdout);
            }
        }
        //std::cout << "thread " << qp_idx << " end process\n";
    }

    // process of client: post recv - post send - poll completion for send - poll completion for recv;
    // RR store the buffer for incoming data;
    // send buffer can be reused;
    void Server::EchoThroughputBench_Client(RDMA_Context *ctx, int threads, int qp_idx,
                                            size_t total_ops, size_t blk_size, size_t max_post) {
        //struct ibv_sge recv_sge[max_post], send_sge[max_post];
        struct ibv_sge recv_sge, send_sge[max_post];
        struct ibv_send_wr wr[max_post], *bad_wr = nullptr;
        //struct ibv_recv_wr rr[max_post], *bad_rr = nullptr;

        int finished = 0, ret = 0;

        for (; finished < total_ops; ) {
            /*for (int i = 0; i < max_post; ++i) {
                recv_sge[i].length = blk_size;
                recv_sge[i].addr = (uintptr_t)ctx->buf + i * blk_size;
                recv_sge[i].lkey = ctx->mr->lkey;

                rr[i].sg_list = &recv_sge[i];
                rr[i].num_sge = 1;
                rr[i].wr_id = i + finished;
                rr[i].next = i < max_post - 1 ? &rr[i + 1] : nullptr;
            }

            int ret = ibv_post_recv(ctx->qps[qp_idx], rr, &bad_rr);

            if (ret) {
                fprintf(stderr, "client failed post recv [%s]\n", strerror(errno));
            }*/

            for (int i = 0; i < max_post; ++i) {
                // post recv
                struct ibv_recv_wr rr, *bad_rr = nullptr;
                recv_sge.length = blk_size;
                recv_sge.addr = (uintptr_t)ctx->buf + i * blk_size;
                recv_sge.lkey = ctx->mr->lkey;

                rr.sg_list = &recv_sge;
                rr.num_sge = 1;
                rr.wr_id = i + finished;
                rr.next = nullptr;

                int ret = ibv_post_recv(ctx->qps[qp_idx], &rr, &bad_rr);

                if (ret) {
                    fprintf(stderr, "client failed post recv [%s]\n", strerror(errno));
                }

                send_sge[i].length = blk_size;
                send_sge[i].addr = (uintptr_t)ctx->buf + i * blk_size;
                send_sge[i].lkey = ctx->mr->lkey;

                wr[i].sg_list = &send_sge[i];
                wr[i].num_sge = 1;
                wr[i].next = i < max_post - 1 ? &wr[i + 1] : nullptr;
                wr[i].opcode = IBV_WR_SEND;
                wr[i].wr_id = i + finished;
                if (i == max_post - 1) {
                    wr[i].send_flags = IBV_SEND_SIGNALED;
                }
            }

            ret = ibv_post_send(ctx->qps[qp_idx], wr, &bad_wr);
            if (ret) {
                fprintf(stderr, "client failed post send [%s]\n", strerror(errno));
            }

            // poll completion for send
            poll_cq(ctx->cqs[qp_idx], 1);
#ifdef DEBUG
            printf("client poll send comp\n");
#endif
            // poll completion for recv
            poll_cq(ctx->cqs[qp_idx], max_post);
#ifdef DEBUG
            printf("client poll recv comp\n");
#endif

#ifdef DEBUG
            printf("finished [%d]\n", finished);
#endif

            finished += max_post;

            if (finished % 10000 == 0) {
                fprintf(stdout, "finished %ld\r", finished);
                fflush(stdout);
            }
        }
    }

    void
    Server::EchoThroughputBench_Server(RDMA_Context *ctx, int threads, int qp_idx, size_t total_ops,
                                       size_t blk_size, size_t max_post) {
        struct ibv_sge recv_sge[max_post], send_sge;
        struct ibv_send_wr wr, *bad_wr = nullptr;
        struct ibv_recv_wr rr[max_post], *bad_rr = nullptr;

        int finished = 0;

        int ret = 0;

        /*for (int i = 0; i < max_post; ++i) {
            recv_sge[i].length = blk_size;
            recv_sge[i].addr = (uintptr_t)ctx->buf + i * blk_size;
            recv_sge[i].lkey = ctx->mr->lkey;

            rr[i].sg_list = &recv_sge[i];
            rr[i].num_sge = 1;
            rr[i].next = i < max_post - 1 ? &rr[i + 1] : nullptr;
        }
        int ret = ibv_post_recv(ctx->qps[qp_idx], rr, &bad_rr);
        if (ret) {
            fprintf(stderr, "server failed post recv [%s]\n", strerror(errno));
        }

        init_count++;
        if (init_count == threads) {
            char tmp;
            char sync = 'c';
            sock::exchange_message(true, &sync, 1, &tmp, 1);
            sock::disconnect_sock(true);
            printf("Server finished init, wait for client\n");
        }*/

        struct ibv_recv_wr sup_rr;
        struct ibv_sge sup_sge;
        sup_sge.length = blk_size;
        sup_sge.addr = (uintptr_t) ctx->buf;
        sup_sge.lkey = ctx->mr->lkey;

        sup_rr.sg_list = &sup_sge;
        sup_rr.num_sge = 1;
        sup_rr.next = nullptr;

        while (finished < total_ops) {
            // poll a recv
            poll_cq(ctx->cqs[qp_idx], 1);

#ifdef DEBUG
            printf("server get request and process\n");
#endif
            // replenish a rr
            //for (int i = 0; i < max_post; ++i) {
                ret = ibv_post_recv(ctx->qps[qp_idx], &sup_rr, &bad_rr);
                if (ret) {
                    fprintf(stderr, "server failed post recv [%s]\n", strerror(errno));
                }

                send_sge.length = blk_size;
                send_sge.addr = (uintptr_t) ctx->buf;
                send_sge.lkey = ctx->mr->lkey;

                wr.sg_list = &send_sge;
                wr.num_sge = 1;
                wr.next = nullptr;
                wr.opcode = IBV_WR_SEND;
                wr.send_flags = IBV_SEND_SIGNALED;

                // post response;
                ret = ibv_post_send(ctx->qps[qp_idx], &wr, &bad_wr);
                if (ret) {
                    fprintf(stderr, "server failed post send [%s]\n", strerror(errno));
                }
            //}

#ifdef DEBUG
            printf("server replenish %d rr\n", 1);
#endif
            poll_cq(ctx->cqs[qp_idx], 1);
            finished += 1;

#ifdef DEBUG
            printf("server response [%d]\n", finished);
#endif
            //getchar();
            if (finished % 10000 == 0) {
                fprintf(stdout, "finished %ld\r", finished);
                fflush(stdout);
            }
        }
    }

    void Server::PrePostRQ(size_t blk_size, size_t max_post) {
        for (int i = 0; i < ctx_->num_qp; ++i) {
            struct ibv_sge recv_sge[max_post];
            struct ibv_recv_wr rr[max_post], *bad_rr = nullptr;
            for (int j = 0; j < max_post; ++j) {
                recv_sge[j].length = blk_size;
                recv_sge[j].addr = (uintptr_t)ctx_->buf + j * blk_size;
                recv_sge[j].lkey = ctx_->mr->lkey;

                rr[j].sg_list = &recv_sge[j];
                rr[j].num_sge = 1;
                rr[j].next = j < max_post - 1 ? &rr[j + 1] : nullptr;
            }
            printf("post recv to qp [%d]\n", i);
            int ret = ibv_post_recv(ctx_->qps[i], rr, &bad_rr);
            if (ret) {
                fprintf(stderr, "server failed pre-post recv [%s]\n", strerror(errno));
            }
        }
#ifdef DEBUG
        printf("server post %d recv wr to recv queue\n", max_post);
#endif
    }
};
