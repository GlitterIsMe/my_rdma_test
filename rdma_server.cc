//
// Created by YiwenZhang on 2022/3/14.
//

#include<infiniband/verbs.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>

#include "rdma_server.h"
#include "sock.h"
#include "util.h"
#include "pmem.h"

namespace rdma {

    void InitRDMAContext(rdma::RDMA_Context* ctx, int num_qp, char* buf, size_t buf_size) {
        int dev_num;
        struct ibv_device** dev_list = ibv_get_device_list(&dev_num);
        std::cout << "Find " << dev_num << " RDMA devices" << std::endl;

        ctx->ib_ctx = ibv_open_device(dev_list[0]);

        ibv_free_device_list(dev_list);

        ctx->pd = ibv_alloc_pd(ctx->ib_ctx);

        ibv_query_port(ctx->ib_ctx, 1, &ctx->port_attr);

        ctx->buf = buf;
        memset(ctx->buf, 0, buf_size);
        std::cout << "Register memory region [" << buf_size / 1024.0 / 1024 / 1024 << "] GB\n";
        ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, buf_size, IBV_ACCESS_LOCAL_WRITE |
                                                      IBV_ACCESS_REMOTE_READ |
                                                      IBV_ACCESS_REMOTE_WRITE |
                                                      IBV_ACCESS_REMOTE_ATOMIC);
        if (ctx->mr == nullptr) {
            fprintf(stderr, "Register memory failed\n");
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
            ctx->cqs[i] = ibv_create_cq(ctx->ib_ctx, 10,
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
        ibv_query_gid(ctx_->ib_ctx, 1, 2, &lgid);

        con_data_t local_con;
        local_con.addr = (uintptr_t)ctx_->buf;
        local_con.rkey = ctx_->mr->rkey;
        local_con.qp_num = qp->qp_num;
        local_con.lid = ctx_->port_attr.lid;
        memcpy(local_con.gid, (char*)&lgid, 16);

        std::cout << "Exchange QP info and connect\n";
        sock::exchange_message(is_server_, (char*)&local_con, sizeof(con_data_t), (char*)remote_conn, sizeof(con_data_t));

        char tmp;
        char sync = 's';
        sock::exchange_message(is_server_, &sync, 1, &tmp, 1);
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
    }

    void Server::modify_qp_to_init(struct ibv_qp* qp) {
        struct ibv_qp_attr attr;
        int flags;
        memset(&attr, 0, sizeof(ibv_qp_attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = 1;
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

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
        attr.max_dest_rd_atomic = 1; // IBV_QP_MAX_DEST_RD_ATOMIC
        attr.min_rnr_timer = 12; // IBV_QP_MIN_RNR_TIMER

        attr.ah_attr.is_global = 1;
        attr.ah_attr.dlid = dlid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = 3;
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
        attr.timeout = 0x12;
        attr.retry_cnt = 6;
        attr.rnr_retry = 0;
        attr.sq_psn = 0;
        attr.max_rd_atomic = 1;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT |
                IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

        int res = ibv_modify_qp(qp, &attr, flags);
        printf("Transfer QP to rts[%d]\n", res);
    }

    size_t Server::Write(const char *payload, size_t pld_size) {
        for (int i = 0; i < ctx_->num_qp; ++i) {
            struct ibv_send_wr wr;
            struct ibv_sge sge;
            struct ibv_send_wr *bad;

            sge.addr = (uintptr_t) ctx_->buf;
            sge.length = pld_size;
            sge.lkey = ctx_->mr->lkey;

            memcpy(ctx_->buf, payload, pld_size);

            wr.next = nullptr;
            wr.wr_id = 0;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;

            wr.wr.rdma.remote_addr = ctx_->remote_conn[i].addr;
            wr.wr.rdma.rkey = ctx_->remote_conn[i].rkey;

            int res = ibv_post_send(ctx_->qps[i], &wr, &bad);
            printf("post write requests[%d]\n", res);
            poll_cq(ctx_->cqs[i], 1);
        }
        return 0;
    }

    size_t Server::Read(char *buffer, size_t bf_size) {
        for (int i = 0; i < ctx_->num_qp; ++i) {
            struct ibv_send_wr wr;
            struct ibv_sge sge;
            struct ibv_send_wr *bad;

            sge.addr = (uintptr_t)ctx_->buf;
            sge.length = 1024;
            sge.lkey = ctx_->mr->lkey;

            wr.next = nullptr;
            wr.wr_id = 0;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.send_flags = IBV_SEND_SIGNALED;

            wr.wr.rdma.remote_addr = ctx_->remote_conn[i].addr;
            wr.wr.rdma.rkey = ctx_->remote_conn[i].rkey;

            int res = ibv_post_send(ctx_->qps[i], &wr, &bad);
            printf("post read requests[%d]\n", res);
            poll_cq(ctx_->cqs[i], 1);
            memcpy(buffer, ctx_->buf, 1024);
        }
        return 0;
    }

    void Server::poll_cq(struct ibv_cq* cq, int num_comps) {
        struct ibv_wc wc;
        int res = 0, total_comp = 0;
        while (total_comp < num_comps) {
            res = ibv_poll_cq(cq, num_comps, &wc);
            if (res > 0) {
#ifdef DEBUG
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stdout, "failed request [%ld] [0x%x]\n", wc.wr_id, wc.status);
                }
#endif
                total_comp += res;
            }
        }
    }
    /*void Server::WriteThroughputBench(size_t total_ops, size_t blk_size, size_t max_bacth, size_t max_post, bool random) {
        *//*std::cout << "Run RDMA Write Benchmark: \n"
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
        std::cout << "Throughput: " << (total_ops * blk_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";*//*
    }*/

        /*
         * parameter:
         * - max_batch: signal frequency
         * - max_post: the number of posted wr for each post_send
         * */
    void Server::WriteThroughputBench(RDMA_Context* ctx, int threads, int qp_idx,
                                      size_t total_ops, size_t blk_size, size_t max_batch,
                                      size_t max_post, bool random, bool persist) {
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
                wr[i].opcode = IBV_WR_RDMA_WRITE;
                wr[i].wr_id = qp_idx;
                wr[i].num_sge = 1;
                wr[i].sg_list = &sge;

                if (i < max_post - 1) {
                    wr[i].next = &wr[i+1];
                } else {
                    wr[i].next = nullptr;
                }

                if (!persist && posted_wr % max_batch == 0) {
                    wr[i].send_flags = IBV_SEND_SIGNALED;
                }

                wr[i].wr.rdma.rkey = ctx->remote_conn[i].rkey;
                if (random) {
                    // random write
                    /*
                     * 1. divide the total PM space into N blocks with granularity of block size;
                     * 2. randomly choose a block in [0, N]
                     * */
                    assert(remote_block_num != 0);
                    size_t rd_blk  = hrd_fastrand(&seed) % remote_block_num;
                    wr[i].wr.rdma.remote_addr = ctx->remote_conn[qp_idx].addr + rd_blk * blk_size;
                } else {
                    wr[i].wr.rdma.remote_addr = ctx->remote_conn[qp_idx].addr + pm_start + offset;
                    offset += blk_size;
#ifdef DEBUG
                    fprintf(stdout, "[%d] write offset [%lu] = (base)[%lu] + (start)[%lu] + offset[%lu]\n",qp_idx,
                            ctx->remote_conn[qp_idx].addr + pm_start + offset,
                            ctx->remote_conn[qp_idx].addr, pm_start, offset);
#endif
                }
            }
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
            if (posted_wr % max_batch == 0 && posted_wr > 0) {
                poll_cq(ctx->cqs[qp_idx], 1);
            }

            if (finished % 1000000 == 0) {
                fprintf(stdout, "finished %ld\n", finished);
                fflush(stdout);
            }
        }
        //std::cout << "thread " << qp_idx << " end process\n";
    }
};
