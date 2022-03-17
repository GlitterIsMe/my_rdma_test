//
// Created by YiwenZhang on 2022/3/14.
//

#include<infiniband/verbs.h>
#include <iostream>
#include <chrono>

#include "rdma_server.h"
#include "sock.h"
#include "util.h"
#include "pmem.h"

namespace rdma {
    void Server::InitConnection() {
        int dev_num;
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
        cq = ibv_create_cq(ib_ctx, 10, nullptr, nullptr, 0);

        struct ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(ibv_qp_init_attr));
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.cap.max_send_wr = 1;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp = ibv_create_qp(pd, &qp_init_attr);

        connect_qp();
    }

    void Server::modify_qp_to_init() {
        struct ibv_qp_attr attr;
        int flags;
        memset(&attr, 0, sizeof(ibv_qp_attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = 1;
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

        flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
        int res = ibv_modify_qp(qp, &attr, flags);
        printf("to init[%d]\n", res);
    }

    void Server::modify_qp_to_rtr(uint32_t remote_qpn, uint16_t dlid) {
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
        memcpy(&attr.ah_attr.grh.dgid, remote_con.gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = 2;
        attr.ah_attr.grh.traffic_class = 0;

        flags = IBV_QP_STATE | IBV_QP_AV |
                IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;

        int res = ibv_modify_qp(qp, &attr, flags);
        printf("to rtr[%d]\n", res);
    }

    void Server::modify_qp_to_rts() {
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
        printf("to rts[%d]\n", res);
    }

    void Server::connect_qp() {
        union ibv_gid lgid;
        ibv_query_gid(ib_ctx, 1, 2, &lgid);

        con_data_t local_con;
        local_con.addr = (uintptr_t)buf_;
        local_con.rkey = mr->rkey;
        local_con.qp_num = qp->qp_num;
        local_con.lid = port_attr.lid;
        memcpy(local_con.gid, (char*)&lgid, 16);

        sock::connect_sock(is_server_, "127.0.0.1", 34567);
        sock::exchange_message(is_server_, (char*)&local_con, sizeof(con_data_t), (char*)&remote_con, sizeof(con_data_t));

        char tmp;
        char sync = 's';
        sock::exchange_message(is_server_, &sync, 1, &tmp, 1);

        printf("local qpn [%d]\n", local_con.qp_num);
        printf("lkey [%d]\n", local_con.rkey);
        printf("local addr [%ld]\n", local_con.addr);
        printf("local lid [%d]\n", local_con.lid);

        printf("remote qpn [%d]\n", remote_con.qp_num);
        printf("rkey [%d]\n", remote_con.rkey);
        printf("remote addr [%ld]\n", remote_con.addr);
        printf("lid [%d]\n", remote_con.lid);

        modify_qp_to_init();

        modify_qp_to_rtr(remote_con.qp_num, remote_con.lid);

        modify_qp_to_rts();

        sock::disconnect_sock(is_server_);
    }

    size_t Server::Write(const char *payload, size_t pld_size) {
        struct ibv_send_wr wr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t) buf_;
        sge.length = pld_size;
        sge.lkey = mr->lkey;

        memcpy(buf_, payload, pld_size);

        wr.next = nullptr;
        wr.wr_id = 0;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;

        wr.wr.rdma.remote_addr = remote_con.addr;
        wr.wr.rdma.rkey = remote_con.rkey;

        int res = ibv_post_send(qp, &wr, &bad);
        printf("post write requests[%d]\n", res);
        poll_cq();
        return 0;
    }

    size_t Server::Read(char *buffer, size_t bf_size) {
        struct ibv_send_wr wr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad;

        sge.addr = (uintptr_t)buf_;
        sge.length = 1024;
        sge.lkey = mr->lkey;

        wr.next = nullptr;
        wr.wr_id = 0;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;

        wr.wr.rdma.remote_addr = remote_con.addr;
        wr.wr.rdma.rkey = remote_con.rkey;

        int res = ibv_post_send(qp, &wr, &bad);
        printf("post read requests[%d]\n", res);
        poll_cq();
        memcpy(buffer, buf_, 1024);
        return 0;
    }

    void Server::poll_cq() {
        struct ibv_wc wc;
        int res = 0;
        while (res == 0) {
            res = ibv_poll_cq(cq, 1, &wc);
            if (res > 0) {
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "failed request[0x%x]\n", wc.status);
                }
                break;
            }
        }
    }

    void Server::WriteThroughputBench(size_t total_ops, size_t blk_size, size_t max_bacth, size_t max_post, bool random) {
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
                    ibv_poll_cq(cq, 1, &wc);
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
            ibv_post_send(qp, wr, &bad);
        }

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "Run time: " << us << " us\n";
        std::cout << "IOPS: " << (double)total_ops / us * 1000000 / 1000 << "KOPS\n";
        std::cout << "Throughput: " << (total_ops * blk_size / 1024 / 1024.0) / us * 1000000 << "MB/s\n";
    }
};
