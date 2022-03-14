//
// Created by YiwenZhang on 2022/3/14.
//

#include<infiniband/verbs.h>
#include <iostream>

#include "rdma_server.h"
#include "sock.h"

namespace rdma {
    void Server::InitConnection() {
        int dev_num;
        struct ibv_device** dev_list = ibv_get_device_list(&dev_num);
        std::cout << dev_num << std::endl;

        ib_ctx = ibv_open_device(dev_list[0]);

        ibv_free_device_list(dev_list);

        pd = ibv_alloc_pd(ib_ctx);

        ibv_query_port(ib_ctx, 1, &port_attr);

        buf = new char[1024];
        memset(buf, 0, 1024);

        mr = ibv_reg_mr(pd, buf, 1024, IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC);

        struct ibv_device_attr dev_attr;
        ibv_query_device(ib_ctx, &dev_attr);

        //struct ibv_srq_init_attr srq_init_attr;
        cq = ibv_create_cq(ib_ctx, 10, nullptr, nullptr, 0);

        struct ibv_qp_init_attr qp_init_attr;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.sq_sig_all =1;
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
        ibv_modify_qp(qp, &attr, flags);
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
        attr.min_rnr_timer = 0x12; // IBV_QP_MIN_RNR_TIMER

        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = dlid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = 1;

        flags = IBV_QP_STATE | IBV_QP_AV |
                IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;

        ibv_modify_qp(qp, &attr, flags);
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

        ibv_modify_qp(qp, &attr, flags);
    }

    void Server::connect_qp() {
        con_data_t local_con;
        local_con.addr = (uintptr_t)buf;
        local_con.rkey = mr->rkey;
        local_con.qp_num = qp->qp_num;
        local_con.lid = port_attr.lid;

        sock::connect_sock(is_server_, "127.0.0.1", 45678);
        sock::exchange_message(is_server_, (char*)&local_con, sizeof(con_data_t), (char*)&remote_con, sizeof(con_data_t));

        modify_qp_to_init();

        modify_qp_to_rtr(remote_con.qp_num, remote_con.lid);

        modify_qp_to_rts();

        char tmp;
        sock::exchange_message(is_server_, "S", 1, &tmp, 1);

        sock::disconnect_sock(is_server_);
    }
};
