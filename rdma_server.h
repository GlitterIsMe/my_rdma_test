//
// Created by YiwenZhang on 2022/3/14.
//

#ifndef RDMA_RDMA_SERVER_H
#define RDMA_RDMA_SERVER_H

#include "infiniband/verbs.h"

namespace rdma {

    struct rdma_resource {

    };

    struct con_data_t {
        uint64_t addr; // buffer addr
        uint32_t rkey; //remote key
        uint32_t qp_num; // qp number
        uint16_t lid; // LID of IB port
        uint8_t  gid[16]; // gid
    }__attribute__((packed));

    class Server {
    public:
        Server(bool is_server);
        ~Server() =default;

        void InitConnection();
    private:

        void modify_qp_to_init();
        void modify_qp_to_rtr(uint32_t remote_qpn, uint16_t dlid);
        void modify_qp_to_rts();

        void connect_qp();

        struct ibv_context* ib_ctx;
        struct ibv_port_attr port_attr;
        struct ibv_pd* pd;
        struct ibv_mr* mr;
        struct ibv_cq* cq;
        struct ibv_qp* qp;

        char* buf;

        bool is_server_{false};

        con_data_t remote_con;
    };
}

#endif //RDMA_RDMA_SERVER_H
