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
        explicit Server(bool is_server, char* buf): is_server_(is_server), buf_(buf) {};
        ~Server() =default;

        void InitConnection();

        size_t Write(const char* payload, size_t pld_size);

        size_t Read(char* buffer, size_t bf_size);

        void WriteThroughputBench(size_t total_ops, size_t blk_size, size_t max_bacth, size_t max_post, bool random);

    private:

        void modify_qp_to_init();
        void modify_qp_to_rtr(uint32_t remote_qpn, uint16_t dlid);
        void modify_qp_to_rts();

        void connect_qp();

        void poll_cq();

        struct ibv_context* ib_ctx {nullptr};
        struct ibv_port_attr port_attr {};
        struct ibv_pd* pd {nullptr};
        struct ibv_mr* mr {nullptr};
        struct ibv_cq* cq {nullptr};
        struct ibv_qp* qp {nullptr};

        char* buf_ {nullptr};

        const bool is_server_{false};

        con_data_t remote_con {};
    };
}

#endif //RDMA_RDMA_SERVER_H
