//
// Created by YiwenZhang on 2022/3/14.
//

#ifndef RDMA_RDMA_SERVER_H
#define RDMA_RDMA_SERVER_H

#include <vector>

#include "infiniband/verbs.h"
#include "histogram.h"

namespace rdma {

    const uint64_t CLIENT_BUF_SIZE = 1024UL * 1024 * 1024;

    struct con_data_t {
        uint64_t addr; // buffer addr
        uint32_t rkey; //remote key
        uint32_t qp_num; // qp number
        uint16_t lid; // LID of IB port
        uint8_t  gid[16]; // gid
    }__attribute__((packed));

    struct RDMA_Context {
        struct ibv_context* ib_ctx {nullptr};
        struct ibv_port_attr port_attr {};
        struct ibv_pd* pd {nullptr};
        struct ibv_mr* mr {nullptr};

        int num_qp;
        struct ibv_cq** cqs {nullptr};
        struct ibv_qp** qps {nullptr};

        char* buf {nullptr};

        rdma::con_data_t* remote_conn;

        ~RDMA_Context() {
            delete[] remote_conn;
            for (int i = 0; i < num_qp; ++i) {
                ibv_destroy_qp(qps[i]);
                ibv_destroy_cq(cqs[i]);
            }
            delete[] cqs;
            delete[] qps;
            ibv_dereg_mr(mr);
            ibv_dealloc_pd(pd);
            ibv_close_device(ib_ctx);
        }
    };

    void InitRDMAContext(rdma::RDMA_Context* ctx, int num_qp, char* buf, size_t buf_size);

    class Server {
    public:
        /*explicit Server(bool is_server, char* buf, int num_qp):
            is_server_(is_server),
            buf_(buf),
            num_qp_(num_qp){};*/
        explicit Server(RDMA_Context* ctx, bool is_server): ctx_(ctx), is_server_(is_server) {};
        ~Server() =default;

        void InitConnection();

        size_t Write(const char* payload, size_t pld_size);

        size_t Read(char* buffer, size_t bf_size);

        /*void WriteThroughputBench(size_t total_ops, size_t blk_size,
                                  size_t max_bacth, size_t max_post,
                                  bool random);*/

        static void WriteThroughputBench(RDMA_Context* ctx, int threads,
                                         int qp_idx, size_t total_ops,
                                  size_t blk_size, size_t max_batch,
                                  size_t max_post, bool random,
                                  bool persist, leveldb::Histogram* hist);

        static void CASThroughputBench(RDMA_Context* ctx, int threads,
                                       int qp_idx, size_t total_ops,
                                       size_t max_batch, size_t max_post,
                                       bool random, bool persist);

        static void EchoThroughputBench_Client(RDMA_Context* ctx, int threads,
                                         int qp_idx, size_t total_ops,
                                         size_t blk_size, size_t max_post);

        static void EchoThroughputBench_Server(RDMA_Context* ctx, int threads,
                                               int qp_idx, size_t total_ops,
                                               size_t blk_size,
                                               size_t max_post);

        void PrePostRQ(size_t blk_size, size_t max_post);

    private:

        void modify_qp_to_init(struct ibv_qp* qp);
        void modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* gid);
        void modify_qp_to_rts(struct ibv_qp* qp);

        void connect_qp(struct ibv_cq* cq, struct ibv_qp* qp, con_data_t* remote_conn);

        static void poll_cq(struct ibv_cq* cq, int num_comps);

        RDMA_Context* ctx_;
        const bool is_server_{false};
    };
}

#endif //RDMA_RDMA_SERVER_H
