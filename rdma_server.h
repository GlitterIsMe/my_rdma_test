//
// Created by YiwenZhang on 2022/3/14.
//

#ifndef RDMA_RDMA_SERVER_H
#define RDMA_RDMA_SERVER_H

#include <vector>
#include <tuple>

#include "infiniband/verbs.h"
#include "histogram.h"

namespace rdma {

    const uint64_t CLIENT_BUF_SIZE = 1024UL * 1024 * 1024;

    enum BenchType {
        ReadLat,
        WriteLat,
        PwriteLat,
        CASLat,
        NoopLat,
    };

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
        uint64_t buf_size;

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
        explicit Server(RDMA_Context* ctx, bool is_server): ctx_(ctx), is_server_(is_server) {
            histogram.Clear();
            post_hist.Clear();
            poll_hist.Clear();
        };
        ~Server() =default;

        void InitConnection(int num_clis, bool server = false);

        void Write(int qp_idx, char* src, size_t src_len, char* dest);

        void Read(int qp_idx, char* src, size_t src_len, char* dest);

        void Pwrite(int qp_idx, char* src, size_t src_len, char* dest);
        void Pwrite(int qp_idx, struct ibv_send_wr* wr, struct ibv_send_wr* bad);

        void Noop(int qp_idx, char* src, size_t src_len, char* dest);

        bool CAS(int qp_idx, char* src, char* dest, uint64_t old_v, uint64_t new_v);

        using cas_op = std::tuple<char*, char*, uint64_t, uint64_t>;
        bool MultiCAS(int qp_idx, std::vector<cas_op>& ops);

        /*void WriteThroughputBench(size_t total_ops, size_t blk_size,
                                  size_t max_bacth, size_t max_post,
                                  bool random);*/

        /*static void WriteThroughputBench(RDMA_Context* ctx, int threads,
                                         int qp_idx, size_t total_ops,
                                  size_t blk_size, size_t max_batch,
                                  size_t max_post, bool random,
                                  bool persist);*/

        static void WriteThroughputBench(Server* server, int threads,
                                         int qp_idx, size_t total_ops,
                                         size_t blk_size, bool random,
                                         bool persist);

        static void PwriteThroughputBench(Server* server, int threads,
                                         int qp_idx, size_t total_ops,
                                         size_t blk_size, bool random,
                                         bool persist);

        static void ReadThroughputBench(Server* server, int threads,
                                         int qp_idx, size_t total_ops,
                                         size_t blk_size, bool random,
                                         bool persist);

        static void CASThroughputBench(Server* server, int threads,
                                         int qp_idx, size_t total_ops, bool random,
                                         int max_post);

        /*static void CASThroughputBench(RDMA_Context* ctx, int threads,
                                       int qp_idx, size_t total_ops,
                                       size_t max_batch, size_t max_post,
                                       bool random, bool persist);*/

        static void EchoThroughputBench_Client(RDMA_Context* ctx, int threads,
                                         int qp_idx, size_t total_ops,
                                         size_t blk_size, size_t max_post);

        static void EchoThroughputBench_Server(RDMA_Context* ctx, int threads,
                                               int qp_idx, size_t total_ops,
                                               size_t blk_size,
                                               size_t max_post);

        static void LatencyBench(Server* server, BenchType type, int threads, int qp_idx, size_t total_ops,
                                  size_t blk_size);

        void HashAccessSimulation();
        void HashAccessSimulation2();

        void PrePostRQ(size_t blk_size, size_t max_post);

    private:

        void modify_qp_to_init(struct ibv_qp* qp);
        void modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* gid);
        void modify_qp_to_rts(struct ibv_qp* qp);

        void connect_qp(struct ibv_cq* cq, struct ibv_qp* qp, con_data_t* remote_conn, int cli_no = 0, int total_cli_no = 1);

        static void poll_cq(struct ibv_cq* cq, int num_comps);

        RDMA_Context* ctx_;
        leveldb::Histogram histogram, post_hist, poll_hist;
        const bool is_server_{false};
    };
}

#endif //RDMA_RDMA_SERVER_H
