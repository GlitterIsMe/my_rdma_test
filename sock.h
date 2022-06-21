//
// Created by YiwenZhang on 2022/3/14.
//

#ifndef RDMA_SOCK_H
#define RDMA_SOCK_H

#include <string>

namespace sock {

    int connect_sock(bool is_server, std::string ip, int port);

    int connect_multi_sock(bool is_server, std::string ip, int port, int num_clients);

    int exchange_message(bool is_server, char* send_buf, size_t send_size, char* recv_buf, size_t recv_size);

    int server_exchange_multi_message(int c_no, bool is_server, char* send_buf, size_t send_size, char* recv_buf, size_t recv_size);

    int disconnect_sock(bool is_server);

    int disconnect_multi_sock(bool is_server);

    int exchange_ready_message(bool is_server, char* send_buf, size_t send_size, char* recv_buf, size_t recv_size);

};

#endif //RDMA_SOCK_H
