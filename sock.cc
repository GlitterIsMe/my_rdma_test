//
// Created by YiwenZhang on 2022/3/14.
//
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <vector>

#include "sock.h"

namespace sock {
    int local_sock, remote_sock;

    struct sockaddr_in server_addr, client_addr;

    std::vector<struct sockaddr_in> client_addrs;
    std::vector<int> remote_socks;

    int clients_nums;

    int connect_sock(bool is_server, std::string ip, int port){
        local_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

        unsigned value = 1;
        setsockopt(local_sock, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value));

        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
        server_addr.sin_port = htons(port);

        if (is_server) {
            int bind_res = bind(local_sock, (struct sockaddr*)(&server_addr), sizeof(server_addr));
            if (bind_res != 0) {
                fprintf(stderr, "bind failed [%s]\n", strerror(errno));
                exit(-1);
            }
            printf("bind local sock %d with ip %s\n", local_sock, ip.c_str());
            listen(local_sock, 10);
            printf("listen to local socket %d\n", local_sock);
            socklen_t client_addr_len;
            remote_sock = accept(local_sock, (struct sockaddr*)&client_addr, &client_addr_len);
            char buf[256];
            printf("accept connection of client %s from socket %d\n", inet_ntop(AF_INET, &client_addr.sin_addr, buf, 256), remote_sock);
        } else {
            int connect_res = connect(local_sock, (struct sockaddr*)&server_addr, sizeof(server_addr));
            printf("connect to the server %s:%d\n", ip.c_str(), port);
            if (connect_res != 0) {
                fprintf(stderr, "connect failed\n");
                exit(-1);
            }
        }
        return 0;
    }

    int connect_multi_sock(bool is_server, std::string ip, int port, int num_clients) {
        clients_nums = num_clients;
        local_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

        unsigned value = 1;
        setsockopt(local_sock, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value));

        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
        server_addr.sin_port = htons(port);

        if (is_server) {
            for (int i = 0; i < num_clients; ++i) {
                struct sockaddr_in caddr;
                client_addrs.push_back(caddr);
            }
            int bind_res = bind(local_sock, (struct sockaddr*)(&server_addr), sizeof(server_addr));
            if (bind_res != 0) {
                fprintf(stderr, "bind failed [%s]\n", strerror(errno));
                exit(-1);
            }
            printf("bind local sock %d with ip %s\n", local_sock, ip.c_str());
            listen(local_sock, 10);
            printf("listen to local socket %d\n", local_sock);
            socklen_t client_addr_len;
            for (int i = 0; i < num_clients; ++i) {
                remote_socks.push_back(accept(local_sock, (struct sockaddr*)&client_addrs[i], &client_addr_len));
                char buf[256];
                printf("accept connection of client %s from socket %d\n", inet_ntop(AF_INET, &client_addrs[i].sin_addr, buf, 256), remote_socks[i]);
            }
            //remote_sock = accept(local_sock, (struct sockaddr*)&client_addr, &client_addr_len);
        } else {
            int connect_res = connect(local_sock, (struct sockaddr*)&server_addr, sizeof(server_addr));
            printf("connect to the server %s:%d\n", ip.c_str(), port);
            if (connect_res != 0) {
                fprintf(stderr, "connect failed\n");
                exit(-1);
            }
        }
        printf("Finish socket connections\n");
        return 0;
    }

    int exchange_message(bool is_server, char* send_buf, size_t send_size, char* recv_buf, size_t recv_size){
        if (is_server) {
            read(remote_sock, recv_buf, recv_size);
            //printf("server read\n");
            //printf("[%lu]%s\n", strlen(recv_buf), recv_buf);
            //sprintf(send_buf, "hello from server\n");
            write(remote_sock, send_buf, send_size);
            //printf("server write\n");
        } else {
            //sprintf(send_buf, "hello from client\n");
            write(local_sock, send_buf, send_size);
            //printf("client write\n");
            read(local_sock, recv_buf, recv_size);
            /*if (recv_size == 1) {
                printf("client read [%c]\n", recv_buf[0]);
            }
            printf("client read %lu\n", recv_size);*/
        }
        return 0;
    }

    int server_exchange_multi_message(int c_no, bool is_server, char* send_buf, size_t send_size, char* recv_buf, size_t recv_size) {
        if (is_server) {
            read(remote_socks[c_no], recv_buf, recv_size);
            //printf("server read\n");
            //printf("[%lu]%s\n", strlen(recv_buf), recv_buf);
            //sprintf(send_buf, "hello from server\n");
            write(remote_socks[c_no], send_buf, send_size);
            //printf("server write\n");
        } else {
            //sprintf(send_buf, "hello from client\n");
            write(local_sock, send_buf, send_size);
            //printf("client write\n");
            read(local_sock, recv_buf, recv_size);
            /*if (recv_size == 1) {
                printf("client read [%c]\n", recv_buf[0]);
            }
            printf("client read %lu\n", recv_size);*/
        }
        return 0;
    }

    int exchange_ready_message(bool is_server, char* send_buf,
                               size_t send_size, char* recv_buf, size_t recv_size) {
        if (is_server) {
            for (int i = 0; i < clients_nums; ++i) {
                read(remote_socks[i], recv_buf, recv_size);
                //printf("recv ready from [%d]\n", remote_socks[i]);
            }

            for (int i = 0; i < clients_nums; ++i) {
                write(remote_socks[i], send_buf, send_size);
                //printf("send ready to [%d]\n", remote_socks[i]);
            }
            //printf("server read\n");
            //printf("[%lu]%s\n", strlen(recv_buf), recv_buf);
            //sprintf(send_buf, "hello from server\n");
            //printf("server write\n");
        } else {
            //sprintf(send_buf, "hello from client\n");
            write(local_sock, send_buf, send_size);
            //printf("client write\n");
            read(local_sock, recv_buf, recv_size);
            /*if (recv_size == 1) {
                printf("client read [%c]\n", recv_buf[0]);
            }
            printf("client read %lu\n", recv_size);*/
        }
        return 0;
    }

    int disconnect_sock(bool is_server) {
        //close(local_sock);
        if (is_server) {
            close(remote_sock);
        }
        close(local_sock);
        return 0;
    }

    int disconnect_multi_sock(bool is_server) {
        //close(local_sock);
        if (is_server) {
            for (int i = 0; i < clients_nums; ++i) {
                close(remote_socks[i]);
            }
        }
        close(local_sock);
        return 0;
    }
}


