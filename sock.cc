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

#include "sock.h"

namespace sock {
    int local_sock, remote_sock;

    struct sockaddr_in server_addr, client_addr;

    int connect_sock(bool is_server, std::string ip, int port){
        local_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
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
            //printf("client read\n");
            //printf("[%lu]%s\n",strlen(recv_buf), recv_buf);
        }
        return 0;
    }

    int disconnect_sock(bool is_server) {
        close(local_sock);
        if (is_server) {
            close(remote_sock);
        }
        return 0;
    }
}


