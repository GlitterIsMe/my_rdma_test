//
// Created by YiwenZhang on 2022/3/14.
//

#include <cstring>
#include "sock.h"

int main() {
    bool is_server = false;
    char send_buf[1024];
    char recv_buf[1024];
    memset(send_buf, 0, 1024);
    memset(recv_buf, 0, 1024);

    sock::connect_sock(is_server, "127.0.0.1", 12345);
    sprintf(send_buf, "hey from client!");
    sock::exchange_message(is_server, send_buf, strlen(send_buf), recv_buf, 1024);
    printf("%s\n", recv_buf);
    sock::disconnect_sock(is_server);
    return 0;
}
