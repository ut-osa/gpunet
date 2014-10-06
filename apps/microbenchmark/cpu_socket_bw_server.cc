#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>

#include <rdma/rsocket.h>

#include "common.h"
#include "microbench_util_cpu.h"

int count_same(char* str, int len) {
	char key = str[0];
	int i;
	for (i = 0; i < len; i++) {
		if (str[i] != key)
			break;
	}
	return i;
}

int main(int argc , char *argv[])
{
	int sock;

	if (argc < 2) {
		microbench_usage_server(argc, argv);
		exit(1);
	}

	sock = microbench_server_listen(argv[1]);
	if (sock < 0) {
		perror("microbench_server_listen");
		return 1;
	}

	int client;
	struct sockaddr_in conn;
	socklen_t addrlen = sizeof(conn);
	if ((client = raccept(sock, (sockaddr*)&conn, &addrlen)) < 0) {
		perror("raccept error");
		return 1;
	}

	bench_recv_send_bw<MSG_SIZE, NR_MSG>(client);

out:

	rclose(sock);
	return 0;
}
