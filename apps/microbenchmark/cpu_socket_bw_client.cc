#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>

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

int main(int argc, char *argv[])
{
	int sock;
	struct sockaddr_in server;

	if (argc < 3) {
		microbench_usage_client(argc, argv);
		exit(1);
	}

	sock = microbench_client_connect(argv[1], argv[2]);

	puts("Connected\n");

	bench_send_recv_bw<MSG_SIZE, NR_MSG>(sock);

out:

	rclose(sock);
	return 0;
}
