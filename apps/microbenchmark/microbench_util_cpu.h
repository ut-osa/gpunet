#ifndef __BENCH_UTIL__H__
#define __BENCH_UTIL__H__

#include <stdlib.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>

#include <rdma/rsocket.h>

static
int microbench_client_connect_async(char* str_ip, char* str_port, int* inprogress) {
	int sock = rsocket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) {
		perror("could not create socket");
		return -1;
	}

	struct sockaddr_in *addr;
	struct addrinfo hints, *server_addr;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	int fd_flags = rfcntl(sock, F_GETFL);
	int ret = rfcntl(sock, F_SETFL, (fd_flags | O_NONBLOCK));
	if (ret == -1) {
		perror("microbench_client_connect_async rfcntl");
		return 1;
	}	

	if (getaddrinfo(str_ip, str_port, &hints, &server_addr)) {
		perror("microbench_client_connect_async getaddrinfo");
		return -1;
	}

	if (rconnect(sock, server_addr->ai_addr, server_addr->ai_addrlen) < 0) {
		if ((errno != EINPROGRESS) && (errno != EALREADY)) {
			perror("microbench_client_connect_async rconnect");
			return -1;
		}
		if (inprogress)
			*inprogress = 1;
	} else if (inprogress) {
		*inprogress = 0;
	}

	return sock;
}

static
int microbench_client_connect(char* str_ip, char* str_port) {
	int sock = rsocket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) {
		perror("could not create socket");
		return -1;
	}

	struct sockaddr_in *addr;
	struct addrinfo hints, *server_addr;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(str_ip, str_port, &hints, &server_addr)) {
		perror("microbench_client_connect getaddrinfo");
		return -1;
	}

	if (rconnect(sock, server_addr->ai_addr, server_addr->ai_addrlen) < 0) {
		if (errno != EINPROGRESS) {
			perror("microbench_client_connect rconnect");
		}
		return -1;
	}

	return sock;
}

int microbench_server_listen(int port) {
	struct sockaddr_in *addr;

	if (port > 65536 || port < 0) {
		errno = EINVAL;
		return -1;
	}

	struct sockaddr_in server;

	int sock = rsocket(AF_INET , SOCK_STREAM , 0);
	if (sock == -1) {
		perror("could not create socket");
		return -1;
	}

	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	if (rbind(sock, (struct sockaddr*)&server , sizeof(server)) < 0) {
		fprintf(stderr, "errno: %d\n", errno);
		perror("connection error");
		return -1;
	}

	if (rlisten(sock, 10) < 0) {
		perror("rlisten error");
		return -1;
	}

	return sock;
}

int microbench_server_listen(char* str_port) {
	long int svr_port = (long int)strtol(str_port, (char**)NULL, 10);
	if(svr_port == LONG_MIN || svr_port == LONG_MAX) {
		return -1;
	}

	return microbench_server_listen(svr_port);
}

void microbench_usage_server(int argc, char** argv) {
	fprintf(stderr, "Usage: %s [port]\n", argv[0]);
}

#ifdef __cplusplus
void microbench_usage_server(int argc, char** argv, const char* option) {
    fprintf(stderr, "Usage: %s [port] %s\n", argv[0], option);
}
#endif

void microbench_usage_client(int argc, char** argv) {
	fprintf(stderr, "Usage: %s [ip addr] [port]\n", argv[0]);
}

#ifdef __cplusplus
void microbench_usage_client(int argc, char** argv, const char* option) {
    fprintf(stderr, "Usage: %s [ip addr] [port] %s\n", argv[0], option);
}
#endif



#endif
