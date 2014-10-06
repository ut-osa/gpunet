#ifndef GSOCKET_CU_H
#define GSOCKET_CU_H

#include <arpa/inet.h>

#include "net_constants.h"
#include "net_globals.h"
#include "net_util.cu.h"

__device__ int gsocket( int domain, int type, int protocol);

__device__ int single_thread_gbind(const char* name, int namelen);
__device__ int single_thread_gbind_in(const sockaddr_in* addr);
__device__ int single_thread_gbind_bounce_in(const sockaddr_in* addr);

__device__ int gconnect(const char* name, int namelen);
__device__ int gconnect_in(const struct sockaddr_in* addr);
__device__ int gconnect_ib(const struct sockaddr_in* addr);
__device__ int gconnect_bounce_in(const struct sockaddr_in* addr);

__device__ int gaccept(int soc);
__device__ int gaccept_nb(int soc);

__device__ int single_thread_gshutdown(int soc, int how=SHUT_RDWR);

__device__ int single_thread_gclose(int soc);

__device__ int gsend(int sock, uchar* to_send, int size);
__device__ int gsend_test(int sock, uchar* to_send, int size);

__device__ int grecv(int sock, uchar* to_recv, int size);
__device__ int grecv_nb(int sock, void* to_recv, int size); // This function is ONLY IMPLEMENTED FOR UNIX SOCKETS

// in Linux, blocking option is typically set through fcntl()
// since we do not have fcntl, this function handles such case separately
__device__ void gsetsock_block(int socket, int blocking);
// returns nonzero if blocking socket
__device__ int ggetsock_block(int socket);

struct gpollfd {
	int fd;
	short events;
	short revents;
	unsigned rbytes;
	unsigned wbytes;
};

__device__ int gpoll(struct gpollfd* fds, size_t nfds, int nclock_timeout);


struct gtimeval {
	long int tv_sec;
	long tv_usec;
};

__device__ void ggettimeofday(struct gtimeval *tv);
__device__ void gputs(const char* str, int len);
__device__ void gputs_single(const char* str, int len);
__device__ void gprintf4_single(const char* str, int arg1, int arg2, int arg3, int arg4);

// c = a - b
__device__ void gtimersub(struct gtimeval *a, struct gtimeval *b, struct gtimeval *c);

__device__ int gsynclaunch(const char* func_name,
                           int grid_dim[3],
                           int block_dim[3],
                           const void* arg);

__device__ int gasynclaunch(const char* func_name,
                            int grid_dim[3],
                            int block_dim[3],
                            const void* arg);

enum GAIOP {
	GAIO_ACCEPT,
	GAIO_RECEIVE,
};

// Returns the slot number
__device__ int gaio(int sock, GAIOP op, void* buf, int size);

__device__ int gaio_poll(int slot, int sock, GAIOP op, void* buf, int size);

#endif
