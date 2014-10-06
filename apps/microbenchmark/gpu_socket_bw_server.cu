#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/param.h>

#include <map>

#include <cuda_runtime.h>
#include <poll.h>

#include <gpufs.cu.h>
#include <gpunet.cu.h>

#include "util.cu.h"

#include "common.h"
#include "microbench_util.h"

int gpu_block_count = 14;

#define TOTAL_GPUS 1
#define NR_CLIENT 1

__device__ volatile int conn_finished, server_done;

__device__ void conn_handler(int sock) {
	int ret;

	if (ret = gbench_recv_send_bw<BUF_SIZE, NR_MSG>(sock)) {
		printf("gbench_recv_send_bw ret: %d\n", ret);
		goto out;
	}

out:
	BEGIN_SINGLE_THREAD_PART {
		single_thread_gclose(sock);
	} END_SINGLE_THREAD_PART;
}

// if 0, stops
__device__ inline int is_server_done(int server_sock) {
	if (server_done) {
		conn_finished = 1;
	}
	return server_done;
}

__global__ void gpuserver(struct sockaddr_in *addr, volatile int* tb_alloc_tbl, int nr_tb) {
	__shared__ int sock;
	__shared__ int nr_connection;

	if (blockIdx.x == 0) {
		BEGIN_SINGLE_THREAD_PART {
			nr_connection = 0;
			server_done = 0;

			sock = single_thread_gbind_in(addr);
			GPU_ASSERT(sock == 0);
		} END_SINGLE_THREAD_PART;

		do {
			int conn_sock;
			BEGIN_SINGLE_THREAD_PART {
				conn_sock = gaccept(sock);
				printf("gaccept %d\n", conn_sock);
				nr_connection++;
			} END_SINGLE_THREAD_PART;

			if (gpunet_notify_connection(conn_sock, tb_alloc_tbl, nr_tb) < 0) {
				printf("notify_connection returned error sock: %d\n", conn_sock);
				break;
			}

			if (conn_sock > 0)
				while (!is_server_done(sock));

			break;
		} while (!is_server_done(sock));

	} else {
		do {
			gpunet_wait_for_connection(&sock, tb_alloc_tbl, nr_tb, &conn_finished);
			if (sock != -1) {
				conn_handler(sock);
				gpunet_finalize_connection(sock, tb_alloc_tbl);
			}
		} while(!conn_finished);
	}
}


int main(int argc, char** argv) {

	cudaStream_t cuda_stream;
	GPUNETGlobals *gpunet;

	gpunet_microbench_init(&gpunet, &cuda_stream);

	int zero = 0;
	ASSERT_CUDA(cudaMemcpyToSymbol(conn_finished, &zero, sizeof(int)));
	ASSERT_CUDA(cudaMemcpyToSymbol(server_done, &zero, sizeof(int)));

	struct sockaddr *addr, *dev_addr;

	if (argc < 2) {
		gpunet_usage_server(argc, argv);
		exit(1);
	}

	gpunet_server_init(&addr, &dev_addr, argv[1]);

	int *dev_tb_alloc_tbl;
	ASSERT_CUDA(cudaMalloc(&dev_tb_alloc_tbl, NR_SERVER_TB * sizeof(*dev_tb_alloc_tbl)));
	ASSERT_CUDA(cudaMemset(dev_tb_alloc_tbl, 255, NR_SERVER_TB * sizeof(*dev_tb_alloc_tbl)));

	gpuserver<<<2, THREADS_PER_TB, 0, cuda_stream>>>((struct sockaddr_in*)dev_addr, dev_tb_alloc_tbl, NR_SERVER_TB);

	gpunet_loop(gpunet, cuda_stream);

	cudaDeviceReset();

	return 0;
}