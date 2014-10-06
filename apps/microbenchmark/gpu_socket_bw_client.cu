#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>

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

#define TOTAL_GPUS 1
#define NR_CLIENT 1

__device__ struct gtimeval tv1, tv2;

__global__ void gpuclient(struct sockaddr_in *addr, int* tb_alloc_tbl, int nr_tb) {
	__shared__ int sock;
	int ret;

	sock = gconnect_in(addr);
	if (sock < 0) {
		BEGIN_SINGLE_THREAD_PART {
			gprintf4_single("ERROR: gconnect_in sock: %d", sock, 0, 0, 0);
		} END_SINGLE_THREAD_PART;
		return;
	}


	if (ret = gbench_send_recv_bw<BUF_SIZE, NR_MSG>(sock)) {
		printf("gbench_send_recv_bw ret: %d\n", ret);
		goto out;
	}

out:
	BEGIN_SINGLE_THREAD_PART {
		single_thread_gclose(sock);
	} END_SINGLE_THREAD_PART;
}


int main(int argc, char** argv) {

	GPUNETGlobals *gpunet;
	cudaStream_t cuda_stream;

	gpunet_microbench_init(&gpunet, &cuda_stream);

	struct sockaddr *addr, *dev_addr;

	if (argc > 2) {
		gpunet_client_init(&addr, &dev_addr, argv[1], argv[2]);
	} else {
		gpunet_usage_client(argc, argv);
		exit(1);
	}

	int *dev_tb_alloc_tbl;

	ASSERT_CUDA(cudaMalloc(&dev_tb_alloc_tbl, NR_CLIENT_TB * sizeof(*dev_tb_alloc_tbl)));
	ASSERT_CUDA(cudaMemset(dev_tb_alloc_tbl, -1, NR_CLIENT_TB * sizeof(*dev_tb_alloc_tbl)));

	gpuclient<<<1, THREADS_PER_TB, 0, cuda_stream>>>((struct sockaddr_in*)dev_addr, dev_tb_alloc_tbl, NR_CLIENT_TB);

	gpunet_loop(gpunet, cuda_stream);

	cudaDeviceReset();

	return 0;

}