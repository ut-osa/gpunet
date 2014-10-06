#ifndef __BENCH_UTIL__H__
#define __BENCH_UTIL__H__

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <cuda_runtime.h>

#include <gpunet.cu.h>

static
void gpunet_microbench_init(GPUNETGlobals** pglobal, cudaStream_t* pkernel_stream) {
	volatile GPUGlobals* gpufs;

	initializer(&gpufs);

	// 32kb stack per thread (or 4MB per TB), and 3GB global memory
	ASSERT_CUDA(cudaDeviceSetLimit(cudaLimitStackSize, 16 * (1u << 10)));
	ASSERT_CUDA(cudaDeviceSetLimit(cudaLimitMallocHeapSize, 3 * (1u << 30)));

	*pglobal = new GPUNETGlobals(0);
	*pkernel_stream = gpufs->streamMgr->kernelStream;
}

int gpunet_server_init(struct sockaddr **paddr, struct sockaddr **dev_addr, int port) {
	struct sockaddr_in *addr;

	if (port > 65536 || port < 0) {
		errno = EINVAL;
		return -1;
	}

	ASSERT_CUDA(cudaHostAlloc((void**)&addr, sizeof(sockaddr), cudaHostAllocMapped));
	memset(addr, 0, sizeof(*addr));
	addr->sin_addr.s_addr = htonl(INADDR_ANY);
	addr->sin_port = ntohs(port);
	addr->sin_family = PF_INET;

	ASSERT_CUDA(cudaHostGetDevicePointer((void**)dev_addr, addr, 0));

	*paddr = (struct sockaddr*)addr;

	return 0;
}

int gpunet_server_init(struct sockaddr **paddr, struct sockaddr **dev_addr, char* str_port) {
    long int svr_port = (long int)strtol(str_port, (char**)NULL, 10);
    if(svr_port == LONG_MIN || svr_port == LONG_MAX) {
        return -1;
    }

    return gpunet_server_init(paddr, dev_addr, (int)svr_port);
}

int gpunet_client_init(struct sockaddr **paddr, struct sockaddr **dev_addr, char* str_ip, char* str_port) {
	struct sockaddr_in *addr;
	struct addrinfo hints, *server_addr;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(str_ip, str_port, &hints, &server_addr)) {
		perror("gpunet_client_init getaddrinfo");
		return -1;
	}

	ASSERT_CUDA(cudaHostAlloc((void**)&addr, sizeof(sockaddr), cudaHostAllocMapped));
	memcpy(addr, server_addr->ai_addr, server_addr->ai_addrlen);

	ASSERT_CUDA(cudaHostGetDevicePointer((void**)dev_addr, addr, 0));

	*paddr = server_addr->ai_addr;

	return 0;
}

void gpunet_usage_server(int argc, char** argv, const char* extra_param_str) {
	fprintf(stderr, "Usage: %s [port] %s\n", argv[0], extra_param_str);
}
void gpunet_usage_server(int argc, char** argv) {
	gpunet_usage_server(argc, argv, "");
}


void gpunet_usage_client(int argc, char** argv, char* extra_param_str) {
	fprintf(stderr, "Usage: %s [ip addr] [port] %s\n", argv[0], extra_param_str);
}

void gpunet_usage_client(int argc, char** argv) {
	gpunet_usage_client(argc, argv, "");
}

int gpunet_loop(GPUNETGlobals* gpunet, cudaStream_t kernel_stream, int timeout_in_ms) {
	ASSERT_CUDA(cudaPeekAtLastError());

	struct timeval tv1, tv2, tv3;

	if (timeout_in_ms != 0)
		gettimeofday(&tv1, NULL);

	long int elapsed_ms;
	bool timed_out = false;

	while (true) {
		for (int i = 0; i < (1 << 10); i++) {
			gpunet->single_hostloop(kernel_stream);
		}
		if (cudaSuccess == cudaStreamQuery(kernel_stream)) {
			DBG("Loop done\n", "");
			break;
		}

		if (timeout_in_ms != 0) {
			gettimeofday(&tv2, NULL);
			timersub(&tv2, &tv1, &tv3);
			elapsed_ms = (long int)tv3.tv_sec * 1000 + (long int)tv3.tv_usec / 1000;
			if (elapsed_ms > timeout_in_ms) {
				timed_out = true;
				break;
			}
		}
	}

	if (!timed_out) {
		ASSERT_CUDA(cudaDeviceSynchronize());
		ASSERT_CUDA(cudaPeekAtLastError());
	}

	return (timed_out ? 1 : 0);
}

int gpunet_loop(GPUNETGlobals* gpunet, cudaStream_t kernel_stream) {
	return gpunet_loop(gpunet, kernel_stream, 0);
}


// called by (blockIdx 0) thread
__device__
int gpunet_notify_connection(int conn_sock, volatile int* tb_alloc_tbl, int nr_tb) {
	// to be optimized.
	__shared__ int ret;

	BEGIN_SINGLE_THREAD_PART {
		ret = -1;

		for(int i = 1; i < nr_tb; i++) {
			if (tb_alloc_tbl[i] == -1) {
				tb_alloc_tbl[i] = conn_sock;
				ret = i;
				break;
			}
		}
	} END_SINGLE_THREAD_PART;

	return ret;
}

// called by threads within non-zero blockIdx
__device__
void gpunet_wait_for_connection(int *sock, volatile int* tb_alloc_tbl, int nr_tb, volatile int *conn_finished) {
	__shared__ int ret;

	BEGIN_SINGLE_THREAD_PART {
		ret = -1;

		while(!*conn_finished) {
			if (tb_alloc_tbl[blockIdx.x] != -1) {
				ret = tb_alloc_tbl[blockIdx.x];
				break;
			}
		}

	} END_SINGLE_THREAD_PART;

	*sock = ret;
}

__device__
void gpunet_finalize_connection(int sock, volatile int* tb_alloc_tbl) {
	BEGIN_SINGLE_THREAD_PART {
		tb_alloc_tbl[blockIdx.x] = -1;
	} END_SINGLE_THREAD_PART;
}


#endif
