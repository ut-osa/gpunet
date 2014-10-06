#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <cuda_runtime.h>

extern "C" {
#include <rdma/gpu.h>
}

#define DEV_PATH "/dev/gpu_usermap"

#define MAP_LEN 7 * (1 << 12)

#define unlikely(x)  __builtin_expect(!!(x), 0)

#define ASSERT_CUDA(val)												\
    if(unlikely((val))) {fprintf(stderr, __FILE__, __LINE__, "ERROR: errno = %3d : %s\n", static_cast<int>(val), cudaGetErrorString(val)); exit(1);}

void fill_mem(volatile char* addr) {
	for (int i = 0; i < MAP_LEN; i++) {
		addr[i] = 'a' + (i % 26);
	}
}

bool check_mem(char *addr) {
	for (int i = 0; i < MAP_LEN; i++) {
		if (addr[i] != 'a' + (i % 26)) {
			fprintf(stderr, "ERROR: %d th character differs (%c instead of %c)\n",
					i, addr[i], 'a' + (i % 26));
			return false;
		}
	}
	return true;
}

#define NR_ITER 32
int main() {
	void *devptr, *hostptr;

	for (int i = 0; i < NR_ITER; i++) {
		ASSERT_CUDA(cudaMalloc(&devptr, MAP_LEN));
		volatile char* addr = (char*)gpu_mmap(devptr, MAP_LEN);
		if (addr == MAP_FAILED) {
			perror("gpu_mmap");
			fprintf(stderr, "gpu mmap failed (iteration %d)\n", i);
			exit(1);
		}
		fill_mem(addr);

		ASSERT_CUDA(cudaMallocHost(&hostptr, MAP_LEN));
		ASSERT_CUDA(cudaMemcpy(hostptr, devptr, MAP_LEN, cudaMemcpyDeviceToHost));
		if (check_mem((char*)hostptr)) {
			printf("SUCCESS %d out of %d\n", i, NR_ITER);
		} else {
			printf("FAILED  %d out of %d\n", i, NR_ITER);
		}
	}

	return 0;
}
