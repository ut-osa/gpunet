#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cuda_runtime.h>

#include "../gpu_usermap_abi.h"

#define DEV_PATH "/dev/gpu_usermap"

#define MAP_LEN 16384

#define unlikely(x)  __builtin_expect(!!(x), 0)

#define ASSERT_CUDA(val)												\
    if(unlikely((val))) {fprintf(stderr, __FILE__, __LINE__, "ERROR: errno = %3d : %s\n", static_cast<int>(val), cudaGetErrorString(val)); exit(1);}

void fill_mem(char* addr) {
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

template<int N>
__global__ void gpu_perf(volatile int* mem) {
	unsigned long t1, t2;

	mem[0] = 0;
	t1 = clock64();
	for (int i = 0; i < N; i++) {
		while (mem[0] != 2*i);
		mem[0] = 2*i + 1;
		//__threadfence_system();
	}
	t2 = clock64();
	printf("clock per update: %ld\n", (t2-t1)/N);
}

int hostptr_gpudirect(volatile int** phostptr, volatile int** pdevptr) {
	int ret;
	gpu_usermap_req req;
	volatile int *devptr;

	int fd = open(DEV_PATH, O_RDWR);
	if (fd < 0) {
		perror("open");
		exit(1);
	}

	ASSERT_CUDA(cudaMalloc(&devptr, MAP_LEN));
	
	req.magic = GUSERMAP_MAGIC;
	req.gpu_addr = (uintptr_t)devptr;
	req.len = MAP_LEN;

	ret = write(fd, &req, sizeof(req));
	if (ret != sizeof(req)) {
		perror("write");
		fprintf(stderr, "ERROR: write ret: %d expected %d\n", ret, sizeof(req));
		exit(1);
	}
	
	volatile int *addr = (volatile int*)mmap(NULL, MAP_LEN, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED) {
		perror("mmap failed");
		exit(1);
	}

	*phostptr = addr;
	*pdevptr = devptr;

	return 0;
}

int hostptr_zerocopy(volatile int** phostptr, volatile int** pdevptr) {

    ASSERT_CUDA(cudaHostAlloc((void**)phostptr, MAP_LEN, cudaHostAllocMapped));
    ASSERT_CUDA(cudaHostGetDevicePointer((void**)pdevptr, (void*)*phostptr, 0));
	
	return 0;
}

void usage() {
	fprintf(stderr, "Usage: ./test_gpu_usermap_perf [case]\n");
	fprintf(stderr, "  case 0 -- gpudirect\n");
	fprintf(stderr, "  case 1 -- zerocopy\n");
}

int main(int argc, char** argv) {
	volatile int* hostptr;
	volatile int *devptr;
	int ret;

	if (argc < 2) {
		usage();
		exit(1);
	}

	if (argv[1][0] == '0') {
		ret = hostptr_gpudirect(&hostptr, &devptr);
	} else if (argv[1][0] == '1') {
		ret = hostptr_zerocopy(&hostptr, &devptr);
	} else {
		usage();
		exit(1);
	}

	if (ret) {
		fprintf(stderr, "hostptr allocation failed ret: %d\n", ret);
		exit(1);
	}

#define NR_ITEMS 1000000

	gpu_perf<NR_ITEMS><<<1, 1>>>(devptr);

	timeval tv1, tv2, tv3;
	for (int i = 0; i < NR_ITEMS; i++) {
		if (i == 1)
			gettimeofday(&tv1, NULL);
			
		while (hostptr[0] != 2*i+1);
		hostptr[0] = 2*i+2;
	}
	gettimeofday(&tv2, NULL);
	
	ASSERT_CUDA(cudaDeviceSynchronize());

	munmap((void*)hostptr, MAP_LEN);

	// int idx_second_non_zero = 0, idx_last;

	// for (int i = 0; i < NR_ITEMS; i++) {
	// 	if (arr[i] != 0 && idx_second_non_zero == 0)
	// 		idx_second_non_zero = i+1;
	// 	if (arr[i] >= (NR_ITEMS - 1)) {
	// 		idx_last = i;
	// 		break;
	// 	}
	// }

	// printf("idx difference: %d avg interval: %d\n",
	// 	   (idx_last - idx_second_non_zero),
	// 	   NR_ITEMS / (idx_last - idx_second_non_zero));

	timersub(&tv2, &tv1, &tv3);

	double t_ms = (tv3.tv_sec*1000.0 + tv3.tv_usec/1000.0);
	printf("total time: %.2f ms, for each pingpong: %2f us \n", t_ms, t_ms*1000/(NR_ITEMS-1));
	
	return 0;
}
