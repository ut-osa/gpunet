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

#include <ringbuf.cu.h>
extern "C" {
#include <rdma/ringbuf.h>
}


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
__global__ void gpu_perf(g_ringbuf::ringbuf* rb) {
	unsigned long t1, t2;

	t1 = clock64();
	for (int i = 0; i < N; i++) {
		g_ringbuf::ringbuf_produce(rb, 1);
	}
	t2 = clock64();
	printf("clock per update: %ld\n", (t2-t1)/N);
	printf("dev size: %ld\n", sizeof(*rb));
}

int ringbuf_gpudirect(ringbuf_t* phostptr, g_ringbuf::ringbuf** pdevptr) {
	int ret;
	gpu_usermap_req req;
	void *devptr;

	struct cuda_dev* dev = gpu_init(BEST_GPU);

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
	
	ringbuf_t addr = (ringbuf_t)mmap(NULL, MAP_LEN, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
	if (((void*)addr) == MAP_FAILED) {
		perror("mmap failed");
		exit(1);
	}

	addr->_size = MAP_LEN;
	fprintf(stderr, "ringbuf size: %ld\n", addr->_size);
	addr->_buf = (uint8_t*)gpu_malloc(dev, MAP_LEN);
	if (!addr->_buf) {
		fprintf(stderr, "gpu_malloc failed to allocate\n");
		exit(1);
	}
	ringbuf_reset(addr);
	
	*phostptr = addr;
	*pdevptr = (g_ringbuf::ringbuf*)devptr;

	return 0;
}

int ringbuf_zerocopy(ringbuf_t* phostptr, g_ringbuf::ringbuf** pdevptr) {

	struct cuda_dev* dev = gpu_init(BEST_GPU);
	*phostptr = ringbuf_gpu_new(dev, MAP_LEN, (devptr*)pdevptr);
	
	return 0;
}

void usage() {
	fprintf(stderr, "Usage: ./test_gpu_usermap_ringbuf [case]\n");
	fprintf(stderr, "  case 0 -- gpudirect\n");
	fprintf(stderr, "  case 1 -- zerocopy\n");
}

int main(int argc, char** argv) {
	ringbuf_t hostptr;
	g_ringbuf::ringbuf *devptr;
	int ret;

	if (argc < 2) {
		usage();
		exit(1);
	}

	if (argv[1][0] == '0') {
		ret = ringbuf_gpudirect(&hostptr, &devptr);
	} else if (argv[1][0] == '1') {
		ret = ringbuf_zerocopy(&hostptr, &devptr);
	} else {
		usage();
		exit(1);
	}

	if (ret) {
		fprintf(stderr, "hostptr allocation failed ret: %d\n", ret);
		exit(1);
	}

#define NR_ITEMS 1000000

	hostptr->_size = MAP_LEN;
	
	gpu_perf<NR_ITEMS><<<1, 1>>>(devptr);

	timeval tv1, tv2, tv3;
	gettimeofday(&tv1, NULL);

	size_t sum = 0;
	size_t to_consume;
	while (sum < NR_ITEMS) {
		to_consume = ringbuf_bytes_used(hostptr);

		if (to_consume > 0) {
			ringbuf_consume(hostptr, to_consume);
		}

		sum += to_consume;
	}
	
	ASSERT_CUDA(cudaDeviceSynchronize());
	gettimeofday(&tv2, NULL);

	//munmap((void*)hostptr, MAP_LEN);

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
