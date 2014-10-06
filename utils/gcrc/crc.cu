#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <cuda.h>
#include <stdint.h>
#include <time.h>
#include "crc.hpp"
#include "sse4crc.h"

#define DATA_SIZE		(1048576+0xf)
#define PAGE_SIZE		1024

double delta(timespec s, timespec e)
{
	double ds = s.tv_sec * 1e9 + s.tv_nsec;
	double de = e.tv_sec * 1e9 + e.tv_nsec;
	double d = de - ds;
	return d/1e6;
}


__global__ void pcrc32(const unsigned char* pbuf, size_t size, crc_t* ret)
{
	*ret = 0;
	__syncthreads();
	atomicXor(ret, dev_crc<UIntV4>(pbuf + PAGE_SIZE*blockIdx.x, PAGE_SIZE));
}

int main()
{
	char* ran = (char*)malloc(DATA_SIZE);
	for(int i = 0; i < DATA_SIZE; i++)
		ran[i] = random();
	unsigned char* gran;
	cudaMalloc(&gran, DATA_SIZE);
	cudaMemcpy(gran, ran, DATA_SIZE, cudaMemcpyHostToDevice);
	crc_t* gret;
	cudaMalloc(&gret, sizeof(crc_t));
	crc_t ret0, ret1, ret2, ret3;
	crc_t ret4;

	while (true) {
		timespec t0, t1, t2, t3, t4;
		timespec t5;
		timespec t6;
		timespec t7;

		clock_gettime(CLOCK_MONOTONIC, &t0);
		crc32b<<<1, 1>>>(gran, DATA_SIZE, gret);
		cudaMemcpy(&ret0, gret, sizeof(crc_t), cudaMemcpyDeviceToHost);

		clock_gettime(CLOCK_MONOTONIC, &t1);
		crc32t<UByte><<<1, 1>>>(gran, DATA_SIZE, gret);
		cudaMemcpy(&ret1, gret, sizeof(crc_t), cudaMemcpyDeviceToHost);

		clock_gettime(CLOCK_MONOTONIC, &t2);
		crc32t<UInt><<<1, 1>>>(gran, DATA_SIZE, gret);
		cudaMemcpy(&ret2, gret, sizeof(crc_t), cudaMemcpyDeviceToHost);

		clock_gettime(CLOCK_MONOTONIC, &t3);
		crc32t<UIntV4><<<1, 1>>>(gran, DATA_SIZE, gret);
		cudaMemcpy(&ret3, gret, sizeof(crc_t), cudaMemcpyDeviceToHost);

		clock_gettime(CLOCK_MONOTONIC, &t4);
		pcrc32<<<1, DATA_SIZE/PAGE_SIZE>>>(gran, DATA_SIZE, gret);
		cudaMemcpy(&ret4, gret, sizeof(crc_t), cudaMemcpyDeviceToHost);

		clock_gettime(CLOCK_MONOTONIC, &t5);
		crc_t cr = crc32(ran, DATA_SIZE, 0);
		clock_gettime(CLOCK_MONOTONIC, &t6);
		crc_t sse42cr = sse42_crc32(ran, DATA_SIZE, 0);
		clock_gettime(CLOCK_MONOTONIC, &t7);

		double d0 = delta(t0, t1);
		double d1 = delta(t1, t2);
		double d2 = delta(t2, t3);
		double d3 = delta(t3, t4);
		double d4 = delta(t4, t5);
		double d5 = delta(t5, t6);
		double d6 = delta(t6, t7);
		printf("crc32b         0x%X, time %f (ms), thpt %f MB/sec\n", ret0, d1,
				DATA_SIZE/1024.0/1024.0/(d0/1e3)
		);
		printf("crc32t<UByte>  0x%X, time %f (ms), thpt %f MB/sec\n", ret1, d1,
				DATA_SIZE/1024.0/1024.0/(d1/1e3)
		);
		printf("crc32t<UInt>   0x%X, time %f (ms), thpt %f MB/sec\n", ret2, d2,
				DATA_SIZE/1024.0/1024.0/(d2/1e3)
		);
		printf("crc32t<UIntV4> 0x%X, time %f (ms), thpt %f MB/sec\n", ret3, d3,
				DATA_SIZE/1024.0/1024.0/(d3/1e3)
		);
		printf("pcrc32t        0x%X, time %f (ms), thpt %f MB/sec\n", ret4, d4,
				DATA_SIZE/1024.0/1024.0/(d4/1e3)
		);
		printf("crc32          0x%X, time %f (ms), thpt %f MB/sec\n", cr, d5,
				DATA_SIZE/1024.0/1024.0/(d5/1e3)
		);
		printf("sse42crc32     0x%X, time %f (ms), thpt %f MB/sec\n", sse42cr, d6,
				DATA_SIZE/1024.0/1024.0/(d6/1e3)
		);
		printf("\n");
	}
}
