#include <cuda.h>
#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include "util.cu"

#if 1
#define MIN_BUFFER_SIZE		(1)
#define MAX_BUFFER_SIZE		(8192)
#define MAX_THREAD_NUMBER_SHIFT	(10)
#define MAX_THREAD_NUMBER	(1 << MAX_THREAD_NUMBER_SHIFT)
#define MAX_OFFSET		15
#else
#define MIN_BUFFER_SIZE		(60)
#define MAX_BUFFER_SIZE		(64)
#define MAX_THREAD_NUMBER_SHIFT	(0)
#define MAX_THREAD_NUMBER	(1 << MAX_THREAD_NUMBER_SHIFT)
#define MAX_OFFSET		15
#endif

namespace {
	char* src;
	char* dst;
	unsigned char* csrc;
	unsigned char* cdst;
	size_t bsize;
};

void init(size_t size)
{
	cudaMalloc(&src, size);
	cudaMalloc(&dst, size);
	csrc = (unsigned char*)malloc(size);
	cdst = (unsigned char*)malloc(size);
	bsize = size;
}

void fini()
{
	cudaFree(src);
	cudaFree(dst);
	free(cdst);
	free(csrc);
}

#define SRCMAGIC	0xDE
#define DSTMAGIC 	0xAC

__device__ char extract_number(int idx)
{
	unsigned int number = 1 + idx / 4;
	unsigned int pos = idx % 4;
	return (number >> pos) & 0xff;
}

__global__
void bufferset(char* src, size_t nbyte, size_t totalsize, int offsetA)
{
	int i = 0;
	for(; i < offsetA; i++)
		src[i] = SRCMAGIC;
	for(; i < nbyte + offsetA; i++)
		src[i] = extract_number(i - offsetA);
	for(; i < totalsize; i++)
		src[i] = SRCMAGIC;
}

__global__
void bufferclean(char* dst, size_t totalsize)
{
	for(int i = 0; i < totalsize; i++)
		dst[i] = DSTMAGIC;
}

__global__ void copy(char* dst, char* src, size_t nbyte, int oA, int oB)
{
	copy_block((uchar*)dst + oB, (volatile uchar*)src + oA, nbyte);
}

typedef void (*copy_t)(char* src, char* dst, size_t nbyte, int oA, int oB);

void dumpbyte(void* ib, size_t nbyte)
{
	unsigned char* buf = (unsigned char*)ib;
	for(int i = 0; i < nbyte; i++)
		printf("%02X ", buf[i]);
	printf("\n");
}

bool check(const char* name, copy_t copy_func, int nth, size_t nbyte, int offsetA, int offsetB)
{
	const char* errinfo = "\x1b[32mNO ERROR\x1b[0m";
	bufferset<<<1, 1>>>(src, nbyte, bsize, offsetA);
	bufferclean<<<1, 1>>>(dst, bsize);
	cudaDeviceSynchronize();
	copy_func<<<1, nth>>>(dst, src, nbyte, offsetA, offsetB);
	memset(csrc, 0x0, bsize);
	memset(cdst, 0x0, bsize);
	cudaMemcpy(csrc, src, bsize, cudaMemcpyDeviceToHost);
	cudaMemcpy(cdst, dst, bsize, cudaMemcpyDeviceToHost);
	if (memcmp(csrc + offsetA, cdst + offsetB, nbyte)) {
		errinfo = "\x1b[33mCorryptcopy\x1b[0m";
		goto out;
	}
	for(int i = 0; i < offsetB; i++)
		if (cdst[i] != DSTMAGIC) {
			printf("\nOvercopy, idx: %d, content %03X\n", i, (unsigned int)(unsigned char)cdst[i]);
			errinfo = "\x1b[33mOvercopy\x1b[0m";
			goto out;
		}
	for(int i = offsetB + nbyte; i < bsize; i++)
		if (cdst[i] != DSTMAGIC) {
			printf("\nOvercopy, idx: %d, content %03X\n", i, (unsigned int)(unsigned char)cdst[i]);
			errinfo = "\x1b[33mOvercopy\x1b[0m";
			goto out;
		}
	return false;
out:
	printf("\nFunction '%s' Error in "
		"%6d threads, "
		"%8lu bytes, "
		"offset %d "
		"and %d "
		"copy: %s\n",
			name,
			nth,
			nbyte,
			offsetA,
			offsetB,
			errinfo);
#if 0
	printf("Dump src\n");
	dumpbyte(csrc, bsize);
	printf("Dump dst\n");
	dumpbyte(cdst, bsize);
#endif
	return true;
}

static void printprog(int prog)
{
	int microprog = prog % 100;
	prog /= 100;
	printf("\r%3d.%02d%%[", prog, microprog);
	for(int i = 0; i < 100; i++)
		if (i < prog)
			printf("=");
		else if (i == prog && microprog > 50)
			printf("-");
		else
			printf(" ");
	printf("]");
	fflush(stdout);
}

int main()
{
	init(MAX_BUFFER_SIZE * 2L);
	long icase = 0;
	long lastprog = -1;
	long totalprog = MAX_THREAD_NUMBER_SHIFT + 1;
	totalprog *= MAX_BUFFER_SIZE - MIN_BUFFER_SIZE + 1;
	totalprog *= MAX_OFFSET + 1;
	totalprog *= MAX_OFFSET + 1; // Multiple *= in case of int32_t overflow
	long currentprog;
	bool noerror = true;

	for(size_t size = MIN_BUFFER_SIZE; size <= MAX_BUFFER_SIZE; size++)
		for(size_t nthread = 1; nthread <= MAX_THREAD_NUMBER; nthread *= 2)
			for (int i = 0; i <= MAX_OFFSET; i++)
				for (int j = 0; j <= MAX_OFFSET; j++)
				{
					bool err = check("copy_block", copy, nthread, size, i, j);
					icase++;
					currentprog = 10000L*icase/totalprog;
					if (err)
						noerror = false;
					if (err || currentprog != lastprog) {
						printprog(currentprog);
						lastprog = currentprog;
					}
				}
	printf("\n");
	if (noerror)
		printf("\x1b[32mNO ERROR\x1b[0m\n");
	fini();
}
