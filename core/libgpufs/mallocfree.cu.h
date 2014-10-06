#ifndef MALLOCFREE_CU_H
#define MALLOCFREE_CU_H
#include "fs_constants.h"
#include "fs_structures.cu.h"

#define FREE_LOCKED 1
#define FREE_UNLOCKED 0
struct PPool{
	
 	volatile Page* rawStorage;

	volatile PFrame frames[PPOOL_FRAMES];

	volatile uint freelist[PPOOL_FRAMES];
	volatile uint lock;
	volatile uint head;

// MUST be called from a single thread
	__device__  void init_thread(volatile Page* _storage) volatile;
	
	__device__ volatile PFrame *allocPage() volatile;

	__device__ void freePage(volatile PFrame* frame, bool lock) volatile ;
};


/* radix tree memory pool */
struct rt_mempool
{
	volatile void* raw_storage;
	volatile void* freelist[MAX_RT_NODES];
	volatile int head;
	volatile int lock;
	int blocksize;
	/* init by providing the raw storage, having MAX_RT_NODES x blocksize bytes */
	__device__ void init_thread(volatile void * raw_storage,int blocksize) volatile;

	/* allocate new node of size $blocksize */
	__device__ volatile void* allocNode() volatile;
	/* free node - also bzeros the data */
	__device__ void freeNode(volatile void*) volatile;
};


#endif
