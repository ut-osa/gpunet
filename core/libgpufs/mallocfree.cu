#ifndef MALLOCFREE_CU
#define MALLOCFREE_CU

#include "fs_constants.h"
#include "fs_debug.cu.h"
#include "util.cu.h"
#include "mallocfree.cu.h"
#include "swapper.cu.h"
#include <assert.h>


// MUST be called from a single thread
DEBUG_NOINLINE __device__  void PPool::init_thread(volatile Page* _storage) volatile
{
	rawStorage=_storage;
	head=0;
	lock=0;
	for(int i=0;i<PPOOL_FRAMES;i++)
	{
		frames[i].init_thread(&rawStorage[i],i);
		freelist[i]=i;
	}
}
	
// TODO: lock free datastructure would be better
DEBUG_NOINLINE __device__ volatile PFrame* PPool::allocPage() volatile
{
	
	volatile PFrame* frame;
	MUTEX_LOCK(lock);
	MALLOC

	if (head==PPOOL_FRAMES) { 
		if (swapout(MIN_PAGES_SWAPOUT)== MIN_PAGES_SWAPOUT)
		{
			// TODO: error handling
			// we failed to swap out
			GPU_ASSERT(NULL);
		}

	}
	frame=&frames[freelist[head]];
	head++;
	__threadfence();
	MUTEX_UNLOCK(lock);
	return frame;
}

DEBUG_NOINLINE __device__ void PPool::freePage(volatile PFrame* frame, bool locked) volatile 
{

	if (frame == NULL) return;
//DEBUG
//	GPU_ASSERT(0); // 

	if (locked==FREE_LOCKED) MUTEX_LOCK(lock);
	FREE
	GPU_ASSERT(head>0);
	head--;
	frame->clean();
	freelist[head]=frame->rs_offset;
	__threadfence();
	if (locked==FREE_LOCKED) MUTEX_UNLOCK(lock);
}


DEBUG_NOINLINE __device__ void rt_mempool::init_thread(volatile void* raw_storage, int datasize) volatile
{
	GPU_ASSERT(raw_storage);
	this->raw_storage=raw_storage;
	this->head=0;
	this->lock=0;
	this->blocksize=datasize;
	for(int i=0;i<MAX_RT_NODES;i++)
	{
		freelist[i]=((char*)raw_storage)+i*datasize;
	}
}

DEBUG_NOINLINE __device__ volatile void* rt_mempool::allocNode() volatile
{
	RT_MALLOC
	MUTEX_LOCK(lock);
	if (head==MAX_RT_NODES) return NULL;
	volatile void* to_return=freelist[head++];
	__threadfence();
	MUTEX_UNLOCK(lock);
	return to_return;
}
DEBUG_NOINLINE __device__ void rt_mempool::freeNode(volatile void* node) volatile
{
	RT_FREE

	if (node==NULL) return;
	GPU_ASSERT(head!=0);
	MUTEX_LOCK(lock);
	head--;
	bzero_thread(node, blocksize);
	freelist[head]=node;
	 __threadfence();
	MUTEX_UNLOCK(lock);
}
#endif
