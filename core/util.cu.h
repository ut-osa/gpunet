/*
 * This expermental software is provided AS IS.
 * Feel free to use/modify/distribute,
 * If used, please retain this disclaimer and cite
 * "GPUfs: Integrating a file system with GPUs",
 * M Silberstein,B Ford,I Keidar,E Witchel
 * ASPLOS13, March 2013, Houston,USA
 */


#ifndef UTIL_CU_H
#define UTIL_CU_H

#include <stdint.h>

#ifdef COPY_PROFILE
#include <stdio.h>
#endif

#include "net_constants.h"
#include "net_debug.h"

#define WAIT_ON_MEM(mem,val)  while(readNoCache(&(mem))!=val);
#define WAIT_ON_MEM_NE(mem,val)  while(readNoCache(&(mem))==val);

//#define GPU_ASSERT(x) ({WRITE_DEBUG(__FILE__,__LINE__); (*(int*)NULL)=0;})
//#define GPU_ASSERT(x) if (!(x)){WRITE_DEBUG(__FILE__,__LINE__); \
//  __threadfence_system(); asm("trap;");}; //return;}

#ifndef RELEASE
#define GPU_ASSERT(x)   assert(x);
#else
#warning "Asserts disabled"
#define GPU_ASSERT(x)
#endif


#define MUTEX_LOCK(lock) while (atomicExch((int*)(&(lock)),1));
#define MUTEX_WAS_LOCKED(lock) atomicExch((int*)(&(lock)),1)

#define MUTEX_UNLOCK(lock) { atomicExch((int*)(&(lock)),0);}

#define FIRST_THREAD_IN_BLOCK() ((threadIdx.x + threadIdx.y + threadIdx.z) == 0)
#define FIRST_BLOCK() ( blockIdx.x + blockIdx.y + blockIdx.z == 0)

#define BEGIN_SINGLE_THREAD_PART __syncthreads(); if(FIRST_THREAD_IN_BLOCK()) { do
#define END_SINGLE_THREAD_PART while(0); } __syncthreads()

#define BEGIN_SINGLE_THREAD BEGIN_SINGLE_THREAD_PART {
#define END_SINGLE_THREAD  } END_SINGLE_THREAD_PART ;

#define BEGIN_UNIQUE_THREAD_PART __syncthreads(); if(FIRST_THREAD_IN_BLOCK() && FIRST_BLOCK()) { do
#define END_UNIQUE_THREAD_PART while(0); } __syncthreads()

#define TID (threadIdx.x+threadIdx.y*blockDim.x+threadIdx.z*blockDim.x*blockDim.y)
#define TBID (blockIdx.x+blockIdx.y*gridDim.x+blockIdx.z*gridDim.x*gridDim.y)
//#define TID (threadIdx.x)


__forceinline__ __device__ void bzero_thread(volatile void* dst, uint size)
{

	int bigsteps=size>>3;
	int i=0;
	for( i=0;i<bigsteps;i++)
		((double*)dst)[i]=0;
	bigsteps=bigsteps<<3;
	for (i=bigsteps;i<size;i++){
		((char*)dst)[i]=0;
	}

}
/*__forceinline__ __device__ void bzero_page(volatile char* dst){
  for(int i=TID;i<FS_BLOCKSIZE>>3;i+=blockDim.x*blockDim.y){
  ((volatile double*)dst)[i]=0;
  }
  }
*/
__forceinline__
__device__ void memcpy_thread(volatile void* _dst, const volatile void* _src, uint size)
{
	volatile char* dst = (volatile char*)_dst;
	volatile char* src = (volatile char*)_src;
	for(int i=0;i<size;i++)
		dst[i]=src[i];
}

__forceinline__ __device__ char strcmp_thread(volatile const char* dst, volatile const char* src, uint size)
{
	int i=0;
	for (i=0;i<size && dst[i]==src[i] ;i++)
	{
		if(dst[i]=='\0' ) return 0;
	}

	if (i == size ) return 0;

	return 1;
}

__forceinline__
__device__ int strncpy_thread(volatile char* dst, volatile const char* src, size_t n)
{
	int i = 0;
	while (i < n && *src) {
		*dst++ = *src++;
		i++;
	}
	*dst = '\0';
	return i;
}

__forceinline__ __device__ char readNoCache(const volatile char* ptr){
	char val;
	val=*ptr;
	//    asm("ld.cv.f64 %0, [%1];"  : "=d"(val):"l"(ptr));
	return val;
}

__forceinline__ __device__ double readNoCache(const volatile double* ptr){
	double val;
	val=*ptr;
	//    asm("ld.cv.f64 %0, [%1];"  : "=d"(val):"l"(ptr));
	return val;
}

__forceinline__ __device__ char2 readNoCache(const volatile uchar* ptr){
	char2 v;v.x=*ptr; v.y=*(ptr+1); return v;
	//    asm("ld.cv.u16 %0, [%1];"  : "=h"(val2):"l"(ptr));
	//    char2 n;
	//    n.x=(char)val2; n.y=(char)val2>>8;
	//          return n;
}
__forceinline__ __device__ unsigned int readNoCache(const volatile unsigned int* ptr){
	unsigned int val;
	val=*ptr;
	//    asm("ld.cv.u32 %0, [%1];"  : "=r"(val):"l"(ptr));
	return val;
}
__forceinline__ __device__ int readNoCache(const volatile int* ptr){
	int val;
	val=*ptr;
	//    asm("ld.cv.u32 %0, [%1];"  : "=r"(val):"l"(ptr));
	return val;
}
__forceinline__ __device__ size_t readNoCache(const volatile size_t* ptr){
	size_t val;
	val=*ptr;
	//    if (sizeof(size_t)==8)
	//    asm("ld.cv.f64 %0, [%1];"  : "=d"(val):"l"(ptr));
	//    else
	//    asm("ld.cv.u32 %0, [%1];"  : "=r"(val):"l"(ptr));
	return val;
}

__device__ void copy_block(uchar* dst, volatile uchar*src, int size);

__device__ void copy_block_src_volatile(uchar* dst, volatile uchar* src, int size);
__device__ void copy_block_dst_volatile(volatile uchar* dst, uchar* src, int size);


__forceinline__ __device__ void copyNoCache_block(uchar* dst, volatile uchar*src, int size)
{
	copy_block(dst,src,size);
}

__forceinline__ __device__ void copyNoCache_thread(char* dst, volatile char*src, int size)
{
	for(int i=0;i<size>>2;i++){
		((int*)dst)[i]=readNoCache((int*)(src)+i);
	}
}


__forceinline__ __device__ void write_thread(uchar* dst, uchar*src, int size)
{
	for(int i=0;i<size>>2;i++){
		((int*)dst)[i]=*((int*)(src)+i);
	}
}

__device__ inline size_t offset2block(size_t offset, int log_blocksize)
{
	return offset>>log_blocksize;
}
__device__ inline uint offset2blockoffset(size_t offset, int blocksize)
{
	return offset&(blocksize-1);
}

struct LAST_SEMAPHORE
{
	int count;
	__device__ int is_last() volatile
	{
		int ret = atomicAdd((int*)&count,1);
		if (ret == gridDim.x*gridDim.y*gridDim.z-1) return 1;
		return 0;
	}
};

struct INIT_LOCK
{
	volatile int lock;
	__device__ int try_wait() volatile{
		int res=atomicMax((int*)&lock,1);
		if (res==0) {
			return 1; // locked now
		}
		while(lock!=2);
		return 0;
	}
	__device__ void signal() volatile{
		lock=2;
		__threadfence();
	}

};



#define GPU_ERROR(str) __assert_fail(str,__FILE__,__LINE__,__func__);

__device__ int getNewFileId();
#endif
