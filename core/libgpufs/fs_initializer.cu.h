#ifndef FS_INITIALIZER_H
#define  FS_INITIALIZER_H


//#ifndef MAIN_FS_FILE
//#error "This file must be included in the fs.cu"
//#endif

#include <stdio.h>

#include "fs_constants.h"
#include "fs_debug.cu.h"
#include "util.cu.h"
#include "cpu_ipc.cu.h"
#include "mallocfree.cu.h"
#include "fs_structures.cu.h"
#include "timer.h"
#include "hash_table.cu.h"
#include "radix_tree.cu.h"
#include "preclose_table.cu.h"
#include "fs_globals.cu.h"
#include "gpufs_lib.h"

extern "C" {
#include <rdma/gpu.h>
}

__global__ void init_fs(volatile CPU_IPC_OPEN_Queue* _ipcOpenQueue, 
			volatile CPU_IPC_RW_Queue* _ipcRWQueue, 
			volatile GPU_IPC_RW_Manager* _ipcRWManager, 
			volatile OTable* _otable, 
			volatile PPool* _ppool, 
			volatile Page* _rawStorage,
			volatile FTable* _ftable,
			volatile void* _rtree_raw_store,
			rtree*volatile _rtree_array,
			volatile preclose_table* _preclose_table);

#ifndef GPU_DIRECT_SHMEM
#define  initGpuShmemPtr(T, h_ptr,symbol)\
{\
 	CUDA_SAFE_CALL(cudaHostAlloc((void**)&(h_ptr), sizeof(T), cudaHostAllocMapped));\
	memset((void*)h_ptr,0,sizeof(T));\
	void* d_ptr;\
	CUDA_SAFE_CALL(cudaHostGetDevicePointer((void**)(&d_ptr), (void*)(h_ptr), 0));\
	CUDA_SAFE_CALL(cudaMemcpyToSymbol((symbol),&d_ptr,sizeof(void*)));\
}
#else
#define  initGpuShmemPtr(T, h_ptr,symbol)\
{\
	void* d_ptr;\
 	CUDA_SAFE_CALL(cudaMalloc((void**)&(d_ptr), sizeof(T)));	\
	h_ptr = (T*)gpu_mmap(d_ptr, sizeof(T));						\
	memset((void*)h_ptr,0,sizeof(T));\
	CUDA_SAFE_CALL(cudaMemcpyToSymbol((symbol),&d_ptr,sizeof(void*)));\
}
#endif

#define initGpuGlobals(T,d_ptr,symbol)\
{\
	CUDA_SAFE_CALL(cudaMalloc((void**)&(d_ptr),sizeof(T)));\
	CUDA_SAFE_CALL(cudaMemset((void*)d_ptr,0,sizeof(T)));\
	CUDA_SAFE_CALL(cudaMemcpyToSymbol((symbol),&(d_ptr),sizeof(void*)));\
}

struct GPUStreamManager
{
// GPU streams 	
	GPUStreamManager(){
		
		CUDA_SAFE_CALL(cudaStreamCreate(&kernelStream));
		for(int i=0;i<RW_IPC_SIZE;i++){
			CUDA_SAFE_CALL(cudaStreamCreate(&memStream[i]));
			CUDA_SAFE_CALL(cudaHostAlloc(&scratch[i], FS_BLOCKSIZE,cudaHostAllocDefault));
			task_array[i]=-1;
		}
	}

	~GPUStreamManager(){
		CUDA_SAFE_CALL(cudaStreamDestroy(kernelStream));
		for(int i=0;i<RW_IPC_SIZE;i++){
			CUDA_SAFE_CALL(cudaStreamDestroy(memStream[i]));
			cudaFreeHost(scratch[i]);
		}
	}
		
	cudaStream_t kernelStream;
	cudaStream_t memStream[RW_IPC_SIZE];
	
	int task_array[RW_IPC_SIZE];
	uchar* scratch[RW_IPC_SIZE];
};

struct GPUGlobals{
	volatile CPU_IPC_OPEN_Queue* cpu_ipcOpenQueue;
	
	volatile CPU_IPC_RW_Queue* cpu_ipcRWQueue; 
// RW GPU manager
	GPU_IPC_RW_Manager* ipcRWManager;
// Open/Close table
	OTable* otable;
// Memory pool
	PPool* ppool;
// File table with block pointers
	FTable* ftable;
// Raw memory pool
	Page* rawStorage;
// gpufs device file decsriptor
        int gpufs_fd;
// Raw memory pool for radix tree
	void* rtree_pool;
// Memory for radix tree array in all file descriptors
	rtree* rtree_array;

// preclose table:
	preclose_table* _preclose_table;
// Streams
	GPUStreamManager* streamMgr;

	GPUGlobals()
	{
		initGpuShmemPtr(CPU_IPC_OPEN_Queue,cpu_ipcOpenQueue,g_cpu_ipcOpenQueue);
		cpu_ipcOpenQueue->init_host();

		initGpuShmemPtr(CPU_IPC_RW_Queue,cpu_ipcRWQueue,g_cpu_ipcRWQueue);
		cpu_ipcRWQueue->init_host();

		initGpuGlobals(GPU_IPC_RW_Manager,ipcRWManager,g_ipcRWManager);
		initGpuGlobals(OTable,otable,g_otable);
		initGpuGlobals(PPool,ppool,g_ppool);
		initGpuGlobals(FTable,ftable,g_ftable);
		initGpuGlobals(preclose_table,_preclose_table,g_preclose_table);
	
		CUDA_SAFE_CALL(cudaMalloc(&rawStorage,sizeof(Page)*PPOOL_FRAMES));
		CUDA_SAFE_CALL(cudaMemset(rawStorage,0,sizeof(Page)*PPOOL_FRAMES));

		CUDA_SAFE_CALL(cudaMalloc(&rtree_pool,sizeof(rt_node)*MAX_RT_NODES));
		CUDA_SAFE_CALL(cudaMemset(rtree_pool,0,sizeof(rt_node)*MAX_RT_NODES));
		
		CUDA_SAFE_CALL(cudaMalloc(&rtree_array,sizeof(rtree)*(MAX_NUM_FILES + MAX_NUM_CLOSED_FILES)));
		CUDA_SAFE_CALL(cudaMemset(rtree_array,0,sizeof(rtree)*(MAX_NUM_FILES + MAX_NUM_CLOSED_FILES)));

		streamMgr=new GPUStreamManager();
		gpufs_fd=-1;
		if (getenv("USE_GPUFS_DEVICE")){
                  //gpufs_fd=gpufs_open(GPUFS_DEV_NAME);
	                if (gpufs_fd<0) {
	                        perror("gpufs_open failed");
			}
                }else{
			fprintf(stderr,"Warning: GPUFS device was not enabled through USE_GPUFS_DEVICE environment variable\n");
		}


	}
	
	~GPUGlobals()
	{
		if (gpufs_fd>=0){
		  if ( gpufs_close(gpufs_fd))
	          {
        	          perror("gpufs_close failed");
                  }
		}
		cudaFreeHost((void*)cpu_ipcOpenQueue);
		cudaFreeHost((void*)cpu_ipcRWQueue);
		
		cudaFree(ipcRWManager);
		cudaFree(otable);
		cudaFree(ppool);
		cudaFree(ftable);
		cudaFree(rawStorage);
		cudaFree(rtree_pool);
		cudaFree(rtree_array);
		cudaFree(_preclose_table);
		delete streamMgr;
	}
};
typedef volatile GPUGlobals* GPUGlobals_ptr;

void initializer(GPUGlobals_ptr* globals);
#endif // FS_INITIALIZER_H
