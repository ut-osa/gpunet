

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
#include "fs_initializer.cu.h"

__device__ int g_file_id;

#define DEVICE_VOLATILE __device__ volatile
// CPU Write-shared memory //
DEVICE_VOLATILE CPU_IPC_OPEN_Queue* g_cpu_ipcOpenQueue;
DEVICE_VOLATILE CPU_IPC_RW_Queue* g_cpu_ipcRWQueue;
//
// manager for rw RPC queue
DEVICE_VOLATILE GPU_IPC_RW_Manager* g_ipcRWManager;
// Open/Close table
DEVICE_VOLATILE OTable* g_otable;
// Memory pool
DEVICE_VOLATILE PPool* g_ppool;
// File table with block pointers
DEVICE_VOLATILE FTable* g_ftable;
// Radix tree memory pool for rt_nodes
DEVICE_VOLATILE rt_mempool g_rtree_mempool;
// Hash table with all the previously opened files indexed by their inodes
DEVICE_VOLATILE hash_table g_closed_ftable;
//pre close table
DEVICE_VOLATILE preclose_table* g_preclose_table;

__global__ void init_fs(volatile CPU_IPC_OPEN_Queue* _ipcOpenQueue, 
			volatile CPU_IPC_RW_Queue* _ipcRWQueue, 
			volatile GPU_IPC_RW_Manager* _ipcRWManager, 
			volatile OTable* _otable, 
			volatile PPool* _ppool, 
			volatile Page* _rawStorage,
			volatile FTable* _ftable,
			volatile void* _rtree_raw_store,
			rtree*volatile _rtree_array,
			volatile preclose_table* _preclose_table)
{
// this must be done from a single thread!

	
	g_cpu_ipcOpenQueue=_ipcOpenQueue;
	g_cpu_ipcRWQueue=_ipcRWQueue;

	g_ipcRWManager=_ipcRWManager;
	g_ipcRWManager->init_thread();

	g_otable=_otable; 
	g_otable->init_thread();

	g_ppool=_ppool;
	g_ppool->init_thread(_rawStorage);
	
	g_rtree_mempool.init_thread(_rtree_raw_store,sizeof(rt_node));
	
	g_ftable=_ftable;
	for(int i=0;i<MAX_NUM_FILES+MAX_NUM_CLOSED_FILES;i++)
	{
		_rtree_array[i].init_thread();
	}	
	// this takes MAX_FILES from _rtree_array 
	g_ftable->init_thread(_rtree_array);

	g_closed_ftable.init_thread(_rtree_array+ MAX_NUM_FILES);

	g_preclose_table=_preclose_table;
	g_preclose_table->init_thread();
	
	g_file_id=0;
	INIT_ALL_STATS
	//INIT_DEBUG
}


typedef volatile GPUGlobals* GPUGlobals_ptr;

void initializer(GPUGlobals_ptr* globals)
{
	CUDA_SAFE_CALL(cudaSetDeviceFlags(cudaDeviceMapHost));
	*globals=new GPUGlobals();


	init_fs<<<1,1>>>((*globals)->cpu_ipcOpenQueue,
                        (*globals)->cpu_ipcRWQueue,
                        (*globals)->ipcRWManager,
                        (*globals)->otable,
                        (*globals)->ppool,
                        (*globals)->rawStorage,
                        (*globals)->ftable,
			(*globals)->rtree_pool,
		 	(*globals)->rtree_array,
			(*globals)->_preclose_table);
	
	cudaThreadSynchronize();
	CUDA_SAFE_CALL(cudaPeekAtLastError());
}
