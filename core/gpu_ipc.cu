/*
 * This expermental software is provided AS IS.
 * Feel free to use/modify/distribute,
 * If used, please retain this disclaimer and cite
 * "GPUfs: Integrating a file system with GPUs",
 * M Silberstein,B Ford,I Keidar,E Witchel
 * ASPLOS13, March 2013, Houston,USA
 */


#ifndef CPU_IPC_CU
#define CPU_IPC_CU
#include <assert.h>
#include "net_constants.h"
#include "util.cu.h"
#include "ipc_shared.h"
#include "gpu_ipc.cu.h"
#include "net_structures.cu.h"
#include "net_globals_add.cu.h"

#include <cuda_runtime.h>

__device__ void clean_cpu_ipc_entry_gpu(cpu_ipc_entry* e)
{
	e->status=CPU_IPC_READY;
	__threadfence_system();
}

__device__ void gpu_ipc_manager::init()
{
	for(int i=0;i<TASK_ARRAY_SIZE;i++)
	{
		_locker[i]=IPC_MGR_EMPTY;
	}
	_lock=0;
}

__device__ int gpu_ipc_manager::findEntry()
{
	const int init = blockIdx.x & (TASK_ARRAY_SIZE - 1);
	int i;
	i = init; // assuming one concurrent call PER TB

	do {
		if (atomicExch((int*)&_locker[i], IPC_MGR_BUSY) == IPC_MGR_EMPTY) {
			return i;
		}
		i = (i + 1) & (TASK_ARRAY_SIZE - 1);
	} while (i != init);

	return -1;
}

__device__ void gpu_ipc_manager::freeEntry(int entry)
{
	assert(_locker[entry] == IPC_MGR_BUSY);
	_locker[entry] = IPC_MGR_EMPTY;
}

#endif
