#ifndef GPU_IPC_H
#define GPU_IPC_H

#include "net_constants.h"
#include "ipc_shared.h"
#include "util.cu.h"
#include <assert.h>

__device__ void clean_cpu_ipc_entry_gpu(cpu_ipc_entry* e);


/** GPU manager to prevent scanning through CPU memory for empty slots*/
struct gpu_ipc_manager
{
	volatile int _locker[TASK_ARRAY_SIZE];
	volatile int _lock;
	volatile int _last;
	__device__ int findEntry();
	__device__ void freeEntry(int entry);
	__device__ void init();
};

/* assuming single TB per socket */

__device__ inline void fire_async(cpu_ipc_entry* e) {
	__threadfence_system();
	GPU_ASSERT(e->status==CPU_IPC_READY);
	e->status=CPU_IPC_PENDING;
	__threadfence_system();
}

__device__ inline int wait_for_ipc(cpu_ipc_entry* e) {
	WAIT_ON_MEM(e->status,CPU_IPC_READY);
	return  readNoCache(&e->ret_val);
}

__device__ inline int fire_and_wait(cpu_ipc_entry* e){
	fire_async(e);
	return wait_for_ipc(e);
}

__device__ inline int peek_the_hole(cpu_ipc_entry* e)
{
	return readNoCache(&(e->status)) == CPU_IPC_READY;
}

#define POKE_QUEUE_SLOT(slot) 	(&g_cpu_ipc_queue->entries[slot])

#define GET_QUEUE_SLOT(entry, e) {              \
		(entry)=g_ipc_manager->findEntry();		\
		assert(entry>=0);						\
		(e)=&g_cpu_ipc_queue->entries[entry];	\
		assert(e->status!=CPU_IPC_PENDING);}	\


#endif
