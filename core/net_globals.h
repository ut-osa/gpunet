#ifndef NET_GLOBALS
#define NET_GLOBALS

#include "net_constants.h"
#include <cuda_runtime.h>
#include <pthread.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <infiniband/ib.h>

class STable;
class gpu_ipc_manager;
class cpu_ipc_queue;
class gpunet_private;
struct cpu_ipc_entry;

struct  GPUNETGlobals{

	// CPU storage for GPU data structure pointers

	cpu_ipc_queue * _h_ipc_queue;

	STable * _g_soctable;

	gpu_ipc_manager* _g_ipc_manager;

	// sysbuf-related GPU variables
	uchar * _g_buffer_space;

	// sysbuf-related CPU variables
	struct ibv_pd *_g_pd;
	struct ibv_mr *_g_mr_buf_space;

	/////constructor/destructor

	GPUNETGlobals(int gpudev);

	~GPUNETGlobals();

	int start_hostloop(cudaStream_t kernel_stream);
	int single_hostloop(cudaStream_t kernel_stream);
	int poll_gsend();

	uintptr_t alloc_sysbuf();
	void dealloc_sysbuf(uintptr_t);

	void register_sysbuf(struct ibv_pd* pd);
	void register_kernel(const char* fname, const void* func);
private:
	gpunet_private *priv_;
	void ipc_service(cpu_ipc_entry* e, int task_array_slot);
	void ipc_kernel_sync_exec(cpu_ipc_entry* e, int slot);
	void ipc_kernel_async_exec(cpu_ipc_entry* e, int slot);
};

#endif
