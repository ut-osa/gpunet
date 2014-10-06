#include "net_globals.h"
#include "net_structures.cu.h"
#include "hostloop.h"
#include "net_private.h"
#include "gpu_ipc.cu.h"
#include "ipc_shared.h"
#include "util.cu.h"

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include <cuda.h>
#include <stdio.h>
#include <errno.h>

#include <bitset>

__device__ STable* g_soctable; // all sockets

#ifdef USE_G_BUFFER_SPACE
__device__ volatile uchar* g_buffer_space; // all the sysbuf
#endif

__device__ gpu_ipc_manager* g_ipc_manager; //gpu IPC manager

__device__ cpu_ipc_queue* g_cpu_ipc_queue; //cpu-gpu shared space


#define  initGpuShmemPtr(T, h_ptr,symbol)                               \
	{																	\
		CUDA_SAFE_CALL(cudaHostAlloc((void**)&(h_ptr), sizeof(T), cudaHostAllocMapped)); \
		memset((void*)h_ptr,0,sizeof(T));								\
		void* d_ptr;													\
		CUDA_SAFE_CALL(cudaHostGetDevicePointer((void**)(&d_ptr), (void*)(h_ptr), 0)); \
		CUDA_SAFE_CALL(cudaMemcpyToSymbol((symbol),&d_ptr,sizeof(void*))); \
	}

#define initGpuGlobals(T,d_ptr,symbol)                                  \
	{																	\
		CUDA_SAFE_CALL(cudaMalloc((void**)&(d_ptr),sizeof(T)));			\
		CUDA_SAFE_CALL(cudaMemset((void*)d_ptr,0,sizeof(T)));			\
		CUDA_SAFE_CALL(cudaMemcpyToSymbol((symbol),&(d_ptr),sizeof(void*))); \
	}

__global__ void init_net()
{
	g_soctable->init();
	g_ipc_manager->init();
}

#define NR_SYSBUF_CHUNK (SOC_TABLE_SIZE*2)

#define TOTAL_SYSBUF_LEN (SOC_TABLE_SIZE*2*CPU_IO_BUF_SIZE)


GPUNETGlobals::GPUNETGlobals(int gpudev) {
	//CUDA_SAFE_CALL(cudaSetDevice(gpudev));
	//CUDA_SAFE_CALL(cudaSetDeviceFlags(cudaDeviceMapHost));

	initGpuShmemPtr(cpu_ipc_queue,_h_ipc_queue,g_cpu_ipc_queue);

	initGpuGlobals(STable,_g_soctable,g_soctable);
#ifdef USE_G_BUFFER_SPACE
	initGpuGlobals(char[TOTAL_SYSBUF_LEN],_g_buffer_space,g_buffer_space);
#endif
	initGpuGlobals(gpu_ipc_manager,_g_ipc_manager,g_ipc_manager);
	priv_ = new gpunet_private;

	init_net<<<1,1>>>();
	CUDA_SAFE_CALL(cudaThreadSynchronize());
	// start the GPU handling thread

	if (init_hostloop(this)){
		perror("Failed to start host loop thread");
		exit(-1);
	}
}

GPUNETGlobals::~GPUNETGlobals(){
	delete priv_;
#ifdef USE_G_BUFFER_SPACE
	CUDA_SAFE_CALL(cudaFree(_g_buffer_space));
#endif
	CUDA_SAFE_CALL(cudaFree(_g_soctable));
	CUDA_SAFE_CALL(cudaFree(_g_ipc_manager));
	CUDA_SAFE_CALL(cudaFreeHost(_h_ipc_queue));
}

void GPUNETGlobals::register_kernel(const char* fname, const void* func)
{
	(void)priv_->register_kernel(fname, func);
}

