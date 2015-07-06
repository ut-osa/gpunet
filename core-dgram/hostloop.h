#ifndef host_loop_h
#define host_loop_h
#include <pthread.h>
#include <cuda_runtime.h>
class GPUNETGlobals;

int init_hostloop(GPUNETGlobals* g);

#endif
