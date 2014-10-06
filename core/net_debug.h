#ifndef net_debug_h
#define net_debug_h

#define CUDA_SAFE_CALL(x) if((x)!=cudaSuccess) { fprintf(stderr,"CUDA ERROR %s: %d %s (%d)\n",__FILE__, __LINE__, cudaGetErrorString(cudaGetLastError()), cudaGetLastError()); fflush(stdout); fflush(stderr); assert(0); exit(-1); }

#endif
