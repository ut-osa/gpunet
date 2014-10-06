#ifndef SWAPPER_CU_H
#define SWAPPER_CU_H

#include "fs_structures.cu.h"

__device__ int swapout(int npages);
__device__ int flush_cpu(volatile FTable_entry* file, volatile OTable_entry* e,
                         int flags);
__device__ int writeback_page(int fd, volatile FTable_page* p);
__device__ int writeback_page(int cpu_fd, volatile FTable_page* p, int flags,
                              bool tolock);

#endif
