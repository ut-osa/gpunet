/*
 * This expermental software is provided AS IS.
 * Feel free to use/modify/distribute,
 * If used, please retain this disclaimer and cite
 * "GPUfs: Integrating a file system with GPUs",
 * M Silberstein,B Ford,I Keidar,E Witchel
 * ASPLOS13, March 2013, Houston,USA
 */


#ifndef GPUNET_UTIL_CU_H
#define GPUNET_UTIL_CU_H

#include "net_constants.h"
//#include "net_debug.h"

#define GPU_ERROR(str) __assert_fail(str,__FILE__,__LINE__,__func__);

__device__ int getNewFileId();

#endif
