#ifndef __NET_GLOBALS_ADD_CU_H__
#define __NET_GLOBALS_ADD_CU_H__

#include "net_constants.h"

#include <pthread.h>
class STable;
class gpu_ipc_manager;
class cpu_ipc_queue;

__device__ extern STable* g_soctable; // all sockets

__device__ extern gpu_ipc_manager* g_ipc_manager; //gpu IPC manager

__device__ extern cpu_ipc_queue* g_cpu_ipc_queue; //cpu-gpu shared space

#ifdef DEFAULT_TO_UNIX_SOCKET
#define USE_G_BUFFER_SPACE
#endif
#ifdef USE_G_BUFFER_SPACE
/* all the buffer space altogether - split into chunks of CPU_IO_BUF_SIZE */
__device__ extern volatile uchar* g_buffer_space;
#endif

#endif
