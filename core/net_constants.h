/* 
* This expermental software is provided AS IS. 
* Feel free to use/modify/distribute, 
* If used, please retain this disclaimer and cite 
* "GPUfs: Integrating a file system with GPUs", 
* M Silberstein,B Ford,I Keidar,E Witchel
* ASPLOS13, March 2013, Houston,USA
*/


#ifndef NET_CONSTANTS
#define NET_CONSTANTS

#define IPC_MGR_EMPTY 0
#define IPC_MGR_BUSY 1

typedef unsigned char uchar;

#define GPU_LOCAL_SOC_MAX_PATH 64 /* max len of the address to connect to */

#define GPU_SERVER_SOCKET_BACKLOG 256 /* max backlog for the server socket on GPU */

#define SOC_TABLE_SIZE (512) /* related to GPUDirect limit. */

/* must be power of 2 */
#define TASK_ARRAY_SIZE  SOC_TABLE_SIZE  /* number of entries in the RPC queue */


#define CPU_IO_BUF_SIZE ((1 << 20))  /* size of a single send/recv buffer */
#define SYSBUF_SIZE ((1 << 18))  /* size of a single send/recv buffer */

#define IPPROTO_IBP 254

#define GPU_LOCAL_FNAME_MAX_SIZE 64 /* max len of kernel name to launch */
#define EKERN		(-4) // Kernel invocation error

#endif
