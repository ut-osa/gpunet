#ifndef FS_CALLS_CU
#define FS_CALLS_CU

#include "radix_tree.cu.h"
#include "fs_constants.h"
#include "fs_debug.cu.h"
#include "util.cu.h"
#include "cpu_ipc.cu.h"
#include "mallocfree.cu.h"
#include "fs_structures.cu.h"
#include "timer.h"
#include "hash_table.cu.h"
#include "swapper.cu.h"
#include "fs_globals.cu.h"

// no reference counting here
 __device__ int single_thread_fsync(int fd);
 __device__ int single_thread_ftruncate(int fd, int size);
 __device__ int single_thread_close(int fd);
 __device__ int single_thread_open(char* filename, int flags);

#define READ 0
#define WRITE 1

__device__ volatile FTable_page* getRwLockedPage(volatile FTable_entry* fentry, size_t block_id, int fd, int cpu_fd,int type_req);

 __device__ int gclose(int fd);
 __device__ int gopen(char* filename, int flags);
__device__ int gmsync(volatile void *addr, size_t length,int flags);
__device__ int gmunmap(volatile void *addr, size_t length);
 __device__ volatile void* gmmap(void *addr, size_t size,
					int prot, int flags, int fd, off_t offset);
__device__ size_t gwrite(int fd,size_t offset, size_t size, uchar* buffer);
 __device__ size_t gread(int fd, size_t offset, size_t size, uchar* buffer);
 __device__ uint gunlink(char* filename);
 __device__ size_t fstat(int fd);


 __device__ int gfsync(int fd);
 __device__ int gftruncate(int fd, int size);

#endif
