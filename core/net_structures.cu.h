#ifndef NET_STRUCTURES
#define NET_STRUCTURES

#include "net_constants.h"
#include "ringbuf.cu.h"

#define E_SOCTABLE_FULL (-1)

struct gpu_socket {
	volatile int cpu_soc;
#ifdef DEFAULT_TO_UNIX_SOCKET
	volatile int recv_buffer_offset;
	volatile int send_buffer_offset;
#endif
	volatile g_ringbuf::ringbuf_t sbuf;

	volatile uint8_t *rbuf;
	volatile int rbuf_size;
	volatile uint64_t *rbuf_bytes_avail;
	volatile uint64_t rbuf_bytes_avail_cache;
	volatile uint64_t *rbuf_offset;

	volatile uchar addr[GPU_LOCAL_SOC_MAX_PATH]; // remote or local address where the socet is connected

	volatile int e_flags; // flags for the table array

	__device__ void init(int e);
	__device__ void allocate();
	__device__ void free();
};

struct STable{
	gpu_socket _sockets[SOC_TABLE_SIZE];
	volatile int lock;
	static const char EMPTY = 0;
	static const char ALLOCATED = 1;

	__device__ void init();
	__device__ int findNewEntry();

	__device__ void free_socket(int soc);
};
#endif
