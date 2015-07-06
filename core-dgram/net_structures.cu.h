#ifndef NET_STRUCTURES
#define NET_STRUCTURES

#include <stdint.h>
#include <sys/socket.h>

#include "net_constants.h"

#define E_SOCTABLE_FULL (-1)

struct ds_smsg_info {
	int msg_index;
	uint16_t length;
	uint16_t addrlen;
	uint8_t  addr[28]; // size of sockaddrin_6
};

struct ds_rmsg_info {
	uintptr_t ptr;
	socklen_t addrlen;
	uint16_t length;
	uint8_t  addr[28]; // size of sockaddrin_6
};

// allocated at the bind() or connect() time.
// freed at shutdown/close()
struct gpu_socket {
	volatile int cpu_sock;           // initialized with successful bind/connect
	volatile uint8_t *sbuf;          // 
	volatile int sbuf_size;
	volatile int sq_size;
	
	// sbuf_free_queue. the host produces an entry
	// size: sq_size + 1
	volatile int *dev_sbuf_free_queue_back; // only updated by CPU, but this one is on GPU.
	volatile int *dev_sbuf_free_queue; // on GPU memory (not necessarily shared), but updated by CPU.
	volatile int sbuf_free_queue_front; // only updated by GPU.

	// sbuf_req_queue. size: sq_size+1. (size should be the size of sbuf_free_queue. refer to comments in gsend_notify_cpu())
	volatile ds_smsg_info *host_sbuf_req_queue;    // on CPU memory, only updated by GPU, only accessed by CPU
	volatile int *dev_sbuf_req_back; // on GPU memory. only updated by GPU. 
	volatile int *host_sbuf_req_back; // on CPU memory. only updated by GPU.

	// many rbuf per a socket, but one rbuf size across those rbufs
	volatile int rbuf_size;
	volatile int rq_size;
	
	volatile ds_rmsg_info *dev_rbuf_recved_queue; // on GPU memory. only updated by CPU. each entry corresponds to each message buffer. 
	volatile int rbuf_recved_front; // only accessed & updated by GPU.
	volatile int *dev_rbuf_recved_back; // on GPU memory. only updated by CPU.

	volatile int rbuf_ack_queue_back;  // on GPU memory, updated by GPU.
	volatile int *host_rbuf_ack_queue_back; // on CPU memory, updated by GPU.

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
	//TODO this can be optimized to avoid contention on newsockets
	__device__ int findNewEntry();

	__device__ void free_socket(int soc);
};
#endif
