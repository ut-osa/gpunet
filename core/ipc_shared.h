#ifndef IPC_SHARED_H
#define IPC_SHARED_H

#include <arpa/inet.h>

#include "net_constants.h"

#define CPU_IPC_READY 0
#define CPU_IPC_PENDING 1

typedef enum { BIND_IPC_REQ=1, ACCEPT_IPC_REQ, CONNECT_IPC_REQ,
			   RECV_IPC_REQ, SEND_IPC_REQ,
			   SEND_POLL_IPC_REQ, RECV_POLL_IPC_REQ,
			   SHUTDOWN_IPC_REQ, CLOSE_IPC_REQ, LOG_TIMESTAMP_REQ,
			   PUTS_REQ,
			   KERNEL_SYNC_EXECUTE_REQ,
			   KERNEL_ASYNC_INVOKE_REQ,
			   KERNEL_MULTI_ASYNC_INVOKE_REQ,
			   RECV_NB_IPC_REQ,
			   ACCEPT_NB_IPC_REQ,
			   NULL_IPC_REQ,
			   BIND_IPC_REQ_BOUNCE,
			   CONNECT_IPC_REQ_BOUNCE
} req_type_t;
typedef enum { GPU_CLIENT_SOCKET, GPU_SERVER_SOCKET} soc_purpose_t;


struct cpu_ipc_entry{
	/** bind**/
	volatile   int sock_domain; // AF_LOCAL
	volatile   int sock_proto;  // IPPROTO_TCP/IPPROTO_IBP
	volatile   req_type_t req_type;

	/** accept */
	volatile   uintptr_t sbuf_addr; // from CPU to GPU
	volatile   uintptr_t rbuf_addr;
	volatile   int rbuf_size;
	volatile   uintptr_t dev_rbuf_bytes_avail;
	volatile   uintptr_t dev_rbuf_offset;

	//soc_purpose_t purpose;
	union {
		volatile char local[GPU_LOCAL_SOC_MAX_PATH];
		volatile struct sockaddr_in in;
	} addr;

	/** recv/send **/
	volatile   int data_size;

	union {
		volatile   int req_buffer_offset;
		volatile   int local_buf_free;
	};

	volatile   int cpu_sock;

	/** connect, accept **/
	volatile   char peer_addr[GPU_LOCAL_SOC_MAX_PATH];

	/** shutdown**/
	volatile int shutdown_flags;

	volatile int ret_val;

	/** queue entry status **/

	volatile char status;

/** timestamp **/
    volatile long int tv_sec;
    volatile long tv_usec;

	/* Kernel Invocation */
	volatile char func_name[GPU_LOCAL_FNAME_MAX_SIZE];
	volatile int grid_dim[3];
	volatile int block_dim[3];
	volatile void* argument;
};

struct cpu_ipc_queue
{
	cpu_ipc_entry entries[TASK_ARRAY_SIZE];
};

void cpu_ipc_queue_init(cpu_ipc_queue* );

void cpu_ipc_entry_init(cpu_ipc_entry*);

#endif
