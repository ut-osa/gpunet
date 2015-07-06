#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <sys/mman.h>

#include <pthread.h>
#include <map>
#include <cuda.h>
#include <cuda_runtime.h>

#include "ipc_shared.h"

#include "net_globals.h"
#include "net_private.h"
#include "net_debug.h"

extern "C" {
#define GPUNET
#include <rdma_dgram/rsocket.h>
#include <rdma_dgram/gpu.h>
}

#include "profiler.h"

struct msg_buffer {
	int size_ready;
	uchar* data;
};

typedef enum {
	TASK_ARRAY_EMPTY,
	TASK_ARRAY_PENDING,
	TASK_ARRAY_GPU_DATA_TRANSFER
} task_array_state_t;

struct TaskArray {

	task_array_state_t task_array[TASK_ARRAY_SIZE];
	cudaEvent_t cuda_events[TASK_ARRAY_SIZE];

	TaskArray() {
		bzero(task_array, TASK_ARRAY_SIZE * sizeof(int));
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			cudaEventCreate(&cuda_events[i]);
		}
	}
	~TaskArray() {
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			cudaEventDestroy(cuda_events[i]);
		}
	}
};

struct GPUStreamManager
{
	// GPU streams
	GPUStreamManager() {
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			CUDA_SAFE_CALL(cudaStreamCreate(&memStream[i]));
		}
	}

	~GPUStreamManager() {
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			CUDA_SAFE_CALL(cudaStreamDestroy(memStream[i]));
		}
	}

	cudaStream_t memStream[TASK_ARRAY_SIZE];
};
static TaskArray* task_queue;  // used only in the hostloop
static GPUStreamManager* stream_mgr;
static GPUNETGlobals* gpu_globals;

struct SockInfo {
	int domain;
	int proto;
};

std::map<int, SockInfo*> sock_tbl;

//#define DEBUG(X) fprintf(stdout,X "\n");
//#define DEBUG(X) (X);
#define DEBUG(x)
#define WARNING(X) fprintf(stderr,"Warning: ");fprintf(stderr, X "\n");

#define ENABLE_DEBUG_SLOT_STATE 0
#if ENABLE_DEBUG_SLOT_STATE
#define DEBUG_SLOT_STATE(x) do { fprintf(stderr, "\tChange SLOT %d's state to %d @ line %d\n", task_array_slot, x, __LINE__); } while (0)
#else
#define DEBUG_SLOT_STATE(x) 
#endif

int sysbuf_init = 0;

int set_nonblocking_fd(int sock, int sock_proto) {
	int val, ret;

	if (sock_proto == IPPROTO_IBP) {
		val = rfcntl(sock, F_GETFL, 0);
	} else {
		val = fcntl(sock, F_GETFL, 0);
	}

	if (val == -1) {
		return -1;
	}

	if (sock_proto == IPPROTO_IBP) {
		ret = rfcntl(sock, F_SETFL, val | O_NONBLOCK);
	} else {
		ret = fcntl(sock, F_SETFL, val | O_NONBLOCK);
	}

	return ret;
}

int sock_close(int sock, int sock_proto) {
	if (sock_proto == IPPROTO_IBP) {
		return rclose(sock);
	} else {
		return close(sock);
	}
}

void ipc_kernel_sync_exec(cpu_ipc_entry* e, int task_array_slot);
void ipc_kernel_async_exec(cpu_ipc_entry* e, int task_array_slot);

// TODO need to add exit code, probably
void GPUNETGlobals::ipc_service(cpu_ipc_entry* e, int task_array_slot)
{
	int bounce = 0;
#if ENABLE_DEBUG_SLOT_STATE
	fprintf(stderr, "\thandling slot %d's req %d\n", task_array_slot, e->req_type);
#endif

	switch (e->req_type) {
	case BIND_IPC_REQ_BOUNCE:
		bounce = 1;
		// followthrough;
	case BIND_IPC_REQ: {
		int sock_domain = e->sock_domain;  // AF_LOCAL or AF_INET
		int sock_proto = e->sock_proto; // IPPROTO_IBP

		int new_socket = 0;
		struct sockaddr* addr;
		size_t addr_len;

		if (sock_domain != AF_LOCAL && sock_domain != AF_INET) {
			fprintf(stderr, "invalid socket domain: %d", sock_domain);
			return;
		}

		if (sock_domain == AF_INET) {
			struct sockaddr_in *in_addr =
				(struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));

			DEBUG("creating socket using ip address");

			memcpy(in_addr, (const void*)&e->addr.in, sizeof(e->addr.in));
			addr = (struct sockaddr*)in_addr;
			addr_len = sizeof(*in_addr);
			addr->sa_family = PF_INET;
		}
		
		if (sock_proto == IPPROTO_IBP) {
			int gpubuf, ret;
			new_socket = rsocket_gpu(sock_domain, SOCK_DGRAM, 0);
			if (new_socket >= 0) {
				gpubuf = 1;
				ret = rsetsockopt(new_socket, SOL_RDMA, RDMA_RECV_GPUBUF,
								  &gpubuf, sizeof(int));
				assert(ret == 0);

				gpubuf = 1;
				ret = rsetsockopt(new_socket, SOL_RDMA, RDMA_SEND_GPUBUF,
								  &gpubuf, sizeof(int));
				assert(ret == 0);

				if (bounce) {
					rsetsockopt(new_socket, SOL_RDMA, RDMA_RECV_GPUBUF_BOUNCE,
								&gpubuf, sizeof(int));
					assert(ret == 0);
					rsetsockopt(new_socket, SOL_RDMA, RDMA_SEND_GPUBUF_BOUNCE,
								&gpubuf, sizeof(int));
					assert(ret == 0);
				}
			}
		} else {
			new_socket = socket(sock_domain, SOCK_DGRAM, 0);
		}

		if (new_socket < 0) {
			perror("socket creation failed\n");
			goto error_socket;
		}

		int ret;

		if (sock_proto == IPPROTO_IBP) {
			ret = rbind(new_socket, addr, addr_len);
		} else {
			fprintf(stderr, "IP: %p %lu\n", addr, addr_len);
			ret = bind(new_socket, addr, addr_len);
		}
		if (ret == -1) {
			perror("bind failed");
			goto error_bind;
		}

		free(addr);

		ret = set_nonblocking_fd(new_socket, sock_proto);
		if (ret == -1) {
			perror("fcntl failed at socket creation\n");
			goto error_bind;
		}

		if (new_socket != -1) {

			SockInfo* sock_info = new SockInfo;
			sock_info->domain = sock_domain;
			sock_info->proto = sock_proto;
			assert(sock_tbl.find(new_socket) == sock_tbl.end());
			sock_tbl[new_socket] = sock_info;

			e->ret_val = new_socket;
			
			// connect successful
			get_bufs_gpu(e->ret_val,
						 (void**)&e->sbuf_addr,
						 (int*)&e->sbuf_size,
						 (int*)&e->rbuf_size,
						 (uintptr_t*)&e->dev_sbuf_free_queue_back,
						 (uintptr_t*)&e->dev_sbuf_free_queue,
						 (uintptr_t*)&e->host_sbuf_req_queue,
						 (uintptr_t*)&e->dev_sbuf_req_back,
						 (uintptr_t*)&e->host_sbuf_req_back,
						 (uintptr_t*)&e->dev_rbuf_recved_queue,
						 (uintptr_t*)&e->dev_rbuf_recved_back,
						 (uintptr_t*)&e->host_rbuf_ack_queue_back);
		}

		goto success;

		error_bind:
		sock_close(new_socket, sock_proto);

		error_socket:
		new_socket = -1;

		success:

		e->ret_val = new_socket;
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();

		break;
	}

	case CONNECT_IPC_REQ_BOUNCE:
		bounce = 1;
	case CONNECT_IPC_REQ:
	{
		int ret = 0;

		int socket = e->cpu_sock;

		struct sockaddr_in *peer_addr =
			(struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
		memcpy(peer_addr, (const void*)&e->addr.in, sizeof(*peer_addr));

		ret = rconnect(socket, (sockaddr*)peer_addr, e->data_size);
		free(peer_addr);
		if (ret < 0 && errno == EWOULDBLOCK) {
			task_queue->task_array[task_array_slot] = TASK_ARRAY_PENDING;
			DEBUG_SLOT_STATE(TASK_ARRAY_PENDING);
			break;
		}
		e->ret_val = ret;
		
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

	case SHUTDOWN_IPC_REQ: {
		if (sock_tbl.find((int)e->cpu_sock) == sock_tbl.end()) {
			fprintf(stderr, "shutdown: socket %d is not registered.\n",
					e->cpu_sock);
			break;
		}

		SockInfo* sock_info = sock_tbl[(int)e->cpu_sock];
		int ret;
		if (sock_info->proto == IPPROTO_IBP) {
			ret = rshutdown(e->cpu_sock, e->shutdown_flags);
		} else {
			ret = shutdown(e->cpu_sock, e->shutdown_flags);
		}

		if (ret < 0) {
			perror("shutdown failed");
		}
		e->ret_val = ret;
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

	case CLOSE_IPC_REQ: {
		int ret;
		if (sock_tbl.find((int)e->cpu_sock) == sock_tbl.end()) {
			fprintf(stderr, "close: socket %d is not registered.\n",
					e->cpu_sock);
			ret = -EBADF;
		} else {
			SockInfo* sock_info = sock_tbl[(int)e->cpu_sock];
			ret = sock_close(e->cpu_sock, sock_info->proto);
			if (ret < 0) {
				perror("shutdown failed");
			}
			delete sock_info;
			sock_tbl.erase((int)e->cpu_sock);
		}
		//unlink((const char*)e->addr.local);
		e->ret_val = ret;
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

    case LOG_TIMESTAMP_REQ: {
		struct timeval tv;
		gettimeofday(&tv, NULL);
		e->tv_sec = (long int)tv.tv_sec;
		e->tv_usec = (long)tv.tv_usec;

		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

	case PUTS_REQ: {
		e->addr.local[GPU_LOCAL_SOC_MAX_PATH-1] = '\0';
		fprintf(stderr, "GPU blk%d %s\n", e->sock_domain, e->addr.local);
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

	case NULL_IPC_REQ: {
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

    default:
		assert(0);
	}
}

//void* handler_thread(void* _globals_ptr){
int GPUNETGlobals::start_hostloop(cudaStream_t kernel_stream)
{
	unsigned int count = 0;
	while (cudaStreamQuery(kernel_stream) == cudaErrorNotReady
		|| priv_->is_any_stream_busy()) {
		
		single_hostloop(kernel_stream);

		if (count++ % 100000 == 0) {
			/*
			 * kernel assertion/failure detection
			 */
			size_t f, t;
			cudaError_t cuda_error = cudaMemGetInfo(&f, &t);
			if (cuda_error != (cudaError_t)CUDA_SUCCESS) {
				fprintf(stderr, "ERROR: CUDA ERROR %s",
						cudaGetErrorString(cuda_error));
				exit(1);
			}
		}
	}
	return NULL;
}

#ifdef GPUNET_PROFILE
struct timeval control_poll_begin, control_poll_end, control_poll_interval, data_poll_end, data_poll_interval;
double data_poll_total = 0.0, control_poll_total = 0.0;
int count = 0;
int service_count[IPC_REQ_TYPE_MAX];
#endif

int GPUNETGlobals::single_hostloop(cudaStream_t kernel_stream)
{
	cpu_ipc_queue* ipc_q = gpu_globals->_h_ipc_queue;

#ifdef GPUNET_PROFILE
	gettimeofday(&control_poll_begin, NULL);
#endif

	for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
		if (ipc_q->entries[i].status != CPU_IPC_READY) {
#if ENABLE_DEBUG_SLOT_STATE
			int old = task_queue->task_array[i];
			int reqt = ipc_q->entries[i].req_type;
			int olds = ipc_q->entries[i].status;
#endif
			ipc_service(&ipc_q->entries[i], i);
#ifdef GPUNET_PROFILE
			service_count[ipc_q->entries[i].req_type]++;
#endif
#if ENABLE_DEBUG_SLOT_STATE
			int nouveau = task_queue->task_array[i];
			int nouveaus = ipc_q->entries[i].status;
			if (old != nouveau) {
				fprintf(stderr, "\tSLOT %d's status was changed from %d to %d in handling %d\n",
						i, old, nouveau, reqt);
			}
			if (olds != nouveaus) {
				fprintf(stderr, "\tSLOT %d's IPC was changed from %d to %d in handling %d\n",
						i, olds, nouveaus, reqt);
			}
#endif
		}
	}

#ifdef GPUNET_PROFILE
	gettimeofday(&control_poll_end, NULL);
#endif
	
	::poll_backend_bufs();
	
#ifdef GPUNET_PROFILE
	gettimeofday(&data_poll_end, NULL);
	
	timersub(&data_poll_end, &control_poll_end, &data_poll_interval);
	timersub(&control_poll_end, &control_poll_begin, &control_poll_interval);
	control_poll_total += (control_poll_interval.tv_sec * 1000.0 + control_poll_interval.tv_usec / 1000.0);
	data_poll_total += (data_poll_interval.tv_sec * 1000.0 + data_poll_interval.tv_usec / 1000.0);
	if (++count == 10000) {
		fprintf(stderr, "control poll average: %.2f us, data poll average: %.2f us\n",
				control_poll_total*1000.0/count, data_poll_total*1000.0/count);
		for (int i = 0 ; i < IPC_REQ_TYPE_MAX; i++) {
			if (service_count[i] != 0) {
				fprintf(stderr, "   req type: %d count: %d\n", i, service_count[i]);
				service_count[i] = 0;
			}
		}
		count = 0;
		control_poll_total = 0;
		data_poll_total = 0;
	}
#endif

	return 0;
}

cudaStream_t get_memstream(int i) {
	return stream_mgr->memStream[i];
}

int init_hostloop(GPUNETGlobals* g) {

	task_queue = new TaskArray();  // used only in the hostloop
	stream_mgr = new GPUStreamManager();

	gpu_globals = g;

	install_memstream_provider(get_memstream, TASK_ARRAY_SIZE);

	return 0;
}

#if 0
#define VERBOSEINFO(...)        do { fprintf(stderr, __VA_ARGS__); } while(0)
#else
#define VERBOSEINFO(...)
#endif


