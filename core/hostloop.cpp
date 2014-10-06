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
#include <rdma/rsocket.h>
#include <rdma/gpu.h>
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
	msg_buffer recv_bufs[TASK_ARRAY_SIZE];
	msg_buffer send_bufs[TASK_ARRAY_SIZE];
	cudaEvent_t cuda_events[TASK_ARRAY_SIZE];

	TaskArray() {
		bzero(task_array, TASK_ARRAY_SIZE * sizeof(int));
		bzero(recv_bufs, TASK_ARRAY_SIZE * sizeof(msg_buffer));
		bzero(send_bufs, TASK_ARRAY_SIZE * sizeof(msg_buffer));
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			CUDA_SAFE_CALL(
				cudaHostAlloc(&recv_bufs[i].data,CPU_IO_BUF_SIZE,cudaHostAllocDefault));
			CUDA_SAFE_CALL(
				cudaHostAlloc(&send_bufs[i].data,CPU_IO_BUF_SIZE,cudaHostAllocDefault));
			cudaEventCreate(&cuda_events[i]);
		}
	}
	~TaskArray() {
		for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
			cudaEventDestroy(cuda_events[i]);
			cudaFreeHost(recv_bufs[i].data);
			cudaFreeHost(send_bufs[i].data);
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

#define DEBUG(x)
#define WARNING(X) fprintf(stderr,"Warning: ");fprintf(stderr, X "\n");

#define ENABLE_DEBUG_SLOT_STATE 0
#if ENABLE_DEBUG_SLOT_STATE
#define DEBUG_SLOT_STATE(x) do { fprintf(stderr, "\tChange SLOT %d's state to %d @ line %d\n", task_array_slot, x, __LINE__); } while (0)
#else
#define DEBUG_SLOT_STATE(x) 
#endif

int sysbuf_init = 0;

void bufs_for_gpu(int sock, uintptr_t* sbuf, uintptr_t* rbuf,
                  uintptr_t* rbuf_bytes_avail, uintptr_t* rbuf_offset) {
	uint32_t* dev_rbuf_bytes_avail, *dev_rbuf_offset;

	get_bufs_gpu(sock, (void**)sbuf, (void**)rbuf, &dev_rbuf_bytes_avail, &dev_rbuf_offset);

	if (*sbuf == 0 || *rbuf == 0 || dev_rbuf_offset == 0 || dev_rbuf_bytes_avail == 0) {
		fprintf(stderr, "ERROR: get_bufs_gpu buffer not initialized\n");
		exit(1);
	}

	*rbuf_bytes_avail = (uintptr_t)dev_rbuf_bytes_avail;
	*rbuf_offset = (uintptr_t)dev_rbuf_offset;
}

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
		int sock_proto = e->sock_proto; // IPPROTO_TCP or IPPROTO_IBP

		int new_socket = 0;
		struct sockaddr* addr;
		size_t addr_len;

		if (sock_domain != AF_LOCAL && sock_domain != AF_INET) {
			fprintf(stderr, "invalid socket domain: %d", sock_domain);
			return;
		}

		// address setup
		if (sock_domain == AF_LOCAL) {
			DEBUG("creating domain socket ");
			fprintf(stderr, "%s\n", e->addr.local);

			struct sockaddr_un *my_addr =
				(struct sockaddr_un*)malloc(sizeof(struct sockaddr_un));
			memset(my_addr, 0, sizeof(struct sockaddr_un));
			my_addr->sun_family = AF_LOCAL;

			assert(GPU_LOCAL_SOC_MAX_PATH<sizeof(my_addr->sun_path));
			strncpy(my_addr->sun_path, (const char*) e->addr.local,
					GPU_LOCAL_SOC_MAX_PATH);

			addr = (struct sockaddr*)my_addr;
			addr_len = sizeof(*my_addr);
			fprintf(stderr, "LOCAL: %p %lu\n", addr, addr_len);

		} else if (sock_domain == AF_INET) {
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
			new_socket = rsocket_gpu(sock_domain, SOCK_STREAM, 0);
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
			new_socket = socket(sock_domain, SOCK_STREAM, 0);
		}

		if (new_socket < 0) {
			perror("socket creation failed\n");
			goto error_socket;
		}

		int ret;

		if (sock_proto == IPPROTO_IBP) {
			ret = rbind(new_socket, addr, addr_len);
		} else {
			fprintf(stderr, "LOCAL: %p %lu\n", addr, addr_len);
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

		if (sock_proto == IPPROTO_IBP) {
			if (rlisten(new_socket, GPU_SERVER_SOCKET_BACKLOG) < 0) {
				goto error_bind;
			}
		} else {
			if (listen(new_socket, GPU_SERVER_SOCKET_BACKLOG) < 0) {
				perror("listen failed at socket creation\n");
				goto error_bind;
			}
		}

		if (new_socket != -1) {
			SockInfo* sock_info = new SockInfo;
			sock_info->domain = sock_domain;
			sock_info->proto = sock_proto;
			assert(sock_tbl.find(new_socket) == sock_tbl.end());
			sock_tbl[new_socket] = sock_info;
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

	case ACCEPT_NB_IPC_REQ:
	case ACCEPT_IPC_REQ: {
		// some context is necessary to differentiate socket types
		struct sockaddr_un peer_addr;
		socklen_t socaddr_len;
		int cpu_sock = e->cpu_sock;
		SockInfo* sock_info;
		std::map<int, SockInfo*>::iterator sock_iter;
		int ret;

		bzero(&peer_addr, sizeof(struct sockaddr_un));

		if ((sock_iter = sock_tbl.find(cpu_sock)) == sock_tbl.end()) {
			// not found in socket_tbl.
			fprintf(stderr, "ERROR: in accept() - socket %d is not listening\n",
					cpu_sock);
			errno = EINVAL;
			ret = -1;
			goto out_accept;
		}

		sock_info = sock_iter->second;

		if (sock_info->proto == IPPROTO_IBP) {
			ret = raccept(cpu_sock, (struct sockaddr*) &peer_addr,
					&socaddr_len);
			if (ret >= 0) {
				set_nonblocking_fd(ret, IPPROTO_IBP);
			}
		} else {
			ret = accept4(e->cpu_sock, (struct sockaddr*) &peer_addr,
					&socaddr_len, SOCK_NONBLOCK);
		}

		if (ret < 0) {
			if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
				perror("socket accept failed - killing the socket\n");
				sock_close(cpu_sock, sock_info->proto);
				goto out_accept;
			} else {
				//non-blocking call
				if (e->req_type == ACCEPT_NB_IPC_REQ) {
					ret = -EWOULDBLOCK;
					goto out_accept;
				}
				task_queue->task_array[task_array_slot] = TASK_ARRAY_PENDING;
				DEBUG_SLOT_STATE(TASK_ARRAY_PENDING);
				break;
			}

		} else {
			task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
			DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
			// we have an accept to handle
			memcpy((void*) e->peer_addr, peer_addr.sun_path,
					GPU_LOCAL_SOC_MAX_PATH);

			if (sock_info->proto == IPPROTO_IBP) {
				bufs_for_gpu(ret,
						(uintptr_t*)&e->sbuf_addr,
						(uintptr_t*)&e->rbuf_addr,
						(uintptr_t*)&e->dev_rbuf_bytes_avail,
						(uintptr_t*)&e->dev_rbuf_offset);

				int rbuf_size_len;

				rgetsockopt(ret,
						SOL_SOCKET, SO_RCVBUF,
						(void*)&e->rbuf_size, (socklen_t*)&rbuf_size_len);
			}

			SockInfo* new_info = new SockInfo;
			new_info->domain = sock_info->domain;
			new_info->proto = sock_info->proto;
			assert(sock_tbl.find(ret) == sock_tbl.end());
			sock_tbl[ret] = new_info;
		}
out_accept:
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
		e->ret_val = ret;  // this is a new socket
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		DEBUG("Accept success");
		break;
	}

	case CONNECT_IPC_REQ_BOUNCE:
		bounce = 1;
	case CONNECT_IPC_REQ: {
		int sock_domain = e->sock_domain;  // AF_LOCAL or AF_INET
		int sock_proto = e->sock_proto; // IPPROTO_TCP or IPPROTO_IBP
		int ret = 0;

		if (task_queue->task_array[task_array_slot] != TASK_ARRAY_EMPTY) {  // we're in a new call
			goto connect_async;
		}

		int new_socket;
		struct sockaddr *addr;
		size_t addr_len;

		if (sock_domain == AF_LOCAL) {
			struct sockaddr_un *peer_addr =
				(struct sockaddr_un*)malloc(sizeof(struct sockaddr_un));
			memset(peer_addr, 0, sizeof(struct sockaddr_un));
			memcpy(peer_addr->sun_path, (const void*) e->addr.local,
				   GPU_LOCAL_SOC_MAX_PATH);
			peer_addr->sun_family = AF_LOCAL;

			addr = (struct sockaddr*)peer_addr;
			addr_len = sizeof(*peer_addr);

		} else if (sock_domain == AF_INET) {
			struct sockaddr_in *peer_addr =
				(struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
			memcpy(peer_addr, (const void*)&e->addr.in, sizeof(*peer_addr));

			addr = (struct sockaddr*)peer_addr;
			addr->sa_family = PF_INET;

			addr_len = sizeof(*peer_addr);
		} else {
			fprintf(stderr, "invalid socket domain: %d", sock_domain);
		}

		if (e->cpu_sock < 0) {
			if (sock_proto == IPPROTO_IBP) {
				new_socket = rsocket_gpu(sock_domain, SOCK_STREAM, 0);
			} else {
				new_socket = socket(sock_domain, SOCK_STREAM, 0);
			}

			e->cpu_sock = e->ret_val = new_socket;
			if (new_socket < 0) {
				perror("socket creation failed during connect\n");
				e->ret_val = -1;
				goto out_connect;
			}
		}

		if (e->ret_val >= 0) {
			int gpubuf = 1;
			ret = rsetsockopt(e->ret_val, SOL_RDMA, RDMA_RECV_GPUBUF,
							  &gpubuf, sizeof(int));
			assert(ret == 0);

			gpubuf = 1;
			ret = rsetsockopt(e->ret_val, SOL_RDMA, RDMA_SEND_GPUBUF,
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

		ret = set_nonblocking_fd(new_socket, sock_proto);
		if (ret == -1) {
			perror("fcntl failed at socket creation at connect\n");
			e->ret_val = -1;
			goto error_connect;
		}

		if (sock_proto == IPPROTO_IBP) {
			ret = rconnect(e->cpu_sock, addr, addr_len);
		} else {
			ret = connect(e->cpu_sock, addr, addr_len);
		}

		if (ret < 0) {
			if (errno != EINPROGRESS && (errno != EWOULDBLOCK)) {
				perror("connect failed, killing the socket");
				e->ret_val = -1;
				goto error_connect;
			} else {
				task_queue->task_array[task_array_slot] = TASK_ARRAY_PENDING;
				DEBUG_SLOT_STATE(TASK_ARRAY_PENDING);
				// we're in a non-blocking mode
			}
		}
		free(addr);

		connect_async:
		// at this point we have to check async status

		struct timeval t;
		t.tv_sec = 0;
		t.tv_usec = 0;
		fd_set rset, wset;
		FD_ZERO(&rset);
		FD_SET(e->cpu_sock, &rset);
		wset = rset;
		if (sock_proto == IPPROTO_IBP) {
			ret = rselect(e->cpu_sock + 1, &rset, &wset, NULL, &t);
		} else {
			ret = select(e->cpu_sock + 1, &rset, &wset, NULL, &t);
		}

		if (ret == 0 || (ret < 0 && errno == EINTR)) {  // not ready
			break;
		}
		if (ret < 0) {
			perror("select failed on connect,killing the socket");
			e->ret_val = -1;
			goto error_connect;
		}

		// we're ready to proceed
		e->ret_val = e->cpu_sock;
		if (FD_ISSET(e->cpu_sock,&wset)) {
			// get errro
			int error = 0;
			socklen_t len = sizeof(error);
			int sock_err;

			if (sock_proto == IPPROTO_IBP) {
				sock_err = rgetsockopt(e->cpu_sock, SOL_SOCKET, SO_ERROR, &error, &len);
			} else {
				sock_err = getsockopt(e->cpu_sock, SOL_SOCKET, SO_ERROR, &error, &len);
			}

			if (sock_err < 0) {
				perror("failed to getsockopt as a part of connect, killing the socket\n");
				e->ret_val = -1;
				goto error_connect;
			}

			if (error != 0) {
				e->ret_val = -1;
				goto error_connect;
			}
			// success
		} else {
			perror(
				"FD_ISSET during connect should have been positive, killing the socket\n");
			e->ret_val = -1;
			goto error_connect;
		}

		error_connect:
		if (e->ret_val == -1) {
			sock_close(e->cpu_sock, sock_proto);
		}

		out_connect:
		if (e->ret_val >= 0) {
			SockInfo* sock_info = new SockInfo;
			sock_info->domain = sock_domain;
			sock_info->proto = sock_proto;
			fprintf(stderr, "Connect: new socket: %d\n", e->cpu_sock);
			sock_tbl[(int)e->ret_val] = sock_info;

			// connect successful
			bufs_for_gpu(e->ret_val,
						 (uintptr_t*)&e->sbuf_addr,
						 (uintptr_t*)&e->rbuf_addr,
						 (uintptr_t*)&e->dev_rbuf_bytes_avail,
						 (uintptr_t*)&e->dev_rbuf_offset);


			int rbuf_size_len;

			rgetsockopt(e->cpu_sock,
						SOL_SOCKET, SO_RCVBUF,
						(void*)&e->rbuf_size, (socklen_t*)&rbuf_size_len);

			e->ret_val = e->cpu_sock;
		}
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}
			      break;

	case RECV_NB_IPC_REQ:
	case RECV_IPC_REQ: {

		const int cpu_sock = e->cpu_sock;
		int total_recv = e->data_size;
		std::map<int, SockInfo*>::iterator sock_iter;
		SockInfo* sock_info;
		int ret;

		sock_iter = sock_tbl.find(cpu_sock);
		if (sock_iter == sock_tbl.end()) {
			ret = -1;
			e->ret_val = -1;
			goto out_recv;
		}

		sock_info = sock_iter->second;

		if (sock_info->proto == IPPROTO_IBP) {
			e->ret_val = -1;
			fprintf(stderr, "ERROR: RECV IPC not supported for rsocket\n");
			goto error_read;
		} else {
			struct msg_buffer* buf = &(task_queue->recv_bufs[task_array_slot]);

			assert(buf);
			assert(CPU_IO_BUF_SIZE>= total_recv);

			int single_recv = 0;
			int len = 0;
			int ret = 0;

			if (task_queue->task_array[task_array_slot] == TASK_ARRAY_GPU_DATA_TRANSFER) {
				cudaError_t cuda_status = cudaStreamQuery(
						stream_mgr->memStream[task_array_slot]);
				if (cudaErrorNotReady == cuda_status) {
					DEBUG("Not ready");
					return ;
				}
				if (cuda_status != cudaSuccess) {
					fprintf(stderr, "Error in the host loop.\n ");
					cudaError_t error = cudaDeviceSynchronize();
					fprintf(stderr, "%d Device failed, CUDA error message is: %s\n\n",
							__LINE__, cudaGetErrorString(error));
					exit(-1);
				}
				// done with transfer - can close the call

				e->ret_val = buf->size_ready;
				DEBUG(" Data copy to GPU done");
				task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
				DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
				buf->size_ready = 0;
				EV_STOP(UPLOAD_TIME, cpu_sock);
				goto out_recv;
			}

			struct timeval t;
			t.tv_sec = 0;
			t.tv_usec = 0;
			fd_set rset;
			FD_ZERO(&rset);
			FD_SET(cpu_sock, &rset);
			ret = select(cpu_sock + 1, &rset, NULL, NULL, &t);

			if (ret == 0 || (ret < 0 && errno == EINTR)) {  // not ready
				if (e->req_type == RECV_NB_IPC_REQ) {
					e->ret_val = -EWOULDBLOCK;
					goto out_recv;
				}
				break;
			}

			if (ret < 0) {
				perror("select failed on read");
				e->ret_val = -1;
				goto error_read;
			}
			DEBUG("Recv running");

			single_recv = total_recv - buf->size_ready;

			EV_FLIPPER(RECEIVE_TIME, RECEIVE_INQUEUE_TIME, cpu_sock);

			len = (int) recv(e->cpu_sock, buf->data + buf->size_ready, single_recv,
					0);

			if (len < 0) {
				perror("recv failed\n");

				e->ret_val = buf->size_ready > 0 ? buf->size_ready : -1;
				goto error_read;
			}

			if (len == 0) {
				int er = errno;
				if (er == EWOULDBLOCK || er == EAGAIN || er == EINTR) {
					fprintf(stderr, "recv(%d) %s\n", e->cpu_sock, strerror(er));
					return ;
				} {
					// socket has been shut down on the other side
					WARNING("socket shutdown");
					fprintf(stderr, "Socket failed, reason %s\n", strerror(er));
					e->ret_val = buf->size_ready > 0 ? buf->size_ready : 0;
					goto error_read;
				}
			}


			buf->size_ready += len;
			if (buf->size_ready >= total_recv) {  // we are done -> enqueue CUDA
				DEBUG("starting async copy to GPU");
				EV_START(UPLOAD_TIME, cpu_sock);
				CUDA_SAFE_CALL(
						cudaMemcpyAsync((char*)gpu_globals->_g_buffer_space+e->req_buffer_offset,
							buf->data,
							buf->size_ready,
							cudaMemcpyHostToDevice,
							stream_mgr->memStream[task_array_slot])
					      );
				task_queue->task_array[task_array_slot] = TASK_ARRAY_GPU_DATA_TRANSFER;
				DEBUG_SLOT_STATE(TASK_ARRAY_GPU_DATA_TRANSFER);
				DEBUG("async copy to GPU initiated");
				return ;
			}

			// here we are only if the read was incomplete
			// keep looping
			return ;
		}
error_read:
out_recv:
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		__sync_synchronize();
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		DEBUG("Ready");
		break;
	}
			   break;

	case SEND_IPC_REQ: {
		int ret;

		int total_send = e->data_size;
		struct msg_buffer* buf;

		if (sock_tbl.find((int)e->cpu_sock) == sock_tbl.end()) {
			fprintf(stderr, "socket %d is not registered.\n",
					e->cpu_sock);
			break;
		}

		SockInfo* sock_info = sock_tbl[(int)e->cpu_sock];

		if (sock_info->proto == IPPROTO_IBP) {
			EV_STOP(DOWNLOAD_TIME, e->cpu_sock);
			EV_FLIPPER(SEND_TIME, SEND_INQUEUE_TIME, e->cpu_sock);
			ret = rsend_gpu(e->cpu_sock, e->req_buffer_offset,
							e->data_size, 0);

			e->ret_val = ret;
			goto out_send;
		} else {
			buf = &task_queue->send_bufs[task_array_slot];
			assert(buf);
			assert(CPU_IO_BUF_SIZE >= total_send);

			int single_send;
			int len;
			if (task_queue->task_array[task_array_slot] == TASK_ARRAY_EMPTY) {
				// start sending
				task_queue->task_array[task_array_slot]=TASK_ARRAY_GPU_DATA_TRANSFER;
				DEBUG_SLOT_STATE(TASK_ARRAY_GPU_DATA_TRANSFER);
				DEBUG("memcpy from gpu mem");
				EV_START(DOWNLOAD_TIME, e->cpu_sock);
				CUDA_SAFE_CALL(cudaMemcpyAsync(buf->data,
							((char*)_g_buffer_space)+e->req_buffer_offset,
							total_send,
							cudaMemcpyDeviceToHost,
							stream_mgr->memStream[task_array_slot]));

#if ENABLE_DEBUG_SLOT_STATE
				fprintf(stderr, "Invoke memcpy from CPU to socket %d's buffer %p, size %d\n",
						e->cpu_sock, buf->data, total_send);
#endif

				DEBUG("memcpy from gpu mem async initiated");
				buf->size_ready=0;
#if ENABLE_DEBUG_SLOT_STATE
				fprintf(stderr, "In send %d: set size_ready to %d\n ", e->cpu_sock, buf->size_ready);
#endif
			}

			if (task_queue->task_array[task_array_slot] == TASK_ARRAY_GPU_DATA_TRANSFER) {
				cudaError_t cuda_status =
					cudaStreamQuery(stream_mgr->memStream[task_array_slot]);
				if (cudaErrorNotReady == cuda_status)
					return ;
				if (cuda_status != cudaSuccess) {
					fprintf(stderr, "Error in the host loop.\n ");
					cudaError_t error = cudaDeviceSynchronize();
					fprintf(stderr, "Device failed, CUDA error message is: %s\n\n",
							cudaGetErrorString(error));
					exit(-1);
				}
				if (buf->size_ready != 0)
					fprintf(stderr, "\t\t\tError in send %d: size_ready is not zero %d SLOT %d\n ",
							e->cpu_sock, buf->size_ready, task_array_slot);
#if ENABLE_DEBUG_SLOT_STATE
				fprintf(stderr, "Finish memcpy from CPU to socket %d's buffer %p, size %d\n",
						e->cpu_sock, buf->data, total_send);
#endif
				task_queue->task_array[task_array_slot] = TASK_ARRAY_PENDING;
				DEBUG_SLOT_STATE(TASK_ARRAY_PENDING);
				EV_STOP(DOWNLOAD_TIME, e->cpu_sock);
			}

			DEBUG("memcpy gpu->cpu complete");
			// we're here we copied from the GPU - so sending
			// 
			// make sure some space available in the send buf	      
			struct timeval t;
			t.tv_sec=0;
			t.tv_usec=0;

			fd_set wset;
			FD_ZERO(&wset);
			FD_SET(e->cpu_sock, &wset);
			ret = select(e->cpu_sock+1, NULL, &wset, NULL, &t);

			if (ret==0||(ret<0 && errno==EINTR))// not ready 
				return ;

			if (ret<0) {
				perror("select failed on write");
				e->ret_val=-1;
				goto error_write;
			}

			// ready to send
			EV_FLIPPER(SEND_TIME, SEND_INQUEUE_TIME, e->cpu_sock);

			if (total_send == 0) {
				fprintf(stderr, "total_send = 0\n");
				exit(-1);
			}
			single_send = total_send - buf->size_ready;
			len = (int)send(e->cpu_sock, buf->data+buf->size_ready, single_send, 0);
			DEBUG("sent from gpu enqueued");

			if (len < 0) {
				perror("send failed\n");
				e->ret_val=buf->size_ready>0?buf->size_ready:-1;
				goto error_write;
			}

			if (len==0) {
				int er = errno;
				if (er == EWOULDBLOCK || er == EAGAIN || er == EINTR) {
					fprintf(stderr, "send(%d) EAGAIN\n", e->cpu_sock);
					e->ret_val=buf->size_ready;
					goto out_send;
					return ;
				} {
					// socket has been shut down on the other side
					WARNING("socket shutdown");
					fprintf(stderr, "Socket failed, reason %s (%d) EAGAIN %d\n", strerror(er), er, EAGAIN);
					e->ret_val = buf->size_ready > 0 ? buf->size_ready : 0;
					goto error_write;
				}
			}

			// we're here if we've written something
			buf->size_ready+=len;
			if (buf->size_ready>=total_send) {// we are done
				e->ret_val=buf->size_ready;
			}
		}

out_send:
error_write:
		task_queue->task_array[task_array_slot] = TASK_ARRAY_EMPTY;
		DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();
		break;
	}

	case SEND_POLL_IPC_REQ: {
		// input: local buffer length.
		int cpu_sock = e->cpu_sock;
		int len = e->data_size;
		int sbuf_bytes_avail = e->local_buf_free;
		int ret;

		EV_FLIPPER(DOWNLOAD_TIME, SEND_INQUEUE_TIME, cpu_sock);
		// queries if recv buffer is available, QP is not filled
		if ((ret = rsend_available(cpu_sock, sbuf_bytes_avail, 0)) < len) {
			if (ret < 0 && errno == EAGAIN) {
				ret = -EAGAIN;
				goto send_out;
			}
			// if enough rsend is not possible, let CQ be handled
			ret = rs_handle_completion(cpu_sock, 0);
			if (!ret) {
				ret = rsend_available(cpu_sock, -1, 0);
			} else if (ret == -1 && errno == EWOULDBLOCK) {
				ret = -EWOULDBLOCK;
				goto send_out;
			} else {
				goto send_out;
			}
		}

		send_out:

		e->ret_val = ret;
		__sync_synchronize();
		e->status = CPU_IPC_READY;
		__sync_synchronize();

		break;
	}

	case RECV_POLL_IPC_REQ: {
		// this queries how much data is available at recv buffer
		int cpu_sock = e->cpu_sock;
		int len = e->data_size;

		assert(cpu_sock >= 0);

		int ret = ack_recved(cpu_sock, len, 0);

		if (ret <= 0 && errno == EWOULDBLOCK) {
			ret = -EWOULDBLOCK;
		}

		e->ret_val = ret;
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

	case KERNEL_SYNC_EXECUTE_REQ:
		ipc_kernel_sync_exec(e, task_array_slot);
		break;
	case KERNEL_ASYNC_INVOKE_REQ:
		ipc_kernel_async_exec(e, task_array_slot);
		break;
    default:
		assert(0);
	}
}

int GPUNETGlobals::start_hostloop(cudaStream_t kernel_stream)
{
	unsigned int count = 0;
	while (cudaStreamQuery(kernel_stream) == cudaErrorNotReady
		|| priv_->is_any_stream_busy()) {
		single_hostloop(kernel_stream);
		poll_gsend();

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

int GPUNETGlobals::single_hostloop(cudaStream_t kernel_stream)
{
	cpu_ipc_queue* ipc_q = gpu_globals->_h_ipc_queue;

	for (int i = 0; i < TASK_ARRAY_SIZE; i++) {
		if (ipc_q->entries[i].status != CPU_IPC_READY) {
#if ENABLE_DEBUG_SLOT_STATE
			int old = task_queue->task_array[i];
			int reqt = ipc_q->entries[i].req_type;
			int olds = ipc_q->entries[i].status;
#endif
			ipc_service(&ipc_q->entries[i], i);
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
	poll_gsend();

	return 0;
}

int GPUNETGlobals::poll_gsend() {
	::poll_backend_bufs();
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

static dim3 getdim3(volatile int d[3])
{
	for(int i = 0; i < 3; i++)
		if (d[i] == 0)
			d[i] = 1;
	return dim3(d[0], d[1], d[2]);
}

#define VERBOSEINFO(...)

void GPUNETGlobals::ipc_kernel_sync_exec(cpu_ipc_entry* e, int slot)
{
	int ret;
	int i;
	cudaError_t err;
	cudaStream_t stream;
	bool aval;
#if ENABLE_DEBUG_SLOT_STATE
	int task_array_slot = slot;
#endif

	if (task_queue->task_array[slot] == TASK_ARRAY_EMPTY) {
		const void* func = priv_->find_kernel((char*)e->func_name);
		if (!func) {
			ret = EKERN;
			fprintf(stderr, "Failed to find kernel %s\n", e->func_name);
			goto done;
		} else {
			VERBOSEINFO("KLaunch Find kernel %s: %p(%s)\n",
					e->func_name, func, (const char*)func);
		}

		VERBOSEINFO("Grid dims:");
		for (i = 0; i < 3; i++)
			VERBOSEINFO("\t%d", e->grid_dim[i]);
		VERBOSEINFO("\n");
		VERBOSEINFO("Block dims:");
		for (i = 0; i < 3; i++)
			VERBOSEINFO("\t%d", e->block_dim[i]);
		VERBOSEINFO("\n");
		dim3 gridconf = getdim3(e->grid_dim);
		dim3 blockconf = getdim3(e->block_dim);
		aval = priv_->find_stream(&stream);
		assert(aval == true);
		CUDA_SAFE_CALL(cudaConfigureCall(gridconf,
										 blockconf,
										 0UL,
										 stream));
		CUDA_SAFE_CALL(cudaSetupArgument((const void*)&(e->argument),
										 8, // Magical number. Assuming size of GPU pointers is 8
										 0));
		::cudaLaunch(func);
		cudaEventRecord(task_queue->cuda_events[slot], stream);
		task_queue->task_array[slot] = TASK_ARRAY_PENDING;
		DEBUG_SLOT_STATE(TASK_ARRAY_PENDING);
	}

#if 1
	err = cudaEventQuery(task_queue->cuda_events[slot]);
	if (err == cudaErrorNotReady) {
		return ;
	} else if (err == cudaSuccess) {
		ret = 0;
		goto done;
	} else {
		ret = -err;
		goto done;
	}
	return ;
#endif
done:
	task_queue->task_array[slot] = TASK_ARRAY_EMPTY;
	DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
	e->ret_val=ret;
	__sync_synchronize();
	e->status=CPU_IPC_READY;
	__sync_synchronize();
	DEBUG("KLaunch processed");
	return ;
}

void GPUNETGlobals::ipc_kernel_async_exec(cpu_ipc_entry* e, int slot)
{
	int ret = 0;
	cudaStream_t stream;
	bool aval;
#if ENABLE_DEBUG_SLOT_STATE
	int task_array_slot = slot;
#endif

	if (task_queue->task_array[slot] == TASK_ARRAY_EMPTY) {
		const void* func = priv_->find_kernel((char*)e->func_name);
		if (!func) {
			ret = EKERN;
			fprintf(stderr, "Failed to find kernel %s\n", e->func_name);
			goto done;
		} else {
			VERBOSEINFO("ALaunch Find kernel %s: %p(%s)\n",
					e->func_name, func, (const char*)func);
		}
		aval = priv_->find_stream(&stream);
		if (!aval) {
			fprintf(stderr, "%s: Falied to find a stream\n", __func__);
			exit(-1);
		}

		dim3 gridconf = getdim3(e->grid_dim);
		dim3 blockconf = getdim3(e->block_dim);
		CUDA_SAFE_CALL(cudaConfigureCall(gridconf,
					blockconf,
					0UL,
					stream));
		CUDA_SAFE_CALL(cudaSetupArgument((const void*)&(e->argument),
				8, // Assuming size of GPU pointers is 8
				0));
		CUDA_SAFE_CALL(::cudaLaunch(func));
	}
done:
	e->ret_val=ret;
	__sync_synchronize();
	e->status=CPU_IPC_READY;
	task_queue->task_array[slot] = TASK_ARRAY_EMPTY;
	DEBUG_SLOT_STATE(TASK_ARRAY_EMPTY);
	__sync_synchronize();
	DEBUG("ALaunch processed");
	return ;
}
