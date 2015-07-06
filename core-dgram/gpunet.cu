#include "net_globals_add.cu.h"
#include "ipc_shared.h"
#include "gpu_ipc.cu.h"
#include "net_constants.h"
#include "gpunet.cu.h"
#include "net_structures.cu.h"
#include "util.cu.h"

#include <assert.h>
#include <stdio.h>
#include <stdint.h>

#include <sys/socket.h> // AF_LOCAL
#include <poll.h> // POLL
#include <sys/param.h>

#include <rdma_dgram/rsocket.h>

#define O_NONBLOCK       00004
#define O_GPUNET_BOUNCE  04000
#ifndef EWOULDBLOCK
#define EWOULDBLOCK      11
#endif

#ifndef UINT32_MAX
#define UINT32_MAX 0xffffffffU
#endif

// this is dictated by rsocket.c
#define RS_SNDLOWAT UNIT_MSGBUF_SIZE

#define SOC_NONBLOCK(soc_e) ((soc_e)->e_flags & O_NONBLOCK)

__device__ int gsocket( int domain, int type, int protocol){
	int entry=g_soctable->findNewEntry();
	return entry;
}

__device__ inline void free_socket(int soc){
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	g_soctable->free_socket(soc);
}

__device__ void sync_ipc_bufs(gpu_socket *sock, cpu_ipc_entry *e) {
	sock->sbuf = (uint8_t*)readNoCache(&e->sbuf_addr);
	sock->sbuf_size = readNoCache(&e->sbuf_size);
	sock->sq_size = sock->sbuf_size / RS_SNDLOWAT;

#define get_value_from_ipc_entry(prefix, name, type) sock-> prefix ## _ ## name = (type)readNoCache(&e-> prefix ## _ ## name);
	get_value_from_ipc_entry(dev, sbuf_free_queue_back, int*);
	get_value_from_ipc_entry(dev, sbuf_free_queue, int*);
	get_value_from_ipc_entry(host, sbuf_req_queue, ds_smsg_info*);
	get_value_from_ipc_entry(dev, sbuf_req_back, int*);
	get_value_from_ipc_entry(host, sbuf_req_back, int*);

	sock->rbuf_size = readNoCache(&e->rbuf_size);
	sock->rq_size = sock->rbuf_size / RS_SNDLOWAT;
	
	get_value_from_ipc_entry(dev, rbuf_recved_queue, ds_rmsg_info*);
	get_value_from_ipc_entry(dev, rbuf_recved_back, int*);
	get_value_from_ipc_entry(host, rbuf_ack_queue_back, int*);
}

__device__ int ll_connect_in(int soc,
							 const struct sockaddr_in *addr,
							 socklen_t addrlen,
							 req_type_t req_type) {
	
	__shared__ int ret_val;
	__shared__ gpu_socket *sock;
	
	int entry;
	cpu_ipc_entry* e;
	GET_QUEUE_SLOT(entry,e);

	sock = &g_soctable->_sockets[soc];
	
	e->cpu_sock = sock->cpu_sock;
	e->req_type = CONNECT_IPC_REQ;
	e->data_size = addrlen;

	memcpy_thread((char*)&e->addr.in, (char*)addr, (int)addrlen);	
	
	ret_val = fire_and_wait(e);
	g_ipc_manager->freeEntry(entry);

	return ret_val;
}


__device__ int ll_bind_in(const struct sockaddr_in *addr,
						  socklen_t addrlen,
						  int sock_domain,
						  int sock_proto,
						  req_type_t req_type) {
	// all GPU failures should happen before getting to the CPU
	GPU_ASSERT(addr);

	__shared__ int ret_val;
	__shared__ int newsocket;
	__shared__ gpu_socket *sock;

	newsocket = gsocket(0,0,0);

	assert(newsocket != E_SOCTABLE_FULL);

	int entry;
	cpu_ipc_entry* e;
	GET_QUEUE_SLOT(entry,e);

	e->sock_domain = sock_domain;
	e->sock_proto = sock_proto;
	e->req_type = req_type; // BIND_IPC_REQ 
	e->cpu_sock = -1;

	memcpy_thread((char*)&e->addr.in, (char*)addr, (int)addrlen);

	ret_val = fire_and_wait(e);
	g_ipc_manager->freeEntry(entry);
	
	if (ret_val >= 0) {
		sock = &g_soctable->_sockets[newsocket];
		sock->cpu_sock = ret_val;
		
		// binding with the addr
		memcpy_thread((char*)sock->addr,
					  (char*)addr, sizeof(*addr));

		// get the buffers at bind/connect
		sync_ipc_bufs(sock, e);
		
		// sbuf_free_list_init(sock); // initialized on CPU. let's uncomment when CPU is too slow.
		sock->sbuf_free_queue_front = -1;
		sock->rbuf_recved_front = -1;
		sock->rbuf_ack_queue_back = 0;
		
		__threadfence(); // propagate to everybody
	} else {
		g_soctable->free_socket(newsocket);
		newsocket = ret_val;
	}
	
	return newsocket;
}

__device__ int single_thread_gbind_in(const sockaddr_in* addr, const socklen_t addrlen) {
	return ll_bind_in(addr, addrlen, AF_INET,
							  IPPROTO_IBP, BIND_IPC_REQ);
}

__device__ int single_thread_gbind_bounce_in(const sockaddr_in* addr, const socklen_t addrlen) {
	return ll_bind_in(addr, addrlen, AF_INET,
							  IPPROTO_IBP, BIND_IPC_REQ_BOUNCE);
}

__device__ int gconnect_in(int socket, const struct sockaddr_in* addr, const socklen_t addrlen) {
	__shared__ int retval;

	BEGIN_SINGLE_THREAD_PART {
		retval = ll_connect_in(socket, addr, addrlen, CONNECT_IPC_REQ);
	} END_SINGLE_THREAD_PART;
	
	return retval;
}

__device__ int gconnect_bounce_in(int socket, const struct sockaddr_in* addr, const socklen_t addrlen) {
	__shared__ int retval;

	BEGIN_SINGLE_THREAD_PART {
		retval = ll_connect_in(socket, addr, addrlen, CONNECT_IPC_REQ_BOUNCE);
	} END_SINGLE_THREAD_PART;

	return retval;
}

__device__ int ll_shutdown_close(int soc, req_type_t req, int how) {
	
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	__shared__  gpu_socket* soc_e;
	__shared__  int ret_val;
	
	int entry;
	cpu_ipc_entry* e;
	
	soc_e = &g_soctable->_sockets[soc];
	
	GET_QUEUE_SLOT(entry, e);
	assert(e->status!=CPU_IPC_PENDING);
	
	e->cpu_sock = soc_e->cpu_sock;
	e->shutdown_flags = how;
	e->req_type = req;
	
	ret_val=fire_and_wait(e);
	g_ipc_manager->freeEntry(entry);
	
	if (req == CLOSE_IPC_REQ)
		g_soctable->free_socket(soc);
	
	return ret_val;
}

__device__ int single_thread_gshutdown(int soc, int how) {
	return ll_shutdown_close(soc,SHUTDOWN_IPC_REQ,how);
}

__device__ int single_thread_gclose(int soc) {
	return ll_shutdown_close(soc,CLOSE_IPC_REQ,SHUT_RDWR);
}

#ifdef GPUNET_PROFILE
#define def_timer(n) __shared__ long long int _t[n];
#define set_timer(n) _t[(n)] = clock64();
#else
#define def_timer(n)
#define set_timer(n)
#endif

// single threaded. currently not used, but may be used in the future for optimization
/*
static void sbuf_free_list_init(gpu_socket *soc_e) {
	int count_msgs = soc_e->sbuf_size / RS_SNDLOWAT;
	for (int i = 0; i < count_msgs; i++) {
		soc_e->dev_sbuf_free_queue[i] = i;
	}
	soc_e->sbuf_free_queue_front = -1;
}
*/

// single threaded
__device__ static volatile uchar* sbuf_free_list_pop(gpu_socket *soc_e) {
	int index;
	// free_queue has size of sq_size + 1
	if (++soc_e->sbuf_free_queue_front == soc_e->sq_size + 1) {
		soc_e->sbuf_free_queue_front = 0;
	}
	index = soc_e->dev_sbuf_free_queue[soc_e->sbuf_free_queue_front];
	
	return soc_e->sbuf + RS_SNDLOWAT * index;
}

// single threaded
// front: the item lastly consumed (initially -1)
// back : the item to be produced (initially 0)
// empty : (front + 1 == back mod sqsize+1)
// filled: (front + rq_size == back or front == back)
__device__ static bool sbuf_free_list_empty(gpu_socket *soc_e) {
	int sbuf_free_queue_front = soc_e->sbuf_free_queue_front;
	int sbuf_free_queue_back = *soc_e->dev_sbuf_free_queue_back;
	return (sbuf_free_queue_front + 1 == sbuf_free_queue_back) ||
		((sbuf_free_queue_back == 0) && (sbuf_free_queue_front == soc_e->sq_size));
}

// get free buffer and mark it as used. this does not have to reach out the CPU,
// but the CPU updates the metadata used by this function.

// this function is the consumer of the free-list, and the producer is at the CPU.
// however, this function should not explictly communicate with the CPU. The CPU would update the list 

// called from single thread
 __device__ static volatile uchar* gsend_get_free_buffer(gpu_socket *soc_e) {
	bool is_empty;
	while ((is_empty = sbuf_free_list_empty(soc_e)) && !SOC_NONBLOCK(soc_e));
	if (is_empty) return NULL;
	return sbuf_free_list_pop(soc_e);
}

// this is the producer of the buffer to be transferred. the consumer is at the CPU,
// not single threaded, but internally single-threaded
__device__ void gsend_notify_cpu(gpu_socket *soc_e, volatile uint8_t *buf, int size, sockaddr *addr, socklen_t addrlen) {
	__shared__ volatile ds_smsg_info *msg_info;
	BEGIN_SINGLE_THREAD_PART {
		msg_info = &soc_e->host_sbuf_req_queue[*soc_e->dev_sbuf_req_back];
		msg_info->msg_index = (buf - soc_e->sbuf) / RS_SNDLOWAT;
		msg_info->length = (uint16_t)size;
		msg_info->addrlen = (uint16_t)addrlen;
	} END_SINGLE_THREAD_PART;
	
	copy_block_dst_volatile(msg_info->addr, (uchar*)addr, (int)addrlen);

	__threadfence_system();
	
	BEGIN_SINGLE_THREAD_PART {	
		// the queue should be never filled because gsend_get_free_buffer() limits the max inflight messages to be less than or equal to sq_size
		// this condition is the reason why sbuf_req_queue is of size sq_size + 1 (capacity = sq_size)
		if (++(*soc_e->dev_sbuf_req_back) == soc_e->sq_size + 1) {
			*soc_e->dev_sbuf_req_back = 0;
		}
		*soc_e->host_sbuf_req_back = *soc_e->dev_sbuf_req_back;
	} END_SINGLE_THREAD_PART;
}

__device__ int gsendto(int soc, uchar* to_send, int size, struct sockaddr *destaddr, int addrlen) {
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	GPU_ASSERT(to_send && size);
	
	__shared__ gpu_socket* soc_e;
	__shared__ volatile uchar *buf;

	BEGIN_SINGLE_THREAD_PART {
		soc_e=&g_soctable->_sockets[soc];
		
		// 1) get the availalble message buffer index from the free list
		buf = gsend_get_free_buffer(soc_e);
	} END_SINGLE_THREAD_PART;

	if (size != 0) {
		if (buf == NULL) {
			return -EWOULDBLOCK;
		}
		
		// 2) copy the buffer to the sbuf
		copy_block_dst_volatile(buf, to_send, size);
								
		// 3) then notify the buffer use to the CPU proxy
		gsend_notify_cpu(soc_e, buf, size, destaddr, addrlen);
	}

	return size;
}

__device__ static bool __grecv_is_empty(gpu_socket *soc_e, int rbuf_recved_front) {
	int rbuf_recved_back = *soc_e->dev_rbuf_recved_back;
	int rq_size = soc_e->rq_size;

	return (rbuf_recved_front + 1) == rbuf_recved_back || (rbuf_recved_front == rbuf_recved_back + rq_size);
}

__device__ static bool grecv_is_empty(gpu_socket *soc_e) {
	return __grecv_is_empty(soc_e, soc_e->rbuf_recved_front);
}

// called from single_thread. returns 0 when nothing is available
// this is the consumer for rbuf buffer. the producer is in the rsocket_dgram.
__device__ static int grecv_check(gpu_socket *soc_e, struct sockaddr *addr, socklen_t *addrlen, uint8_t** msgbuf) {

	// front: the item lastly consumed (initially -1)
	// back : the item to be produced (initially 0)
	// empty : (front + 1 == back mod rqsize)
	// filled: (front + rq_size == back or front == back)

	__shared__ int rbuf_recved_front;
	__shared__ volatile ds_rmsg_info *rmsg;
	__shared__ int msglen;
		
	BEGIN_SINGLE_THREAD_PART {
		rbuf_recved_front = soc_e->rbuf_recved_front;
		
		if (__grecv_is_empty(soc_e, rbuf_recved_front)) {
			rmsg = NULL;
		} else {
			// progress the front pointer before accessing.
			if (++rbuf_recved_front == soc_e->rq_size + 1) {
				rbuf_recved_front = 0;
			}
			
			rmsg = &soc_e->dev_rbuf_recved_queue[rbuf_recved_front];

			msglen = rmsg->length;
			*addrlen = (socklen_t)rmsg->addrlen;
			*msgbuf = (uint8_t*)rmsg->ptr;
		}
	
	} END_SINGLE_THREAD_PART;

	if (rmsg == NULL)
		return 0;

	copy_block_dst_volatile((uchar*)addr, (uchar*)rmsg->addr, (size_t)*addrlen);

	BEGIN_SINGLE_THREAD_PART {
		soc_e->rbuf_recved_front = rbuf_recved_front;
	} END_SINGLE_THREAD_PART;

	return msglen;
}

// single threaded
__device__ static void grecv_ack(gpu_socket *soc_e) {
	__threadfence_system();
	if (++soc_e->rbuf_ack_queue_back == soc_e->rq_size + 1) {
		soc_e->rbuf_ack_queue_back = 0;
	}
	*soc_e->host_rbuf_ack_queue_back = soc_e->rbuf_ack_queue_back;
}


__device__ int grecvfrom(int soc, void* buf, int size, struct sockaddr *addr, socklen_t *addrlen) {
	GPU_ASSERT(soc>=0 && soc< SOC_TABLE_SIZE);
	GPU_ASSERT(buf&&size);
	__shared__  gpu_socket* soc_e;
	__shared__  int ret_val;
	__shared__  uint8_t *msgbuf;
	
	BEGIN_SINGLE_THREAD_PART {
		soc_e = &g_soctable->_sockets[soc];
	} END_SINGLE_THREAD_PART;
	
	// 1) see if there is any new data by accessing the CPU updated buffer (get the size and addr!)
	
	do {
		ret_val = grecv_check(soc_e, addr, addrlen, &msgbuf);
	} while (ret_val == 0 && !SOC_NONBLOCK(soc_e));
	
	if (ret_val == 0) {
		return -EWOULDBLOCK;
	}
	assert(ret_val > 0);

	// 3) copying the data to the buffer
	copy_block_src_volatile((uchar*)buf, msgbuf, ret_val);

	BEGIN_SINGLE_THREAD_PART {
		// acking the cpu is necessary to free the buffer
		grecv_ack(soc_e);
	} END_SINGLE_THREAD_PART;

	return ret_val;
}

__device__ int gpoll(struct gpollfd* fds, size_t nfds, int nclock_timeout)
{
	__shared__ long long clock_start;
	__shared__ size_t nth;
	__shared__ bool is_timeout;
	__shared__ int may_return;
	BEGIN_SINGLE_THREAD_PART {
		clock_start = clock64();
		is_timeout = false;
		nth = blockDim.x * blockDim.y * blockDim.z;
		may_return = 0;
	} END_SINGLE_THREAD_PART;
	
	do {
		for(int i = TID; i < nfds; i+= nth) {
			struct gpollfd pfd = fds[i];
			pfd.revents = 0;
			pfd.rbytes = 0;
			pfd.wbytes = 0;

			int soc = pfd.fd;
			if (soc < 0 || soc >= SOC_TABLE_SIZE) {
				// invalid socket.
				pfd.revents |= POLLNVAL;
			} else {
				// common case: the valid socket.
				gpu_socket *soc_e = &g_soctable->_sockets[soc];
				if (!grecv_is_empty(soc_e)) {
					pfd.revents |= POLLIN;
					pfd.rbytes = RS_SNDLOWAT;
					may_return = 1;
				}
				if (!sbuf_free_list_empty(soc_e)) {
					pfd.revents |= POLLOUT;
					pfd.wbytes = RS_SNDLOWAT;
					may_return = 1;
				}
			}
			pfd.revents &= pfd.events | POLLERR | POLLNVAL | POLLHUP;
			fds[i] = pfd;
		}
		BEGIN_SINGLE_THREAD_PART {
			if (nclock_timeout > 0 && clock64() - clock_start > nclock_timeout)
				is_timeout = true;
		} END_SINGLE_THREAD_PART;
	} while (may_return == 0 && !is_timeout);
	return may_return;
}

__device__ void gsetsock_block(int socket, int blocking) {
	__shared__  gpu_socket* soc_e;

	BEGIN_SINGLE_THREAD_PART {

		soc_e = &g_soctable->_sockets[socket];
		if (!blocking) {
			soc_e->e_flags |= O_NONBLOCK;
		} else {
			soc_e->e_flags &= (~O_NONBLOCK);
		}

	} END_SINGLE_THREAD_PART;

}

__device__ void gsetsock_bounce(int socket, int bounce) {
	__shared__  gpu_socket* soc_e;

	BEGIN_SINGLE_THREAD_PART {

		soc_e = &g_soctable->_sockets[socket];
		if (bounce)
			soc_e->e_flags |= O_GPUNET_BOUNCE;

	} END_SINGLE_THREAD_PART;

}

__device__ int ggetsock_block(int socket) {
	__shared__ gpu_socket* soc_e;
	__shared__ int ret;

	BEGIN_SINGLE_THREAD_PART {

		soc_e = &g_soctable->_sockets[socket];
		ret = (soc_e->e_flags & O_NONBLOCK);

	} END_SINGLE_THREAD_PART;

	return (!ret);
}

__device__ void ggettimeofday(struct gtimeval *tv) {
	__shared__  int ipc_slot;
	__shared__ cpu_ipc_entry* e;

	BEGIN_SINGLE_THREAD_PART {
		GET_QUEUE_SLOT(ipc_slot,e);

		e->req_type = LOG_TIMESTAMP_REQ;
		fire_and_wait(e);

		tv->tv_sec = e->tv_sec;
		tv->tv_usec = e->tv_usec;

		g_ipc_manager->freeEntry(ipc_slot);

	} END_SINGLE_THREAD_PART;
}

__device__ void gtimersub(struct gtimeval *a, struct gtimeval *b, struct gtimeval *c) {
	c->tv_sec = 0;
	c->tv_usec = a->tv_usec - b->tv_usec;
	if (c->tv_usec < 0) {
		c->tv_usec += 1000000;
		c->tv_sec = -1;
	}

	c->tv_sec += (a->tv_sec - b->tv_sec);
}

__device__ void gputs_single(const char* str, int len) {
	__shared__  int ipc_slot;
	__shared__ cpu_ipc_entry* e;

	GET_QUEUE_SLOT(ipc_slot,e);
	// use local for copying string
	e->sock_domain = blockIdx.z * (blockDim.x * blockDim.y) + blockIdx.y * blockDim.x + blockIdx.x;
	e->req_type = PUTS_REQ;
	
	int len_copy = (len < GPU_LOCAL_SOC_MAX_PATH) ? len : GPU_LOCAL_SOC_MAX_PATH;
	
	strncpy_thread(e->addr.local, str, len_copy);

	e->addr.local[len_copy-1] = '\0';
	
	fire_and_wait(e);

	g_ipc_manager->freeEntry(ipc_slot);
}

__device__ void gputs_single(const char* str, int len, unsigned int* threads) {
	gputs_single(str, len);
	for (int i = 0; i < blockDim.x; i++) {
		if (!(threads[i >> 5] & (1 << (i & 31)))) {
			gprintf4_single("thread %d missing\n", i, 0, 0, 0);
		}
	}
}

__device__ int ui2a(unsigned int num, unsigned int base,char * bf)
{
    int n=0;
    unsigned int d=1;
    while (num/d >= base)
        d*=base;        
    while (d!=0) {
        int dgt = num / d;
        num%= d;
        d/=base;
        if (n || dgt>0 || d==0) {
            *bf++ = dgt+(dgt<10 ? '0' : 'a'-10);
            ++n;
		}
	}
    *bf=0;
	return n;
}

__device__ int i2a (int num, char * bf)
{
    if (num<0) {
        num=-num;
        *bf++ = '-';
	}
    return ui2a(num,10,bf);
}

// printf for one integer
__device__ void gprintf4_single(const char* str, int arg1, int arg2, int arg3, int arg4) {
	__shared__  int ipc_slot;
	__shared__ cpu_ipc_entry* e;
	char* buf, ch;
	__shared__ char bf[12];

	assert(threadIdx.x == 0);

	GET_QUEUE_SLOT(ipc_slot,e);
	// use local for copying string
	e->sock_domain = blockIdx.z * (blockDim.x * blockDim.y) + blockIdx.y * blockDim.x + blockIdx.x;
	e->req_type = PUTS_REQ;

	buf = (char*)e->addr.local;
	int cnt = 0, len_copy;
	while ((ch=*(str++)) && (buf < (e->addr.local + GPU_LOCAL_SOC_MAX_PATH - 1))) {
		if (ch!='%') {
			*buf = ch;
			buf++;
		} else {
			ch=*(str++);
			
			if (ch == 'd'){
				switch(cnt) {
				case 0:
					len_copy = i2a(arg1, bf);
					break;
				case 1:
					len_copy = i2a(arg2, bf);
					break;
				case 2:
					len_copy = i2a(arg3, bf);
					break;
				case 3:
					len_copy = i2a(arg4, bf);
					break;
				default:
					len_copy = i2a(arg4, bf);
					break;
				}
				strncpy_thread(buf, bf, len_copy);
				buf += len_copy;
				
			} else if (ch == 'x') {
				switch(cnt) {
				case 0:
					len_copy = ui2a(arg1, 16, bf);
					break;
				case 1:
					len_copy = ui2a(arg2, 16, bf);
					break;
				case 2:
					len_copy = ui2a(arg3, 16, bf);
					break;
				case 3:
					len_copy = ui2a(arg4, 16, bf);
					break;
				default:
					len_copy = ui2a(arg4, 16, bf);
					break;
				}
				strncpy_thread(buf, bf, len_copy);
				buf += len_copy;
			}
			
			if (ch != '%')
				cnt++;
		}
	}
	*buf = '\0';
	
	fire_and_wait(e);

	g_ipc_manager->freeEntry(ipc_slot);
}

__device__ void gputs(const char* str, int len) {
	__shared__ unsigned int threads[32];

	atomicOr(&threads[threadIdx.x >> 5], (1 << (threadIdx.x & 31))); 
	
	BEGIN_SINGLE_THREAD_PART {
		gprintf4_single(str, 0, 0, 0, 0);
	} END_SINGLE_THREAD_PART;
}

__device__ int ggetcpusock(int sock) {
	gpu_socket *soc_e =&g_soctable->_sockets[sock];
	return soc_e->cpu_sock;
}