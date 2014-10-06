#include "net_globals_add.cu.h"
#include "ipc_shared.h"
#include "gpu_ipc.cu.h"
#include "net_constants.h"
#include "gpunet.cu.h"
#include "net_structures.cu.h"
#include "util.cu.h"
#include "ringbuf.cu.h"

#include <assert.h>
#include <stdio.h>
#include <stdint.h>

#include <sys/socket.h> // AF_LOCAL
#include <poll.h> // POLL
#include <sys/param.h>

#define O_NONBLOCK       00004
#define O_GPUNET_BOUNCE  04000
#define EWOULDBLOCK      11

#ifndef UINT32_MAX
#define UINT32_MAX 0xffffffffU
#endif

#define SOC_NONBLOCK(soc_e) ((soc_e)->e_flags & O_NONBLOCK)

__device__ int gsocket( int domain, int type, int protocol){
	int entry=g_soctable->findNewEntry();
	return entry;
}

__device__ inline void free_socket(int soc){
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	g_soctable->free_socket(soc);
}

// MUST BE CALLED FROM A SINGLE THREAD!!
__device__ int ll_bind_connect(const char* name, int namelen, req_type_t req_type) {

	// all GPU failures should happen before getting to the CPU
	GPU_ASSERT(name);

	__shared__ int ret_val;
	__shared__ int newsocket;
	__shared__ gpu_socket *sock;

	newsocket=gsocket(0,0,0);

	assert(newsocket != E_SOCTABLE_FULL);

	int entry;
	cpu_ipc_entry* e;

	GET_QUEUE_SLOT(entry, e);

	e->sock_domain=AF_LOCAL;
	e->sock_proto = 0;
	e->req_type = req_type;
	e->cpu_sock = -1;

	memcpy_thread((char*)e->addr.local, name, namelen + 1);

	ret_val = fire_and_wait(e);
	g_ipc_manager->freeEntry(entry);

	sock = &g_soctable->_sockets[newsocket];
	sock->cpu_soc = ret_val;

	memcpy_thread((char*)sock->addr,
				  (char*)name, namelen+1);

	__threadfence(); // propagate to everybody

	if (ret_val<0)  {
		g_soctable->free_socket(newsocket);
		newsocket = ret_val;
	}

	return newsocket;
}

__device__ int ll_bind_connect_in(const struct sockaddr_in *addr,
                                  int sock_domain,
                                  int sock_proto,
                                  req_type_t req_type) {
	// all GPU failures should happen before getting to the CPU
	GPU_ASSERT(addr);

	__shared__ int ret_val;
	__shared__ int newsocket;
	__shared__ gpu_socket *sock;

	newsocket=gsocket(0,0,0);

	assert(newsocket != E_SOCTABLE_FULL);

	int entry;
	cpu_ipc_entry* e;
	GET_QUEUE_SLOT(entry,e);

	e->sock_domain= sock_domain;
	e->sock_proto = sock_proto;
	e->req_type=req_type;
	e->cpu_sock=-1;

	memcpy_thread((char*)&e->addr.in, (char*)addr, sizeof(*addr));

	ret_val=fire_and_wait(e);
	g_ipc_manager->freeEntry(entry);

	if (ret_val >= 0) {
		sock = &g_soctable->_sockets[newsocket];
		sock->cpu_soc=ret_val;

		if ((req_type == CONNECT_IPC_REQ) || req_type == CONNECT_IPC_REQ_BOUNCE) {

			sock->sbuf = (g_ringbuf::ringbuf_t)readNoCache(&e->sbuf_addr);
			sock->rbuf = (uint8_t*)readNoCache(&e->rbuf_addr);
			sock->rbuf_offset = (uint64_t*)readNoCache(&e->dev_rbuf_offset);
			sock->rbuf_bytes_avail = (uint64_t*)readNoCache(&e->dev_rbuf_bytes_avail);
			sock->rbuf_size = readNoCache(&e->rbuf_size);
			sock->rbuf_bytes_avail_cache = 0;
		}

		memcpy_thread((char*)g_soctable->_sockets[newsocket].addr,
					  (char*)addr, sizeof(*addr));
		
		__threadfence(); // propagate to everybody
	} else {
		g_soctable->free_socket(newsocket);
		newsocket=ret_val;
	}

	return newsocket;
}

__device__ int single_thread_gbind(const char* name, int namelen) {
	return ll_bind_connect(name,namelen,BIND_IPC_REQ);
}

__device__ int single_thread_gbind_in(const sockaddr_in* addr) {
	return ll_bind_connect_in(addr, AF_INET,
							  IPPROTO_IBP, BIND_IPC_REQ);
}

__device__ int single_thread_gbind_bounce_in(const sockaddr_in* addr) {
	return ll_bind_connect_in(addr, AF_INET,
							  IPPROTO_IBP, BIND_IPC_REQ_BOUNCE);
}

__device__ int gconnect(const char* name, int namelen) {
	__shared__ int retval;

	BEGIN_SINGLE_THREAD_PART {
		retval=ll_bind_connect(name,namelen,CONNECT_IPC_REQ);
	} END_SINGLE_THREAD_PART;

	return retval;
}

__device__ int gconnect_in(const struct sockaddr_in* addr) {
	__shared__ int retval;

	BEGIN_SINGLE_THREAD_PART {
		retval = ll_bind_connect_in(addr, AF_INET,
									IPPROTO_IBP, CONNECT_IPC_REQ);
	} END_SINGLE_THREAD_PART;


	return retval;
}

__device__ int gconnect_bounce_in(const struct sockaddr_in* addr) {
	__shared__ int retval;

	BEGIN_SINGLE_THREAD_PART {
		retval = ll_bind_connect_in(addr, AF_INET,
									IPPROTO_IBP, CONNECT_IPC_REQ_BOUNCE);

	} END_SINGLE_THREAD_PART;

	return retval;
}

__device__ int gconnect_ib(const struct sockaddr_in* addr) {
	return gconnect_in(addr);
}

__device__ int gaccept(int soc) {
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	__shared__  int newsocket;
	__shared__   gpu_socket *serv_soc_e, *soc_e;
	__shared__  int ret_val;

	BEGIN_SINGLE_THREAD_PART {
		serv_soc_e=&g_soctable->_sockets[soc];

		// init the new client socket
		newsocket=gsocket(0,0,0);
		soc_e=&g_soctable->_sockets[newsocket];

		int entry;
		cpu_ipc_entry* e;
		GET_QUEUE_SLOT(entry,e);

		e->cpu_sock=serv_soc_e->cpu_soc; // server socket
		e->req_type=ACCEPT_IPC_REQ;

		ret_val=fire_and_wait(e);

		soc_e->cpu_soc=ret_val;

		soc_e->sbuf = (g_ringbuf::ringbuf_t)readNoCache(&e->sbuf_addr);
		soc_e->rbuf = (uint8_t*)readNoCache(&e->rbuf_addr);
		soc_e->rbuf_offset = (uint64_t*)readNoCache(&e->dev_rbuf_offset);
		soc_e->rbuf_bytes_avail = (uint64_t*)readNoCache(&e->dev_rbuf_bytes_avail);
		soc_e->rbuf_size = readNoCache(&e->rbuf_size);
		soc_e->rbuf_bytes_avail_cache = 0;

		__threadfence();

		g_ipc_manager->freeEntry(entry);
		if (ret_val < 0) {
			GPU_ASSERT(ret_val>=0);
			g_soctable->free_socket(newsocket);
		}
	} END_SINGLE_THREAD_PART;

	if (ret_val<0)
		return ret_val;
	return newsocket;
}

__device__ int gaccept_nb(int soc)
{
	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	__shared__  int newsocket;
	__shared__   gpu_socket *serv_soc_e, *soc_e;
	__shared__  int ret_val;

	BEGIN_SINGLE_THREAD_PART {
		serv_soc_e=&g_soctable->_sockets[soc];

		// init the new client socket
		newsocket=gsocket(0,0,0);
		soc_e=&g_soctable->_sockets[newsocket];

		int entry;
		cpu_ipc_entry* e;
		GET_QUEUE_SLOT(entry,e);

		e->cpu_sock=serv_soc_e->cpu_soc; // server socket
		e->req_type = ACCEPT_NB_IPC_REQ;

		ret_val=fire_and_wait(e);
		g_ipc_manager->freeEntry(entry);

		soc_e->cpu_soc=ret_val;

		soc_e->sbuf = (g_ringbuf::ringbuf_t)readNoCache(&e->sbuf_addr);
		soc_e->rbuf = (uint8_t*)readNoCache(&e->rbuf_addr);
		soc_e->rbuf_offset = (uint64_t*)readNoCache(&e->dev_rbuf_offset);
		soc_e->rbuf_bytes_avail = (uint64_t*)readNoCache(&e->dev_rbuf_bytes_avail);
		soc_e->rbuf_size = readNoCache(&e->rbuf_size);

		__threadfence();
	} END_SINGLE_THREAD_PART;

	if (ret_val<0)  {
		g_soctable->free_socket(newsocket);
		GPU_ASSERT(ret_val>=0);
		return ret_val;
	}
	return newsocket;
}

__device__ int ll_shutdown_close(int soc, req_type_t req, int how) {

	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	__shared__  gpu_socket* soc_e;
	__shared__  int ret_val;

	int entry;
	cpu_ipc_entry* e;

	soc_e=&g_soctable->_sockets[soc];

	// make sure that send ringbuf is all consumed.
	// otherwise, close message may be sent before send requests, though the user requested send first.
	if (soc_e->sbuf)
		while (g_ringbuf::ringbuf_bytes_used(soc_e->sbuf) != 0);
	
	GET_QUEUE_SLOT(entry,e);
	assert(e->status!=CPU_IPC_PENDING);
	e->cpu_sock=soc_e->cpu_soc; // closing
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

#ifdef DEFAULT_TO_UNIX_SOCKET
__device__ int gsend(int soc, uchar* to_send, int size)
{
	GPU_ASSERT(soc>=0 && soc< SOC_TABLE_SIZE);
	GPU_ASSERT(to_send&&size);
	__shared__ gpu_socket* soc_e;
	__shared__  int ret_val, ipc_slot,total_sent;
	__shared__ cpu_ipc_entry* e;
	
BEGIN_SINGLE_THREAD

		GET_QUEUE_SLOT(ipc_slot,e);
        	assert(e->status!=CPU_IPC_PENDING);

END_SINGLE_THREAD

	soc_e=&g_soctable->_sockets[soc];
// copy to the socket buffer
	total_sent=0;

	while(total_sent!=size){

		int single_send=min(size-total_sent,CPU_IO_BUF_SIZE);
		
		 copy_block((uchar*)g_buffer_space+soc_e->send_buffer_offset,(uchar*)to_send+total_sent,single_send);
		 __threadfence(); // make sure everything reached the main memory

BEGIN_SINGLE_THREAD

	        e->cpu_sock=soc_e->cpu_soc;
	        e->req_type=SEND_IPC_REQ;
	        e->data_size=single_send;

        	e->req_buffer_offset=soc_e->send_buffer_offset;
 
		ret_val=fire_and_wait(e);

		if (ret_val>=0) total_sent+=ret_val;
END_SINGLE_THREAD
		if (ret_val<0) { total_sent=ret_val; break;}
		if (ret_val==0) break;
	}

BEGIN_SINGLE_THREAD
		g_ipc_manager->freeEntry(ipc_slot);     
END_SINGLE_THREAD
	return total_sent;

}
#else

#ifdef GPUNET_PROFILE_SEND
__device__ unsigned int __cnt = 0;
#endif

__device__ int gsend(int soc, uchar* to_send, int size) {

	GPU_ASSERT((soc >= 0) && (soc < SOC_TABLE_SIZE));
	GPU_ASSERT(to_send && size);
	
	__shared__ size_t send_size;
	__shared__ gpu_socket* soc_e;
	
#ifdef GPUNET_PROFILE_SEND
	def_timer(4);
#endif
	
	BEGIN_SINGLE_THREAD_PART {
#ifdef GPUNET_PROFILE_SEND
		_t[0] = _t[1] = _t[2] = _t[3] = 0;

#endif
		soc_e=&g_soctable->_sockets[soc];
	
		// checking recv buffer is necessary, so calling to CPU is
		// inevitable.

#ifdef GPUNET_PROFILE_SEND		
		set_timer(0);
#endif

        if (size != 0) {
          do {
            send_size = g_ringbuf::ringbuf_bytes_free(soc_e->sbuf);
          } while(!SOC_NONBLOCK(soc_e) && send_size == 0);
        }
        
		send_size = MIN(send_size, size);

#ifdef GPUNET_PROFILE_SEND
		set_timer(1);
#endif

	} END_SINGLE_THREAD_PART;

    if (send_size == 0)
      return -EWOULDBLOCK;
    
	// send_size is the expected size to be sent. though the actual send
	// size may be increased after CQ handling, we can safely ignore the increase unless send_size is 0.
	if (send_size != 0) {
#ifdef GPUNET_PROFILE_SEND
		g_ringbuf::ringbuf_memcpy_into(soc_e->sbuf,
				to_send, send_size, &send_size, 0);

		if (FIRST_THREAD_IN_BLOCK()) {
			set_timer(2);
			g_ringbuf::ringbuf_produce(soc_e->sbuf, send_size);
		}

#else
		g_ringbuf::ringbuf_memcpy_into(soc_e->sbuf,
				to_send, send_size, &send_size, 1);
#endif
	}

#ifdef GPUNET_PROFILE_SEND
	if (FIRST_THREAD_IN_BLOCK()) {
		if (((_t[0] % 101) == 0)) {
			set_timer(3);
#define t_diff(n) (_t[(n)] - _t[(n-1)])
			printf("1: %ld\t2: %ld\t3: %ld\tsend: %d, offset: %lu, free: %lu\n",
				   t_diff(1),
				   t_diff(2),
				   t_diff(3),
				   send_size,
				   g_ringbuf::ringbuf_head_offset(soc_e->sbuf),
				   g_ringbuf::ringbuf_bytes_free(soc_e->sbuf)
				);
		}
	}
#endif

	return send_size;

}
#endif

#ifdef DEFAULT_TO_UNIX_SOCKET
__device__ int grecv(int soc, uchar* to_recv, int size)
{
	GPU_ASSERT(soc>=0 && soc< SOC_TABLE_SIZE);
	GPU_ASSERT(to_recv&&size);
	__shared__  gpu_socket* soc_e;
	__shared__  int ret_val, ipc_slot,total_recv;
	__shared__ cpu_ipc_entry* e;
	
BEGIN_SINGLE_THREAD

		GET_QUEUE_SLOT(ipc_slot,e);
        	assert(e->status!=CPU_IPC_PENDING);

END_SINGLE_THREAD

	soc_e=&g_soctable->_sockets[soc];
// copy to the socket buffer
	total_recv=0;

	while(total_recv<size) {

		int single_recv=min(size-total_recv,CPU_IO_BUF_SIZE);
		

BEGIN_SINGLE_THREAD

	        e->cpu_sock=soc_e->cpu_soc;
	        e->req_type=RECV_IPC_REQ;
	        e->data_size=single_recv;
        	e->req_buffer_offset = soc_e->recv_buffer_offset;
 
		ret_val=fire_and_wait(e);

		if (ret_val>=0) {total_recv+=ret_val;}
END_SINGLE_THREAD
		if (ret_val<0) { total_recv=ret_val; break;}
		if (ret_val==0) break;

		copy_block((uchar*)to_recv+total_recv-ret_val,
				(uchar*)g_buffer_space+soc_e->recv_buffer_offset,
				ret_val);
	}

BEGIN_SINGLE_THREAD
	g_ipc_manager->freeEntry(ipc_slot);     
END_SINGLE_THREAD

	return total_recv;

}


__device__ int grecv_nb(int soc, void* to_recv, int size)
{
	GPU_ASSERT(soc>=0 && soc< SOC_TABLE_SIZE);
	GPU_ASSERT(to_recv&&size);
	__shared__ gpu_socket* soc_e;
	__shared__ int ret_val, ipc_slot,total_recv;
	__shared__ cpu_ipc_entry* e;
	
BEGIN_SINGLE_THREAD

		GET_QUEUE_SLOT(ipc_slot,e);
        	assert(e->status!=CPU_IPC_PENDING);

END_SINGLE_THREAD

	soc_e = &g_soctable->_sockets[soc];
// copy to the socket buffer
	total_recv = 0;

	while (total_recv < size) {

		int single_recv = min(size-total_recv, CPU_IO_BUF_SIZE);

BEGIN_SINGLE_THREAD
	        e->cpu_sock = soc_e->cpu_soc;
	        e->req_type = RECV_NB_IPC_REQ;
	        e->data_size = single_recv;
        	e->req_buffer_offset = soc_e->recv_buffer_offset;
 
		ret_val = fire_and_wait(e);

		if (ret_val >= 0)
			total_recv += ret_val;
		if (total_recv == 0)
			total_recv = ret_val;
END_SINGLE_THREAD
		if (ret_val <= 0)
			break;

		copy_block((uchar*)to_recv + total_recv - ret_val,
			   (uchar*)g_buffer_space+soc_e->recv_buffer_offset,
			   ret_val);
	}

BEGIN_SINGLE_THREAD
	g_ipc_manager->freeEntry(ipc_slot);     
END_SINGLE_THREAD

	return total_recv;
}

#else
__device__ int grecv(int soc, uchar* to_recv, int size) {

	GPU_ASSERT(soc>=0 && soc< SOC_TABLE_SIZE);
	GPU_ASSERT(to_recv&&size);
	__shared__  gpu_socket* soc_e;
	__shared__  int ret_val, end_size, rbuf_size;
	__shared__ uint64_t rbuf_offset, rbuf_bytes_avail;

#ifdef GPUNET_PROFILE_RECV
	def_timer(6);
#endif

	BEGIN_SINGLE_THREAD_PART {
#ifdef GPUNET_PROFILE_RECV
		_t[0] = _t[1] = _t[2] = _t[3] = _t[4] = _t[5] = 0;

		set_timer(0);
#endif
		soc_e = &g_soctable->_sockets[soc];

		ret_val = 0;
		rbuf_size = soc_e->rbuf_size;
		rbuf_bytes_avail = soc_e->rbuf_bytes_avail_cache;
		
		do {
			rbuf_offset = *(soc_e->rbuf_offset);
			if (rbuf_offset > rbuf_bytes_avail) {
				ret_val = (rbuf_offset - rbuf_bytes_avail);
			} else if (rbuf_offset < rbuf_bytes_avail) {
				// rbuf_offset is overflown
				ret_val = (UINT32_MAX - rbuf_bytes_avail) + rbuf_offset + 1;
			}
		} while(!SOC_NONBLOCK(soc_e) && ret_val == 0);

#ifdef GPUNET_PROFILE_RECV
		set_timer(1);
#endif

		end_size = rbuf_size - (rbuf_bytes_avail % rbuf_size);
		ret_val = MIN(ret_val, size);

#ifdef GPUNET_PROFILE_RECV
		set_timer(2);
#endif
	} END_SINGLE_THREAD_PART;

	if (ret_val == 0) {
		return -EWOULDBLOCK;
	}

	if (ret_val > end_size) {
		// rbuf_size should be the power of 2.
		copy_block_src_volatile(to_recv, &soc_e->rbuf[rbuf_bytes_avail % rbuf_size], end_size);
		copy_block_src_volatile(to_recv + end_size, soc_e->rbuf, ret_val - end_size);
	} else {
		copy_block_src_volatile(to_recv, &soc_e->rbuf[rbuf_bytes_avail % rbuf_size], ret_val);
	}

	BEGIN_SINGLE_THREAD_PART {
#ifdef GPUNET_PROFILE_RECV
		set_timer(3);
#endif
		// indefinitely increases, and even wraps around.
		soc_e->rbuf_bytes_avail_cache += ret_val;
		*soc_e->rbuf_bytes_avail = soc_e->rbuf_bytes_avail_cache;

#ifdef GPUNET_PROFILE_RECV
		set_timer(4);
#endif

#ifdef GPUNET_PROFILE_RECV
		set_timer(5);

#define t_diff(n) (_t[(n)] - _t[(n-1)])
		printf("1: %ld\t2: %ld\t3: %ld\t4: %ld\t5: %ld\ttotal: %ld\n",
			   t_diff(1),
			   t_diff(2),
			   t_diff(3),
			   t_diff(4),
			   t_diff(5),
			   _t[5] - _t[0]);
#endif
	} END_SINGLE_THREAD_PART;

	return ret_val;
}
#endif

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
				pfd.revents |= POLLNVAL;
			} else {
				gpu_socket *soc_e = &g_soctable->_sockets[soc];
				int buf_size = soc_e->rbuf_size;
				uint64_t buf_avlb = soc_e->rbuf_bytes_avail_cache;
				uint64_t buf_offset = *(soc_e->rbuf_offset);
				int op_size = 0;
				if (buf_offset > buf_avlb) {
					op_size = (buf_offset - buf_avlb);
				} else if (buf_offset < buf_avlb) {
					op_size = (UINT32_MAX - buf_avlb) +
						  buf_offset + 1;
				}
				if (op_size > 0) {
					pfd.revents |= POLLIN;
					pfd.rbytes = op_size;
					may_return = 1;
				}
				op_size = g_ringbuf::ringbuf_bytes_free(soc_e->sbuf);
				if (op_size > 0) {
					pfd.revents |= POLLOUT;
					pfd.wbytes = op_size;
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
		if (!blocking)
			soc_e->e_flags |= O_NONBLOCK;

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


static inline
__device__ int __glaunch(req_type_t req,
		const char* func_name,
		int grid_dim[3],
		int block_dim[3],
		const void* ka)
{
	__shared__ int ret;
	BEGIN_SINGLE_THREAD_PART {
		int entry;
		__shared__ cpu_ipc_entry *e;
		GET_QUEUE_SLOT(entry, e);
		int c = strncpy_thread(e->func_name, func_name, GPU_LOCAL_FNAME_MAX_SIZE);
		memcpy_thread(e->grid_dim, grid_dim, sizeof(int)*3);
		memcpy_thread(e->block_dim, block_dim, sizeof(int)*3);
		e->argument = (volatile void*)ka;
		e->req_type = req;

		ret = fire_and_wait(e);

		g_ipc_manager->freeEntry(entry);
		__threadfence();
	} END_SINGLE_THREAD_PART;

	return ret;
}


__device__ int gsynclaunch(const char* func_name,
						   int grid_dim[3],
						   int block_dim[3],
						   const void* ka)
{
	return __glaunch(KERNEL_SYNC_EXECUTE_REQ, func_name, grid_dim, block_dim, ka);
}

__device__ int gasynclaunch(const char* func_name,
							int grid_dim[3],
							int block_dim[3],
							const void* ka)
{
	return __glaunch(KERNEL_ASYNC_INVOKE_REQ, func_name, grid_dim, block_dim, ka);
}

__device__
int gaio_accept_th(cpu_ipc_entry* ipcent, int sock, void* buf, int size)
{
	gpu_socket *serv_soc_e;

	serv_soc_e = &g_soctable->_sockets[sock];

	ipcent->cpu_sock = serv_soc_e->cpu_soc;
	ipcent->req_type = ACCEPT_IPC_REQ;

	fire_async(ipcent);

	return 0;
}

__device__
int gaio_accept_bh(cpu_ipc_entry* e, int ret_val, int sock, void* buf, int size)
{
	__shared__ int newsocket;
	BEGIN_SINGLE_THREAD
	gpu_socket *soc_e;

	newsocket = gsocket(0, 0, 0);
	soc_e = &g_soctable->_sockets[newsocket];

	soc_e->cpu_soc = ret_val;
#ifndef DEFAULT_TO_UNIX_SOCKET
	soc_e->sbuf = (g_ringbuf::ringbuf_t)readNoCache(&e->sbuf_addr);
	soc_e->rbuf = (uint8_t*)readNoCache(&e->rbuf_addr);
	soc_e->rbuf_offset = (uint64_t*)readNoCache(&e->dev_rbuf_offset);
	soc_e->rbuf_bytes_avail = (uint64_t*)readNoCache(&e->dev_rbuf_bytes_avail);
	soc_e->rbuf_size = readNoCache(&e->rbuf_size);
#endif
	END_SINGLE_THREAD
	return newsocket;
}

__device__
int gaio_recv_th(cpu_ipc_entry* ipcent, int sock, void* buf, int size)
{
	int single_recv = min(size, CPU_IO_BUF_SIZE);
	gpu_socket* soc_e = &g_soctable->_sockets[sock];

	ipcent->cpu_sock = soc_e->cpu_soc;
	ipcent->req_type = RECV_IPC_REQ;
	ipcent->data_size = single_recv;
#ifdef DEFAULT_TO_UNIX_SOCKET
	ipcent->req_buffer_offset = soc_e->recv_buffer_offset;
#endif

	fire_async(ipcent);

	return 0;
}

__device__
int gaio_recv_bh(int , int sock, void* buf, int size)
{
#ifdef DEFAULT_TO_UNIX_SOCKET
	gpu_socket *soc_e = &g_soctable->_sockets[sock];
	copy_block((uchar*)buf, (uchar*)g_buffer_space+soc_e->recv_buffer_offset, size);
#endif
	return size;
}

__device__ int gaio(int sock, GAIOP op, void* buf, int size)
{
	GPU_ASSERT((sock >= 0) && (sock < SOC_TABLE_SIZE));
	
	__shared__ int slot;
	BEGIN_SINGLE_THREAD

	cpu_ipc_entry *ent;
	GET_QUEUE_SLOT(slot, ent);
       	assert(ent->status!=CPU_IPC_PENDING);

	if (op == GAIO_ACCEPT)
		gaio_accept_th(ent, sock, buf, size);
	else if (op == GAIO_RECEIVE) 
		gaio_recv_th(ent, sock, buf, size);

	END_SINGLE_THREAD
	return slot;
}

__device__ int gaio_poll(int slot, int sock, GAIOP op, void* buf, int size)
{
	GPU_ASSERT((slot >= 0) && (slot < TASK_ARRAY_SIZE));

	__shared__ int ipc_ret;
	__shared__ cpu_ipc_entry* e;
	int func_ret;
	BEGIN_SINGLE_THREAD
	e = POKE_QUEUE_SLOT(slot);
	if (peek_the_hole(e)) {
		ipc_ret = wait_for_ipc(e);
	} else {
		ipc_ret = -EWOULDBLOCK;
	}
	END_SINGLE_THREAD

	if (ipc_ret < 0)
		return ipc_ret;

	if (op == GAIO_ACCEPT)
		func_ret = gaio_accept_bh(e, ipc_ret, sock, buf, size);
	else
		func_ret = gaio_recv_bh(0, sock, buf, ipc_ret);

	BEGIN_SINGLE_THREAD
	g_ipc_manager->freeEntry(slot);
	END_SINGLE_THREAD
	return func_ret;
}
