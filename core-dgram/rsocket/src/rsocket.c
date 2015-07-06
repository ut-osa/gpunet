
/*
 * Copyright (c) 2008-2013 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#if HAVE_CONFIG_H
#  include <config.h>
#endif /* HAVE_CONFIG_H */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdarg.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <search.h>
#include <execinfo.h>
#include <rdma_dgram/rdma_cma.h>
#include <rdma_dgram/rdma_verbs.h>
#include <rdma_dgram/rsocket.h>

#include <rdma_dgram/gpu.h> // sk: gpu-related
#include <arpa/inet.h>

#include "cma.h"
#include "indexer.h"

#include "bitset.h"   // sk: bitset-related
#include "list.h"     // sk: alloc_list

#define RS_OLAP_START_SIZE 2048
#define RS_MAX_TRANSFER 65536
#define RS_SNDLOWAT UNIT_MSGBUF_SIZE
#define RS_QP_MAX_SIZE 0xFFFE
#define RS_QP_CTRL_SIZE 4
#define RS_CONN_RETRIES 6
#define RS_SGL_SIZE 2
static struct index_map idm;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

#define NR_SBUF (64*2)
#define NR_RBUF (64*2)
#define SBUF_SIZE  (1 << 18)
#define RBUF_SIZE  (1 << 18)

#define TOTAL_SBUF_LEN (NR_SBUF*SBUF_SIZE)
#define TOTAL_RBUF_LEN (NR_RBUF*RBUF_SIZE)

struct rsocket;

enum {
	RS_SVC_NOOP,
	RS_SVC_ADD_DGRAM,
	RS_SVC_REM_DGRAM,
	RS_SVC_ADD_KEEPALIVE,
	RS_SVC_REM_KEEPALIVE,
	RS_SVC_MOD_KEEPALIVE
};

struct rs_svc_msg {
	uint32_t cmd;
	uint32_t status;
	struct rsocket *rs;
};

struct rs_svc {
	pthread_t id;
	int sock[2];
	int cnt;
	int size;
	int context_size;
	void *(*run)(void *svc);
	struct rsocket **rss;
	void *contexts;
};

static struct pollfd *udp_svc_fds;
static void *udp_svc_run(void *arg);
static struct rs_svc udp_svc = {
	.context_size = sizeof(*udp_svc_fds),
	.run = udp_svc_run
};

static uint16_t def_inline = 64;
static uint16_t def_sqsize = 384;
static uint16_t def_rqsize = 384;
static uint32_t def_mem = (1 << 17);
static uint32_t def_wmem = (1 << 17);
static uint32_t polling_time = 10;

/*
 * Immediate data format is determined by the upper bits
 * bit 31: message type, 0 - data, 1 - control
 * bit 30: buffers updated, 0 - target, 1 - direct-receive
 * bit 29: more data, 0 - end of transfer, 1 - more data available
 *
 * for data transfers:
 * bits [28:0]: bytes transferred
 * for control messages:
 * SGL, CTRL
 * bits [28-0]: receive credits granted
 * IOMAP_SGL
 * bits [28-16]: reserved, bits [15-0]: index
 */

enum {
	RS_OP_DATA,
	RS_OP_RSVD_DATA_MORE,
	RS_OP_WRITE, /* opcode is not transmitted over the network */
	RS_OP_RSVD_DRA_MORE,
	RS_OP_SGL,
	RS_OP_RSVD,
	RS_OP_IOMAP_SGL,
	RS_OP_CTRL
};
#define rs_msg_set(op, data)  ((op << 29) | (uint32_t) (data))
#define rs_msg_op(imm_data)   (imm_data >> 29)
#define rs_msg_data(imm_data) (imm_data & 0x1FFFFFFF)
#define RS_MSG_SIZE	      sizeof(uint32_t)

// sk doc:
// bit 63: recv if set
// bit 62: forward from UDP if set
#define RS_WR_ID_FLAG_RECV (((uint64_t) 1) << 63)
#define rs_send_wr_id(data) ((uint64_t) data)
#define rs_recv_wr_id(data) (RS_WR_ID_FLAG_RECV | (uint64_t) data)
#define rs_wr_is_recv(wr_id) (wr_id & RS_WR_ID_FLAG_RECV)
#define rs_wr_data(wr_id) ((uint32_t) wr_id)

#define RS_WR_ID_FLAG_FORWARD ((uint64_t) 1 << 62)
#define rs_forward_wr_id(data) (((uint64_t) data) | RS_WR_ID_FLAG_FORWARD)
#define rs_wr_is_forward(wr_id) (wr_id & RS_WR_ID_FLAG_FORWARD)

enum {
	RS_CTRL_DISCONNECT,
	RS_CTRL_SHUTDOWN
};

struct rs_msg {
	uint32_t op;
	uint32_t data;
};

struct ds_qp;

struct ds_rmsg {
	struct ds_qp	*qp;
	uint32_t	offset;
	uint32_t	length;
};

struct ds_smsg {
	struct ds_smsg	*next;
};

struct rs_sge {
	uint64_t addr;
	uint32_t key;
	uint32_t length;
};

#define RS_MIN_INLINE      (sizeof(struct rs_sge))
#define rs_host_is_net()   (1 == htonl(1))
#define RS_CONN_FLAG_NET   (1 << 0)
#define RS_CONN_FLAG_IOMAP (1 << 1)

struct rs_conn_data {
	uint8_t		  version;
	uint8_t		  flags;
	uint16_t	  credits;
	uint8_t		  reserved[3];
	struct rs_sge	  target_sgl;
	struct rs_sge	  data_buf;
};

struct rs_conn_private_data {
	union {
		struct rs_conn_data		conn_data;
		struct {
			struct ib_connect_hdr	ib_hdr;
			struct rs_conn_data	conn_data;
		} af_ib;
	};
};

// sk: for ipc. should be in sync with the one in net_structures.cu.h
struct ds_smsg_info {
	int msg_index;
	uint16_t length;
	uint16_t addrlen;
	uint8_t  addr[28]; // size of sockaddrin_6
};


// sk: for ipc. should be in sync with the one in net_structures.cu.h
struct ds_rmsg_info {	
	uintptr_t ptr;
	socklen_t addrlen;
	uint16_t length;
	uint8_t  addr[28]; // size of sockaddrin_6
};

/*
 * rsocket states are ordered as passive, connecting, connected, disconnected.
 */
enum rs_state {
	rs_init,
	rs_bound	   =		    0x0001,
	rs_listening	   =		    0x0002,
	rs_opening	   =		    0x0004,
	rs_resolving_addr  = rs_opening |   0x0010,
	rs_resolving_route = rs_opening |   0x0020,
	rs_connecting      = rs_opening |   0x0040,
	rs_accepting       = rs_opening |   0x0080,
	rs_connected	   =		    0x0100,
	rs_writable 	   =		    0x0200,
	rs_readable	   =		    0x0400,
	rs_connect_rdwr    = rs_connected | rs_readable | rs_writable,
	rs_connect_error   =		    0x0800,
	rs_disconnected	   =		    0x1000,
	rs_error	   =		    0x2000,
};

#define RS_OPT_SWAP_SGL   (1 << 0)
/*
 * iWarp does not support RDMA write with immediate data.  For iWarp, we
 * transfer rsocket messages as inline sends.
 */
#define RS_OPT_SVC_ACTIVE (1 << 2)

#define RS_OPT_SENDBUF_GPU (1 << 4)
#define RS_OPT_RECVBUF_GPU (1 << 5)
#define RS_OPT_SENDBUF_BOUNCE (1 << 6)
#define RS_OPT_RECVBUF_BOUNCE (1 << 7)


enum {
	MEMCPY_TYPE_CPU_TO_GPU,
	MEMCPY_TYPE_GPU_TO_CPU
};

struct cu_memcpy_async_req;
typedef int (*cu_memcpy_req_comp)(struct cu_memcpy_async_req* req);

struct cu_memcpy_async_req {
	struct list_head reqs;

	int type;
	
	void *gpu_buf;
	void *cpu_buf;
	
	size_t len;
	cudaStream_t stream;
	cuda_event ev;

	cu_memcpy_req_comp comp_handler;

	struct rsocket *rs;
};

int register_cu_memcpy_req(struct rsocket* rs, int type, void* gpu_buf, void* cpu_buf, size_t len, cu_memcpy_req_comp handler) ;

union socket_addr {
	struct sockaddr		sa;
	struct sockaddr_in	sin;
	struct sockaddr_in6	sin6;
};

struct ds_header {
	uint8_t		  version;
	uint8_t		  length;
	uint16_t	  port;
	union {
		uint32_t  ipv4;
		struct {
			uint32_t flowinfo;
			uint8_t  addr[16];
		} ipv6;
	} addr;
};

#define DS_IPV4_HDR_LEN  8
#define DS_IPV6_HDR_LEN 24

struct ds_dest {
	union socket_addr addr;	/* must be first */
	struct ds_qp	  *qp;
	struct ibv_ah	  *ah;
	uint32_t	   qpn;
};

struct ds_qp {
	dlist_entry	  list;
	struct rsocket	  *rs;
	struct rdma_cm_id *cm_id;
	struct ds_header  *send_hdr;
	struct ds_header  *recv_hdr;
	struct ds_dest	  dest;

	struct ibv_mr	  *smr;
	struct ibv_mr	  *rmr;
	struct ibv_mr	  *rmr_grh; 
	struct ibv_mr	  *send_hdr_mr; 
	struct ibv_mr	  *recv_hdr_mr; 
	uint8_t		  *rbuf;
	struct ibv_grh  *rbuf_grh;  // sk: header for routing. todo: understand this

	int		  cq_armed;

	struct ibv_mr	  *fmr; // sk: for forward buffer (fbuf)
};

LIST_HEAD(alloc_socks);

struct rsocket {
	int		  type;
	int		  index;
	fastlock_t	  slock;
	fastlock_t	  rlock;
	fastlock_t	  cq_lock;
	fastlock_t	  cq_wait_lock;
	fastlock_t	  map_lock; /* acquire slock first if needed */

	union {
		/* datagram */
		struct {
			struct ds_qp	  *qp_list;
			void		  *dest_map;
			struct ds_dest    *conn_dest;
			struct ds_dest    *mcast_dest;

			int		  udp_sock;
			int		  epfd;
			int		  rqe_avail;
			struct ds_smsg	  *smsg_free; // not used with GPU sbuf

			// TODO: CHECK these. 
			// sk: these are for udp forwarding with GPU send. shares sqe_avail
			uint8_t           *fbuf;      // sk: forward buffer (CPU mapped)
			struct ds_smsg	  *fmsg_free; // sk: free list for forward messages with GPU sbuf
		};
	};

	struct list_head alloc_list;
	cudaStream_t g2c_stream;
	int g2c_stream_refcount;

	int		  opts;

    int       gpu_dev_id;
	struct gpunet_ctx *gpu_ctx;

	long		  fd_flags;
	uint64_t	  so_opts;
	uint64_t	  ipv6_opts;
	void		  *optval;
	size_t		  optlen;
	int		  state;
	int		  cq_armed;
	int		  retries;
	int		  err;

	int		  sqe_avail;
	uint32_t	  sbuf_size;
	uint16_t	  sq_size;
	uint16_t	  sq_inline;

#ifdef PROFILE_SEND_COMPLETION
	struct timeval *send_start_tvs;
	double send_interval_sum;
	int send_interval_count;
#endif

	uint32_t	  rbuf_size;
	uint16_t	  rq_size;
	int		  rmsg_head; // sk: these are used only to show the availability of rdata
	int		  rmsg_tail;
	union {
		struct rs_msg	  *rmsg;
		struct ds_rmsg	  *dmsg;
	};

	uint8_t		  *sbuf;
	uint8_t		  *hostptr_host_sbuf_bounce;

	int sbuf_free_queue_back;
	int* devptr_dev_sbuf_free_queue_back;
	int* hostptr_dev_sbuf_free_queue_back;

	int* devptr_dev_sbuf_free_queue;
	int* hostptr_dev_sbuf_free_queue;

	struct ds_smsg_info* devptr_host_sbuf_req_queue;
	struct ds_smsg_info* hostptr_host_sbuf_req_queue;

	// front: the item lastly consumed (initially -1)
    // back : the item to be produced (initially 0)
    // empty : (front + 1 == back mod (sqsize + 1))
    // filled: (front + rq_size == back or front == back)
	int* devptr_dev_sbuf_req_back;
	int* hostptr_dev_sbuf_req_back;
	int* devptr_host_sbuf_req_back;
	int* hostptr_host_sbuf_req_back;
    int sbuf_req_front;

	struct ds_rmsg_info* devptr_dev_rbuf_recved_queue;
	struct ds_rmsg_info* hostptr_dev_rbuf_recved_queue;
	
	int* devptr_dev_rbuf_recved_back;
	int* hostptr_dev_rbuf_recved_back;

	int rbuf_recved_back;
	
	int* devptr_host_rbuf_ack_queue_back;
	int* hostptr_host_rbuf_ack_queue_back;
	int rbuf_ack_queue_front;

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
	double dsend_gpu_total;
	int dsend_gpu_count;
#endif
	struct timeval last_udp_sent;
};

#define DS_UDP_TAG 0x55555555

struct ds_udp_header {
	uint32_t	  tag;
	uint8_t		  version;
	uint8_t		  op;
	uint8_t		  length;
	uint8_t		  reserved;
	uint32_t	  qpn;  /* lower 8-bits reserved */
	union {
		uint32_t ipv4;
		uint8_t  ipv6[16];
	} addr;
};

struct ipc_buf_pool {
	bool on_gpu; // true if located in gpu, false otherwise
	void *dev_ptr;
	void *host_ptr;
	uint32_t len_unit;
	uint32_t unit_count;
};

struct gpunet_ctx {
    struct cuda_dev *dev;
	
	void* dev_sbuf;
	struct ipc_buf_pool host_pool_sbuf_bounce;
	struct ipc_buf_pool dev_pool_sbuf_free_queue_back;
	struct ipc_buf_pool dev_pool_sbuf_free_queue;
	struct ipc_buf_pool host_pool_sbuf_req_queue;
	struct ipc_buf_pool dev_pool_sbuf_req_back;
	struct ipc_buf_pool host_pool_sbuf_req_back;

	void* dev_rbuf;
	struct ipc_buf_pool dev_pool_rbuf_recved_queue;
	struct ipc_buf_pool dev_pool_rbuf_recved_back;
	struct ipc_buf_pool host_pool_rbuf_ack_queue_back;
	
    struct ibv_mr **mr_sbuf;
	struct ibv_mr **mr_rbuf; // multiple mrs because of multiple HCAs
	bitset_t map_sbuf;
	bitset_t map_rbuf;


    struct ibv_pd **pds;
    int count_pds;
};

static struct gpunet_ctx *_g_ctx = NULL;

void alloc_gpu_ctx() {
	int nr_gpu = gpu_device_count();
	assert(_g_ctx == NULL);
	_g_ctx = (struct gpunet_ctx*)calloc(nr_gpu, sizeof(*_g_ctx));
	if (_g_ctx == NULL) {
		fprintf(stderr, "alloc_gpu_ctx allocation error\n");
		exit(1);
	}
}

void free_gpu_ctx() {
	free(_g_ctx);
	_g_ctx = NULL;
}

void preregister_sysbuf(struct gpunet_ctx *ctx);

static void ipc_pool_init(struct ipc_buf_pool *pool, struct cuda_dev *dev, bool on_gpu, const uint32_t len, const uint32_t count) {
	pool->on_gpu = on_gpu;
	pool->len_unit = len;
	pool->unit_count = count;
	
	int total_size = len * count;
	if (on_gpu) {
		pool->dev_ptr = gpu_malloc(dev, total_size);
		pool->host_ptr = gpu_mmap(pool->dev_ptr, total_size);
	} else {
		gpu_shmem_alloc(dev, total_size, &pool->host_ptr, &pool->dev_ptr);
	}
}

static void* ipc_pool_host_ptr(struct ipc_buf_pool *pool, int index) {
	return ((uint8_t*)pool->host_ptr) + index * pool->len_unit;
}

static void* ipc_pool_dev_ptr(struct ipc_buf_pool *pool, int index) {
	return ((uint8_t*)pool->dev_ptr) + index * pool->len_unit;	
}

void __init_gpunet_ctx(int gpu_id, bool no_dev_change) {
	int orig_dev_id;
	struct gpunet_ctx *ctx = NULL;
    struct cuda_dev *dev;
#ifdef GPUDIRECT_SBUF
	size_t sbuf_pool_size;
#endif
	if (!_g_ctx)
		alloc_gpu_ctx();

	if (no_dev_change)
		CudaSafeCall(cudaGetDevice(&orig_dev_id));

	CudaSafeCall(cudaSetDevice(gpu_id));
	ctx = &_g_ctx[gpu_id];

    bitset_create(&(ctx->map_sbuf), NR_SBUF);
    bitset_create(&(ctx->map_rbuf), NR_RBUF);

	assert(ctx->map_sbuf != NULL);
	assert(ctx->map_rbuf != NULL);
    
    dev = gpu_init(gpu_id);

	ctx->dev = dev;
	ctx->dev_sbuf = gpu_malloc(dev, TOTAL_SBUF_LEN);
	
	ipc_pool_init(&ctx->host_pool_sbuf_bounce, dev, false, SBUF_SIZE, NR_SBUF);
	
	assert(ctx->dev_sbuf != NULL);

	const int sq_size = SBUF_SIZE / RS_SNDLOWAT;
	ipc_pool_init(&ctx->dev_pool_sbuf_free_queue_back, dev, true, sizeof(int), NR_SBUF);
	ipc_pool_init(&ctx->dev_pool_sbuf_free_queue, dev, true, sizeof(int) * (sq_size + 1), NR_SBUF); // sbuf_free_queue should contain sq_size items
	ipc_pool_init(&ctx->host_pool_sbuf_req_queue, dev, false, sizeof(struct ds_smsg_info) * (sq_size + 1), NR_SBUF);
	ipc_pool_init(&ctx->dev_pool_sbuf_req_back, dev, true, sizeof(int), NR_SBUF);
	ipc_pool_init(&ctx->host_pool_sbuf_req_back, dev, false, sizeof(int), NR_SBUF);

	const int rq_size = RBUF_SIZE / RS_SNDLOWAT;
	ctx->dev_rbuf = gpu_malloc(dev, TOTAL_RBUF_LEN);
	assert(ctx->dev_rbuf != NULL);
	ipc_pool_init(&ctx->dev_pool_rbuf_recved_queue, dev, true, sizeof(struct ds_rmsg_info) * (rq_size + 1), NR_RBUF);
	ipc_pool_init(&ctx->dev_pool_rbuf_recved_back, dev, true, sizeof(int), NR_RBUF);
	ipc_pool_init(&ctx->host_pool_rbuf_ack_queue_back, dev, false, sizeof(int), NR_RBUF);
	
	if (no_dev_change)
		CudaSafeCall(cudaSetDevice(orig_dev_id));

    preregister_sysbuf(ctx);
}

void init_gpunet_ctx(int gpu_id) {
	__init_gpunet_ctx(gpu_id, true);
}

void register_sysbuf(struct gpunet_ctx* ctx, struct ibv_pd* pd, int dev_index) {
	assert(ctx->mr_sbuf);
	assert(ctx->mr_rbuf);
	assert(pd);


    ctx->mr_sbuf[dev_index] = ibv_reg_mr(pd, ctx->dev_sbuf, TOTAL_SBUF_LEN,
                                         IBV_ACCESS_LOCAL_WRITE);
	assert(ctx->mr_sbuf[dev_index] != NULL);
#ifdef VERBOSE
	fprintf(stderr, "register_sysbuf() register dev %d sbuf %p, lkey: %d\n", dev_index, ctx->dev_sbuf, ctx->mr_sbuf[dev_index]->lkey);
#endif
    
    ctx->mr_rbuf[dev_index] = ibv_reg_mr(pd, ctx->dev_rbuf, TOTAL_RBUF_LEN,
                                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	assert(ctx->mr_rbuf[dev_index] != NULL);
}

void preregister_sysbuf(struct gpunet_ctx *ctx) {
  struct ibv_pd **pds;
  int i = 0, count_dev = 0;

  assert(ctx->dev_sbuf);
  assert(ctx->dev_rbuf);

  pds = rdma_get_pds();
  assert(pds);

  while(pds[i++]) count_dev++;

  ctx->mr_sbuf = (struct ibv_mr**)calloc(1, sizeof(*ctx->mr_sbuf) * count_dev);
  ctx->mr_rbuf = (struct ibv_mr**)calloc(1, sizeof(*ctx->mr_rbuf) * count_dev);
  
  ctx->pds = pds;
  ctx->count_pds = count_dev;
  
  for (i = 0; i < count_dev; i++) {
    register_sysbuf(ctx, pds[i], i);
  }
}

int index_for_pd(struct gpunet_ctx *ctx, struct ibv_pd* pd) {
  int i;
  for (i = 0; i < ctx->count_pds; i++) {
    if (pd == ctx->pds[i]) return i;
  }
  return -1;
}

void init_gpunet_ctx_all(int nr_gpu) {
	int i = 0;
	int orig_dev_id;
	
	if (!_g_ctx)
		alloc_gpu_ctx();

	CudaSafeCall(cudaGetDevice(&orig_dev_id));
	for (; i < nr_gpu; i++) {
		__init_gpunet_ctx(i, false);
	}
	CudaSafeCall(cudaSetDevice(orig_dev_id));
}

int pick_unset_bit(bitset_t bs, int cardinality) {
    int ret = -1;
    int i;
    
    for(i = 0; i < cardinality; i++ ) {
        if (!bitset_contains(bs, i)) {
            bitset_add(bs, i);
            ret = i;
            break;
        }
    }
    return ret;
}

// returns negative with error, non-negative with 
int alloc_sbuf(struct gpunet_ctx* ctx, uint8_t** dev_sbuf) {
    int ret = -1;

	assert(ctx->map_sbuf != NULL);
    ret = pick_unset_bit(ctx->map_sbuf, NR_SBUF);
    if (ret >= 0) {
        *dev_sbuf = ctx->dev_sbuf + ret * SBUF_SIZE;
    }
    return ret;
}

void dealloc_sbuf(struct gpunet_ctx* ctx, uint8_t *dev_sbuf) {
    int offset = (int)(((unsigned char*)dev_sbuf) - (unsigned char*)ctx->dev_sbuf);
    int idx = offset / SBUF_SIZE;

    bitset_remove(ctx->map_sbuf, idx);
}

int alloc_rbuf(struct gpunet_ctx* ctx,
			   uint8_t** pbuf) {
    int ret = -1;

    ret = pick_unset_bit(ctx->map_rbuf, NR_RBUF);
    if (ret >= 0) {
        *pbuf = (ctx->dev_rbuf + ret * RBUF_SIZE);
    }
    return ret;
}

// only by giving rbuf, this also deallocates rbuf_bytes_avail, rbuf_offset
void dealloc_rbuf(struct gpunet_ctx* ctx, uint8_t *rbuf) {
    int offset = (int)(((unsigned char*)rbuf) - (unsigned char*)ctx->dev_rbuf);
    int idx = offset / RBUF_SIZE;

    bitset_remove(ctx->map_rbuf, idx);
}


#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
int send_count = 0;
int recv_count = 0;
#endif


#define DS_UDP_IPV4_HDR_LEN 16
#define DS_UDP_IPV6_HDR_LEN 28

#define ds_next_qp(qp) container_of((qp)->list.next, struct ds_qp, list)

static void ds_insert_qp(struct rsocket *rs, struct ds_qp *qp)
{
	if (!rs->qp_list)
		dlist_init(&qp->list);
	else
		dlist_insert_head(&qp->list, &rs->qp_list->list);
	rs->qp_list = qp;
}

static void ds_remove_qp(struct rsocket *rs, struct ds_qp *qp)
{
	if (qp->list.next != &qp->list) {
		rs->qp_list = ds_next_qp(qp);
		dlist_remove(&qp->list);
	} else {
		rs->qp_list = NULL;
	}
}

static int rs_notify_svc(struct rs_svc *svc, struct rsocket *rs, int cmd)
{
	struct rs_svc_msg msg;
	int ret;

	pthread_mutex_lock(&mut);
	if (!svc->cnt) {
		ret = socketpair(AF_UNIX, SOCK_STREAM, 0, svc->sock);
		if (ret)
			goto unlock;

		ret = pthread_create(&svc->id, NULL, svc->run, svc);
		if (ret) {
			ret = ERR(ret);
			goto closepair;
		}
	}

	msg.cmd = cmd;
	msg.status = EINVAL;
	msg.rs = rs;
	write(svc->sock[0], &msg, sizeof msg);
	read(svc->sock[0], &msg, sizeof msg);
	ret = rdma_seterrno(msg.status);
	if (svc->cnt)
		goto unlock;

	pthread_join(svc->id, NULL);
closepair:
	close(svc->sock[0]);
	close(svc->sock[1]);
unlock:
	pthread_mutex_unlock(&mut);
	return ret;
}

static int ds_compare_addr(const void *dst1, const void *dst2)
{
	const struct sockaddr *sa1, *sa2;
	size_t len;

	sa1 = (const struct sockaddr *) dst1;
	sa2 = (const struct sockaddr *) dst2;

	len = (sa1->sa_family == AF_INET6 && sa2->sa_family == AF_INET6) ?
	      sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);
	return memcmp(dst1, dst2, len);
}

void rs_configure(void)
{
	FILE *f;
	static int init;

	if (init)
		return;

	pthread_mutex_lock(&mut);
	if (init)
		goto out;

	if (ucma_init())
		goto out;
	ucma_ib_init();

	if ((f = fopen(RS_CONF_DIR "/polling_time", "r"))) {
		(void) fscanf(f, "%u", &polling_time);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/inline_default", "r"))) {
		(void) fscanf(f, "%hu", &def_inline);
		fclose(f);

		if (def_inline < RS_MIN_INLINE)
			def_inline = RS_MIN_INLINE;
	}

	if ((f = fopen(RS_CONF_DIR "/sqsize_default", "r"))) {
		(void) fscanf(f, "%hu", &def_sqsize);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/rqsize_default", "r"))) {
		(void) fscanf(f, "%hu", &def_rqsize);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/mem_default", "r"))) {
		(void) fscanf(f, "%u", &def_mem);
		fclose(f);

		if (def_mem < 1)
			def_mem = 1;
	}

	if ((f = fopen(RS_CONF_DIR "/wmem_default", "r"))) {
		(void) fscanf(f, "%u", &def_wmem);
		fclose(f);
		if (def_wmem < RS_SNDLOWAT)
			def_wmem = RS_SNDLOWAT << 1;
	}

	init = 1;
out:
	pthread_mutex_unlock(&mut);
}

static int rs_insert(struct rsocket *rs, int index)
{
	pthread_mutex_lock(&mut);
	rs->index = idm_set(&idm, index, rs);
	pthread_mutex_unlock(&mut);
	return rs->index;
}

static void rs_remove(struct rsocket *rs)
{
	pthread_mutex_lock(&mut);
	idm_clear(&idm, rs->index);
	pthread_mutex_unlock(&mut);
}

static struct rsocket *rs_alloc(struct rsocket *inherited_rs, int type)
{
	struct rsocket *rs;

	rs = calloc(1, sizeof *rs);
	if (!rs)
		return NULL;

	rs->type = type;
	rs->index = -1;
	rs->udp_sock = -1;
	rs->epfd = -1;

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
	rs->dsend_gpu_count = 0;
	rs->dsend_gpu_total = 0.0;
#endif

	if (inherited_rs) {
		rs->sbuf_size = inherited_rs->sbuf_size;
		rs->rbuf_size = inherited_rs->rbuf_size;
		rs->sq_inline = inherited_rs->sq_inline;
		rs->sq_size = inherited_rs->sq_size;
		rs->rq_size = inherited_rs->rq_size;
	} else {
		rs->sbuf_size = SBUF_SIZE; // def_wmem;
		rs->rbuf_size = RBUF_SIZE; // def_mem;
		rs->sq_inline = def_inline;
		rs->sq_size = def_sqsize;
		rs->rq_size = def_rqsize;
	}

#ifdef PROFILE_SEND_COMPLETION
	rs->send_start_tvs = (struct timeval*)calloc(rs->sq_size,
												 sizeof(*rs->send_start_tvs));
	rs->send_interval_count = 0;
	rs->send_interval_sum = 0.0;
#endif

	INIT_LIST_HEAD(&rs->alloc_list);
	rs->last_udp_sent.tv_sec = 0;
	
	fastlock_init(&rs->slock);
	fastlock_init(&rs->rlock);
	fastlock_init(&rs->cq_lock);
	fastlock_init(&rs->cq_wait_lock);
	fastlock_init(&rs->map_lock);
	return rs;
}

static int rs_set_nonblocking(struct rsocket *rs, long arg)
{
	struct ds_qp *qp;
	int ret = 0;

	ret = fcntl(rs->epfd, F_SETFL, arg);
	if (!ret && rs->qp_list) {
		qp = rs->qp_list;
		do {
			ret = fcntl(qp->cm_id->recv_cq_channel->fd,
					    F_SETFL, arg);
			qp = ds_next_qp(qp);
		} while (qp != rs->qp_list && !ret);
	}

	return ret;
}

static void ds_set_qp_size(struct rsocket *rs)
{
	uint16_t max_size;

	max_size = min(ucma_max_qpsize(NULL), RS_QP_MAX_SIZE);

	if (rs->sq_size > max_size)
		rs->sq_size = max_size;
	if (rs->rq_size > max_size)
		rs->rq_size = max_size;

	if (rs->rq_size > (rs->rbuf_size / RS_SNDLOWAT))
		rs->rq_size = rs->rbuf_size / RS_SNDLOWAT;
	else {
		assert(false);
		rs->rbuf_size = rs->rq_size * RS_SNDLOWAT;
	}

	if (rs->sq_size > (rs->sbuf_size / RS_SNDLOWAT))
		rs->sq_size = rs->sbuf_size / RS_SNDLOWAT;
	else {
		assert(false);
		rs->sbuf_size = rs->sq_size * RS_SNDLOWAT;
	}
}

static int ds_init_bufs(struct ds_qp *qp)
{
	if (qp->rs->opts & RS_OPT_SENDBUF_GPU) {
		// TODO: this should be cached.
		qp->fmr = rdma_reg_msgs(qp->cm_id, qp->rs->fbuf, qp->rs->sbuf_size);
		if (!qp->fmr)
			return -1;
	}
	
	if (qp->rs->opts & RS_OPT_RECVBUF_GPU) {
		if (alloc_rbuf(qp->rs->gpu_ctx, &qp->rbuf) < 0) {
			fprintf(stderr, "ds_init_bufs alloc_rbuf error\n");
            exit(1);
		}
		
		qp->rbuf_grh = calloc(1, sizeof(struct ibv_grh));
		if (!qp->rbuf_grh)
			return ERR(ENOMEM);

		int pd_index = index_for_pd(qp->rs->gpu_ctx, qp->cm_id->pd);
		qp->smr = qp->rs->gpu_ctx->mr_sbuf[pd_index];
		if (!qp->smr)
			return -1;

		qp->rmr = qp->rs->gpu_ctx->mr_rbuf[pd_index];
		if (!qp->rmr)
			return -1;

		// TODO: this should be cached
		qp->rmr_grh = rdma_reg_write(qp->cm_id, qp->rbuf, sizeof(struct ibv_grh));
		if (!qp->rmr_grh)
			return -1;

		// TODO: registration cache
		qp->recv_hdr_mr = rdma_reg_write(qp->cm_id, qp->recv_hdr, (RBUF_SIZE / RS_SNDLOWAT) * sizeof(*qp->recv_hdr));
		if (!qp->recv_hdr_mr)
			return -1;

		// TODO: registration cache
		qp->send_hdr_mr = rdma_reg_msgs(qp->cm_id, qp->send_hdr, sizeof(*qp->send_hdr));
		if (!qp->send_hdr_mr)
			return -1;
										
	} else {
		qp->rbuf = calloc(qp->rs->rbuf_size + sizeof(struct ibv_grh), 1);
		if (!qp->rbuf)
			return ERR(ENOMEM);

		qp->smr = rdma_reg_msgs(qp->cm_id, qp->rs->sbuf, qp->rs->sbuf_size);
		if (!qp->smr)
			return -1;
		
		qp->rmr = rdma_reg_msgs(qp->cm_id, qp->rbuf, qp->rs->rbuf_size +
								sizeof(struct ibv_grh));
		if (!qp->rmr)
			return -1;
	}

	return 0;
}

/*
 * If a user is waiting on a datagram rsocket through poll or select, then
 * we need the first completion to generate an event on the related epoll fd
 * in order to signal the user.  We arm the CQ on creation for this purpose
 */
static int rs_create_cq(struct rsocket *rs, struct rdma_cm_id *cm_id)
{
	cm_id->recv_cq_channel = ibv_create_comp_channel(cm_id->verbs);
	if (!cm_id->recv_cq_channel)
		return -1;

	cm_id->recv_cq = ibv_create_cq(cm_id->verbs, rs->sq_size + rs->rq_size,
				       cm_id, cm_id->recv_cq_channel, 0);
	if (!cm_id->recv_cq)
		goto err1;

	if (rs->fd_flags & O_NONBLOCK) {
		if (fcntl(cm_id->recv_cq_channel->fd, F_SETFL, O_NONBLOCK))
			goto err2;
	}

	ibv_req_notify_cq(cm_id->recv_cq, 0);
	cm_id->send_cq_channel = cm_id->recv_cq_channel;
	cm_id->send_cq = cm_id->recv_cq;
	return 0;

err2:
	ibv_destroy_cq(cm_id->recv_cq);
	cm_id->recv_cq = NULL;
err1:
	ibv_destroy_comp_channel(cm_id->recv_cq_channel);
	cm_id->recv_cq_channel = NULL;
	return -1;
}

static inline int ds_post_recv(struct rsocket *rs, struct ds_qp *qp, uint32_t offset)
{
	struct ibv_recv_wr wr, *bad;
	struct ibv_sge sge[3];
	int nr_sge;

	if (rs->opts & RS_OPT_RECVBUF_GPU) {
		sge[0].addr = (uintptr_t) qp->rbuf_grh;
		sge[0].length = sizeof(struct ibv_grh);
		sge[0].lkey = qp->rmr_grh->lkey;
		sge[1].addr = (uintptr_t)&(qp->recv_hdr[offset / RS_SNDLOWAT]);
		sge[1].length = DS_IPV4_HDR_LEN;
		sge[1].lkey = qp->recv_hdr_mr->lkey;
		nr_sge = 2;
	} else {
		sge[0].addr = (uintptr_t) qp->rbuf + rs->rbuf_size;
		sge[0].length = sizeof(struct ibv_grh);
		sge[0].lkey = qp->rmr->lkey;
		nr_sge = 1;
	}
	sge[nr_sge].addr = (uintptr_t) qp->rbuf + offset;
	sge[nr_sge].length = RS_SNDLOWAT;
	sge[nr_sge].lkey = qp->rmr->lkey;
	
	wr.wr_id = rs_recv_wr_id(offset);
	wr.next = NULL;
	wr.sg_list = sge;
	wr.num_sge = nr_sge + 1;

	return rdma_seterrno(ibv_post_recv(qp->cm_id->qp, &wr, &bad));
}

static void ds_free_qp(struct ds_qp *qp)
{
	if (qp->smr) {
		if (!(qp->rs->opts & RS_OPT_SENDBUF_GPU))
			rdma_dereg_mr(qp->smr);
	}

	if (qp->rbuf && !(qp->rs->opts & RS_OPT_RECVBUF_GPU)) {
		if (qp->rmr) {
			rdma_dereg_mr(qp->rmr);
		}
		free(qp->rbuf);
	}

	if (qp->cm_id) {
		if (qp->cm_id->qp) {
			tdelete(&qp->dest.addr, &qp->rs->dest_map, ds_compare_addr);
			epoll_ctl(qp->rs->epfd, EPOLL_CTL_DEL,
				  qp->cm_id->recv_cq_channel->fd, NULL);
			rdma_destroy_qp(qp->cm_id);
		}
		rdma_destroy_id(qp->cm_id);
	}

	free(qp);
}

static void ds_free(struct rsocket *rs)
{
	struct ds_qp *qp;

	if (rs->udp_sock >= 0)
		close(rs->udp_sock);

	if (rs->index >= 0)
		rs_remove(rs);

	if (rs->dmsg)
		free(rs->dmsg);

	while ((qp = rs->qp_list)) {
		ds_remove_qp(rs, qp);
		ds_free_qp(qp);
	}

	if (rs->epfd >= 0)
		close(rs->epfd);

	if (rs->sbuf)
		free(rs->sbuf);

	tdestroy(rs->dest_map, free);
	fastlock_destroy(&rs->map_lock);
	fastlock_destroy(&rs->cq_wait_lock);
	fastlock_destroy(&rs->cq_lock);
	fastlock_destroy(&rs->rlock);
	fastlock_destroy(&rs->slock);
	free(rs);
}

static void rs_free(struct rsocket *rs)
{
	if (!list_empty(&rs->alloc_list)) {
		list_del(&rs->alloc_list);
	}

	ds_free(rs);
	return;
}

static int ds_init(struct rsocket *rs, int domain)
{
	rs->udp_sock = socket(domain, SOCK_DGRAM, 0);
	if (rs->udp_sock < 0)
		return rs->udp_sock;

	rs->epfd = epoll_create(2);
	if (rs->epfd < 0)
		return rs->epfd;

	return 0;
}

static int ds_init_ep(struct rsocket *rs)
{
	struct ds_smsg *msg;
	int i, ret;

	ds_set_qp_size(rs);
	int sbuf_index = -1;

	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		sbuf_index = alloc_sbuf(rs->gpu_ctx, &rs->sbuf);
		if (sbuf_index < 0) {
            fprintf(stderr, "rs_init_bufs alloc_sbuf error\n");
            exit(1);
        }

		rs->fbuf = calloc(rs->sq_size, RS_SNDLOWAT);
		if (!rs->fbuf) {
			return ERR(ENOMEM);
		}

#define get_devptr_from_pool(prefix, name, index) rs-> devptr_ ## prefix ## _ ## name = ipc_pool_ ## prefix ## _ptr(&rs->gpu_ctx-> prefix ## _pool_ ## name, index) ;

#define get_devptr_from_pool_type(prefix, name, index, type) rs-> devptr_## prefix ## _ ## name = (type)ipc_pool_dev_ptr(&rs->gpu_ctx-> prefix ## _pool_ ## name, index) ;

#define get_hostptr_from_pool_type(prefix, name, index, type) rs-> hostptr_ ## prefix ## _ ## name = (type)ipc_pool_host_ptr(&rs->gpu_ctx-> prefix ## _pool_ ## name, index)

#define get_ptrs_from_pool_type(prefix, name, index, type)		\
		get_devptr_from_pool_type(prefix, name, index, type);	\
		get_hostptr_from_pool_type(prefix, name, index, type);
		
		get_hostptr_from_pool_type(host, sbuf_bounce, sbuf_index, uint8_t*);

		// initially free_queue is filled
		get_ptrs_from_pool_type(dev, sbuf_free_queue, sbuf_index, int*);
		for (i = 0; i < rs->sq_size; i++) rs->hostptr_dev_sbuf_free_queue[i] = i;

		get_ptrs_from_pool_type(dev, sbuf_free_queue_back, sbuf_index, int*);
		*rs->hostptr_dev_sbuf_free_queue_back = rs->sq_size; // filled (sbuf_free_queue has size of sq_size + 1)
		
		get_ptrs_from_pool_type(host, sbuf_req_queue, sbuf_index, struct ds_smsg_info*);
		get_ptrs_from_pool_type(dev, sbuf_req_back, sbuf_index, int*);
		get_ptrs_from_pool_type(host, sbuf_req_back, sbuf_index, int*);

		*rs->hostptr_dev_sbuf_req_back = 0;
		*rs->hostptr_host_sbuf_req_back = 0;

		rs->sbuf_free_queue_back = rs->sq_size;
		rs->sbuf_req_front = -1;
    } else {
		rs->sbuf = calloc(rs->sq_size, RS_SNDLOWAT);
		if (!rs->sbuf)
			return ERR(ENOMEM);
	}

	rs->dmsg = calloc(rs->rq_size + 1, sizeof(*rs->dmsg));
	if (!rs->dmsg)
		return ERR(ENOMEM);

	rs->sqe_avail = rs->sq_size;
	rs->rqe_avail = rs->rq_size;

	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		rs->fmsg_free = (struct ds_smsg *)rs->fbuf;
		msg = rs->fmsg_free;
		for (i = 0; i < rs->sq_size - 1; i++) {
			msg->next = (void *) msg + RS_SNDLOWAT;
			msg = msg->next;
		}
		msg->next = NULL;
	} else {
		rs->smsg_free = (struct ds_smsg *) rs->sbuf;
		msg = rs->smsg_free;
		for (i = 0; i < rs->sq_size - 1; i++) {
			msg->next = (void *) msg + RS_SNDLOWAT;
			msg = msg->next;
		}
		msg->next = NULL;
	}

	ret = rs_notify_svc(&udp_svc, rs, RS_SVC_ADD_DGRAM);
	if (ret)
		return ret;

	if (rs->opts & RS_OPT_RECVBUF_GPU) {
		// uses sbuf_index because these data strctures are for sockets.
		// rbuf is a qp resource whereas sbuf is a socket resource.
		assert(sbuf_index != -1);
		get_ptrs_from_pool_type(dev, rbuf_recved_queue, sbuf_index, struct ds_rmsg_info*);
		get_ptrs_from_pool_type(dev, rbuf_recved_back, sbuf_index, int*);
		get_ptrs_from_pool_type(host, rbuf_ack_queue_back, sbuf_index, int*);

		(*rs->hostptr_dev_rbuf_recved_back) = 0;
		rs->rbuf_recved_back = 0;
		rs->rbuf_ack_queue_front = -1;
	}

	list_add(&rs->alloc_list, &alloc_socks);

	rs->state = rs_readable | rs_writable;

	return 0;
}

int __rsocket(int domain, int type, int protocol, bool gpu)
{
	struct rsocket *rs;
	int index, ret;

	if ((domain != AF_INET && domain != AF_INET6 && domain != AF_IB) ||
	    (type != SOCK_DGRAM) ||
	    (type == SOCK_DGRAM && protocol && protocol != IPPROTO_UDP))
		return ERR(ENOTSUP);

	rs_configure();
	rs = rs_alloc(NULL, type);
	if (!rs)
		return ERR(ENOMEM);

	ret = ds_init(rs, domain);
	if (ret)
		goto err;
	
	index = rs->udp_sock;

	if (gpu) {
		rs->gpu_dev_id = gpu_get_device_id();
		if (_g_ctx == NULL || !_g_ctx[rs->gpu_dev_id].dev_sbuf) {
			init_gpunet_ctx(rs->gpu_dev_id);
		}
		rs->gpu_ctx = &_g_ctx[rs->gpu_dev_id];
	}

	ret = rs_insert(rs, index);
	if (ret < 0)
		goto err;

	return rs->index;

err:
	rs_free(rs);
	return ret;
}

int rsocket(int domain, int type, int protocol) {
	return __rsocket(domain, type, protocol, false);
}

int rsocket_gpu(int domain, int type, int protocol) {
	return __rsocket(domain, type, protocol, true);
}

int rbind(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;
#ifdef VERBOSE
	struct sockaddr_in *addr_in = (struct sockaddr_in*)addr;
	printf("rbind %d %d.%d.%d.%d:%d\n",
		   socket,
		   (int)(addr_in->sin_addr.s_addr&0xFF),
		   (int)((addr_in->sin_addr.s_addr&0xFF00)>>8),
		   (int)((addr_in->sin_addr.s_addr&0xFF0000)>>16),
		   (int)((addr_in->sin_addr.s_addr&0xFF000000)>>24),
		   (int)(ntohs(addr_in->sin_port)));
#endif

	rs = idm_at(&idm, socket);
	if (rs->state == rs_init) {
		ret = ds_init_ep(rs);
		if (ret)
			return ret;
	}
	ret = bind(rs->udp_sock, addr, addrlen);
	return ret;
}

static int rs_any_addr(const union socket_addr *addr)
{
	if (addr->sa.sa_family == AF_INET) {
		return (addr->sin.sin_addr.s_addr == INADDR_ANY ||
			addr->sin.sin_addr.s_addr == INADDR_LOOPBACK);
	} else {
		return (!memcmp(&addr->sin6.sin6_addr, &in6addr_any, 16) ||
			!memcmp(&addr->sin6.sin6_addr, &in6addr_loopback, 16));
	}
}

static int ds_get_src_addr(struct rsocket *rs,
			   const struct sockaddr *dest_addr, socklen_t dest_len,
			   union socket_addr *src_addr, socklen_t *src_len)
{
	int sock, ret;
	uint16_t port;

	*src_len = sizeof *src_addr;
	ret = getsockname(rs->udp_sock, &src_addr->sa, src_len);
	if (ret || !rs_any_addr(src_addr)) {
		return ret;
	}

	port = src_addr->sin.sin_port;
	sock = socket(dest_addr->sa_family, SOCK_DGRAM, 0);
	if (sock < 0) {
		fprintf(stderr, "socket sa_family: 0x%x\n", dest_addr->sa_family);
		perror("socket\n");
		return sock;
	}

	ret = connect(sock, dest_addr, dest_len);
	if (ret) {
		fprintf(stderr, "connect error\n");
		goto out;
	}

	*src_len = sizeof *src_addr;
	ret = getsockname(sock, &src_addr->sa, src_len);
	src_addr->sin.sin_port = port;
out:
	close(sock);
	return ret;
}

static void ds_format_hdr(struct ds_header *hdr, union socket_addr *addr)
{
	if (addr->sa.sa_family == AF_INET) {
		hdr->version = 4;
		hdr->length = DS_IPV4_HDR_LEN;
		hdr->port = addr->sin.sin_port;
		hdr->addr.ipv4 = addr->sin.sin_addr.s_addr;
	} else {
		hdr->version = 6;
		hdr->length = DS_IPV6_HDR_LEN;
		hdr->port = addr->sin6.sin6_port;
		hdr->addr.ipv6.flowinfo= addr->sin6.sin6_flowinfo;
		memcpy(&hdr->addr.ipv6.addr, &addr->sin6.sin6_addr, 16);
	}
}

static int ds_add_qp_dest(struct ds_qp *qp, union socket_addr *addr,
			  socklen_t addrlen)
{
	struct ibv_port_attr port_attr;
	struct ibv_ah_attr attr;
	int ret;

	memcpy(&qp->dest.addr, addr, addrlen);
	qp->dest.qp = qp;
	qp->dest.qpn = qp->cm_id->qp->qp_num;

	ret = ibv_query_port(qp->cm_id->verbs, qp->cm_id->port_num, &port_attr);
	if (ret)
		return ret;

	memset(&attr, 0, sizeof attr);
	attr.dlid = port_attr.lid;
	attr.port_num = qp->cm_id->port_num;
	qp->dest.ah = ibv_create_ah(qp->cm_id->pd, &attr);
	if (!qp->dest.ah)
		return ERR(ENOMEM);

	tsearch(&qp->dest.addr, &qp->rs->dest_map, ds_compare_addr);
	return 0;
}

static int ds_create_qp(struct rsocket *rs, union socket_addr *src_addr,
			socklen_t addrlen, struct ds_qp **new_qp)
{
	struct ds_qp *qp;
	struct ibv_qp_init_attr qp_attr;
	struct epoll_event event;
	int i, ret;

	qp = calloc(1, sizeof(*qp));
	if (!qp)
		return ERR(ENOMEM);

	qp->rs = rs;
	ret = rdma_create_id(NULL, &qp->cm_id, qp, RDMA_PS_UDP);
	if (ret)
		goto err;

	// for reception
	qp->recv_hdr = calloc(RBUF_SIZE / RS_SNDLOWAT, sizeof(*qp->recv_hdr));
	if (!qp->recv_hdr)
		goto err;

	qp->send_hdr = calloc(1, sizeof(*qp->send_hdr));
	if (!qp->send_hdr)
		goto err;

	ds_format_hdr(qp->send_hdr, src_addr);
	ret = rdma_bind_addr(qp->cm_id, &src_addr->sa);
	if (ret) {
		perror("rdma_bind_addr");
		goto err;
	}

	ret = ds_init_bufs(qp);
	if (ret) {
		perror("ds_init_bufs");
		goto err;
	}

	ret = rs_create_cq(rs, qp->cm_id);
	if (ret)
		goto err;

	memset(&qp_attr, 0, sizeof qp_attr);
	qp_attr.qp_context = qp;
	qp_attr.send_cq = qp->cm_id->send_cq;
	qp_attr.recv_cq = qp->cm_id->recv_cq;
	qp_attr.qp_type = IBV_QPT_UD;
	qp_attr.sq_sig_all = 1;
	qp_attr.cap.max_send_wr = rs->sq_size;
	qp_attr.cap.max_recv_wr = rs->rq_size;
	qp_attr.cap.max_send_sge = 2;
	qp_attr.cap.max_recv_sge = 3;
	qp_attr.cap.max_inline_data = rs->sq_inline;
	ret = rdma_create_qp(qp->cm_id, NULL, &qp_attr);
	if (ret)
		goto err;

	ret = ds_add_qp_dest(qp, src_addr, addrlen);
	if (ret)
		goto err;

	event.events = EPOLLIN;
	event.data.ptr = qp;
	ret = epoll_ctl(rs->epfd,  EPOLL_CTL_ADD,
			qp->cm_id->recv_cq_channel->fd, &event);
	if (ret)
		goto err;

	for (i = 0; i < rs->rq_size; i++) {
		ret = ds_post_recv(rs, qp, i * RS_SNDLOWAT);
		if (ret)
			goto err;
	}

	ds_insert_qp(rs, qp);
	*new_qp = qp;
	return 0;
err:
	ds_free_qp(qp);
	return ret;
}

static int ds_get_qp(struct rsocket *rs, union socket_addr *src_addr,
		     socklen_t addrlen, struct ds_qp **qp)
{
	if (rs->qp_list) {
		*qp = rs->qp_list;
		do {
			if (!ds_compare_addr(rdma_get_local_addr((*qp)->cm_id),
					     src_addr))
				return 0;

			*qp = ds_next_qp(*qp);
		} while (*qp != rs->qp_list);
	}

	return ds_create_qp(rs, src_addr, addrlen, qp);
}

static int ds_get_dest(struct rsocket *rs, const struct sockaddr *addr,
					   socklen_t addrlen, struct ds_dest **dest)
{
	union socket_addr src_addr;
	socklen_t src_len;
	struct ds_qp *qp = NULL;
	struct ds_dest **tdest, *new_dest;
	int ret = 0;

	fastlock_acquire(&rs->map_lock);

	// do we have this destination address in the dest_map?
	// if so, we found one!
	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (tdest)
		goto found;

	// ok, we don't have this destination address in our dest_map.
	// fine. what's the IP address of the interface we will use to communicate with the destination? (src_addr/src_len)
	ret = ds_get_src_addr(rs, addr, addrlen, &src_addr, &src_len);
	if (ret) {
		fprintf(stderr, "ds_get_dest ds_get_src_addr error ret: %d\n", ret);
		goto out;
	}

	// now that we know which interface to use, let's make a qp
	// but, we just want one qp per interface, so reuse one if already exists
	ret = ds_get_qp(rs, &src_addr, src_len, &qp);
	if (ret) {
		fprintf(stderr, "ds_get_dest ds_get_qp error ret: %d\n", ret);
		goto out;
	}

	// sk: why is it searching again? probably to check some intermediate changes in ds_get_src_addr or ds_get_qp, but I can't understand
	//
	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (!tdest) {
		new_dest = calloc(1, sizeof(*new_dest));
		if (!new_dest) {
			ret = ERR(ENOMEM);
			goto out;
		}

		assert(qp != NULL);

		memcpy(&new_dest->addr, addr, addrlen);
		new_dest->qp = qp;
		tdest = tsearch(&new_dest->addr, &rs->dest_map, ds_compare_addr);
	}

found:
	*dest = *tdest;
out:
	fastlock_release(&rs->map_lock);
	return ret;
}

static int ds_get_dest_mcast(struct rsocket *rs, const struct sockaddr *addr,
							 socklen_t addrlen, struct sockaddr *srcaddr, struct ds_dest **dest)
{
	union socket_addr src_addr;
	socklen_t src_len;
	struct ds_qp *qp = NULL;
	struct ds_dest **tdest, *new_dest;
	int ret = 0;

	fastlock_acquire(&rs->map_lock);

	// do we have this destination address in the dest_map?
	// if so, we found one!
	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (tdest)
		goto found;

	memcpy(&src_addr, srcaddr, addrlen);

	// now that we know which interface to use, let's make a qp
	// but, we just want one qp per interface, so reuse one if already exists
	ret = ds_get_qp(rs, &src_addr, src_len, &qp);
	if (ret) {
		fprintf(stderr, "ds_get_dest ds_get_qp error ret: %d\n", ret);
		goto out;
	}

	// sk: why is it searching again? probably to check some intermediate changes in ds_get_src_addr or ds_get_qp, but I can't understand
	//
	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (!tdest) {
		new_dest = calloc(1, sizeof(*new_dest));
		if (!new_dest) {
			ret = ERR(ENOMEM);
			goto out;
		}

		assert(qp != NULL);

		memcpy(&new_dest->addr, addr, addrlen);
		new_dest->qp = qp;
		tdest = tsearch(&new_dest->addr, &rs->dest_map, ds_compare_addr);
	}

found:
	*dest = *tdest;
out:
	fastlock_release(&rs->map_lock);
	return ret;
}

static ssize_t ds_send_udp(struct rsocket *rs, const void *buf, size_t len,
						   int flags, uint8_t op);
static int rs_nonblocking(struct rsocket *rs, int flags);

int rconnect(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;
	struct timeval tv, tv_diff;

	rs = idm_at(&idm, socket);

	if (rs->state == rs_init) {
		ret = ds_init_ep(rs);
		if (ret)
			return ret;
	}

	fastlock_acquire(&rs->slock);
	if (!rs->conn_dest || ds_compare_addr(addr, &rs->conn_dest->addr)) {
		ret = ds_get_dest(rs, addr, addrlen, &rs->conn_dest);
	}
	fastlock_release(&rs->slock);

	gettimeofday(&tv, NULL);
	timersub(&tv, &rs->last_udp_sent, &tv_diff);
	// 5ms interval
	if (tv_diff.tv_sec == 0 && tv_diff.tv_usec <= 5000) {
		if (!rs->conn_dest->ah) return ERR(EWOULDBLOCK);
	}
		
	do {
		fastlock_acquire(&rs->slock);
		// sk: originally calling connect, but changed for benchmarking.
		ds_send_udp(rs, NULL, 0, 0, RS_OP_DATA);
#ifdef VERBOSE
		fprintf(stderr, "rconnect %d sending udp\n", socket);
#endif
		rs->last_udp_sent = tv;
		fastlock_release(&rs->slock);

		if (!rs_nonblocking(rs, 0) && !rs->conn_dest->ah) {
			usleep(1000);
		}
	} while (!rs_nonblocking(rs, 0) && !rs->conn_dest->ah);

	if (!rs->conn_dest->ah) return ERR(EWOULDBLOCK);

#ifdef VERBOSE
	fprintf(stderr, "rconnet %d we got ah!\n", socket);
#endif
	
	return ret;
}

static int __ds_post_send(struct rsocket *rs, struct ibv_sge *sge,
						  int num_sge,
						  uint32_t wr_data, int forward)
{
	struct ibv_send_wr wr, *bad;
	int ret;
	struct ds_dest *dest = (rs->mcast_dest ? rs->mcast_dest : rs->conn_dest);

	if (forward) {
		wr.wr_id = rs_forward_wr_id(wr_data);
	} else {
		wr.wr_id = rs_send_wr_id(wr_data);
	}
	wr.next = NULL;
	wr.sg_list = sge;
	wr.num_sge = num_sge;
	wr.opcode = IBV_WR_SEND;
	if (num_sge == 1) {
		wr.send_flags = (sge->length <= rs->sq_inline) ? IBV_SEND_INLINE : 0;
	} else if (num_sge == 2) {
		wr.send_flags = (sge[0].length + sge[1].length <= rs->sq_inline) ? IBV_SEND_INLINE : 0;
	} else {
		assert(false);
	}

	wr.wr.ud.ah = dest->ah;
	wr.wr.ud.remote_qpn = dest->qpn;
	wr.wr.ud.remote_qkey = RDMA_UDP_QKEY;

	ret = rdma_seterrno(ibv_post_send(dest->qp->cm_id->qp, &wr, &bad));
	
	return ret;
}

static int ds_post_send(struct rsocket *rs, struct ibv_sge *sge, int num_sge,
						uint32_t wr_data)
{
	return __ds_post_send(rs, sge, num_sge, wr_data, 0);
}

static int ds_post_forward(struct rsocket *rs, struct ibv_sge *sge,
						   uint32_t wr_data) {
	return __ds_post_send(rs, sge, 1, wr_data, 1);
}


static int ds_valid_recv(struct ds_qp *qp, struct ibv_wc *wc)
{
	struct ds_header *hdr;

	hdr = (struct ds_header *) (qp->rbuf + rs_wr_data(wc->wr_id));
	return ((wc->byte_len >= sizeof(struct ibv_grh) + DS_IPV4_HDR_LEN) &&
		((hdr->version == 4 && hdr->length == DS_IPV4_HDR_LEN) ||
		 (hdr->version == 6 && hdr->length == DS_IPV6_HDR_LEN)));
}

static void ds_set_src(struct sockaddr *addr, socklen_t *addrlen, struct ds_header *hdr);

/*
 * Poll all CQs associated with a datagram rsocket.  We need to drop any
 * received messages that we do not have room to store.  To limit drops,
 * we only poll if we have room to store the receive or we need a send
 * buffer.  To ensure fairness, we poll the CQs round robin, remembering
 * where we left off.
 */
static void ds_poll_cqs(struct rsocket *rs)
{
	struct ds_qp *qp;
	struct ds_smsg *smsg, *fmsg;
	struct ds_rmsg *rmsg;
	struct ibv_wc wc;
	int ret, cnt;

	if (!(qp = rs->qp_list))
		return;

	do {
		cnt = 0;
		do {
			ret = ibv_poll_cq(qp->cm_id->recv_cq, 1, &wc);
			if (ret <= 0) {
				qp = ds_next_qp(qp);
				continue;
			}

			if (rs_wr_is_recv(wc.wr_id)) {
				if (rs->opts & RS_OPT_RECVBUF_GPU) {
					if (rs->rqe_avail && wc.status == IBV_WC_SUCCESS) {
						volatile struct ds_rmsg_info *rmsg_info;
						volatile struct sockaddr_in *sa;
						struct ds_header *header = qp->recv_hdr + (rs_wr_data(wc.wr_id) / RS_SNDLOWAT);
						rs->rqe_avail--;
						rmsg_info = &rs->hostptr_dev_rbuf_recved_queue[rs->rbuf_recved_back];
						rmsg_info->ptr = (uintptr_t)qp->rbuf + rs_wr_data(wc.wr_id);
						rmsg_info->length = wc.byte_len - sizeof(struct ibv_grh) - DS_IPV4_HDR_LEN;
#ifdef VERBOSE
						fprintf(stderr, "ds_poll_cqs recved header version: %d, len recved total: %d, len recved with header: %zu, len recved data: %zu\n", header->version, wc.byte_len, wc.byte_len - sizeof(struct ibv_grh), wc.byte_len - sizeof(struct ibv_grh) - DS_IPV4_HDR_LEN);
#endif

						assert(header->version == 4);
						sa = (struct sockaddr_in*)rmsg_info->addr;
						rmsg_info->addrlen = sizeof(struct sockaddr_in);
						sa->sin_family = AF_INET;
						sa->sin_port = header->version;
						sa->sin_addr.s_addr = header->addr.ipv4;
						
						rmsg = &rs->dmsg[rs->rmsg_tail];
						rmsg->qp = qp;
						rmsg->offset = rs_wr_data(wc.wr_id);
						
						__sync_synchronize();
							
						if (++rs->rbuf_recved_back == rs->rq_size + 1)
							rs->rbuf_recved_back = 0;
						*rs->hostptr_dev_rbuf_recved_back = rs->rbuf_recved_back;

#ifdef VERBOSE	
						fprintf(stderr, "rbuf_recved_back: %d len: %d rs: %d\n", rs->rbuf_recved_back, wc.byte_len, rs->index);
#endif

						if (++rs->rmsg_tail == rs->rq_size + 1)
							rs->rmsg_tail = 0;

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
						recv_count++;
#endif

#ifdef VERBOSE
						fprintf(stderr, "received RDMA and handled!\n");
#endif						
					} else {
						fprintf(stderr, "ERROR: recv error: %s\n", ibv_wc_status_str(wc.status));
						exit(1);
						// originally the following code
						// ds_post_recv(rs, qp, rs_wr_data(wc.wr_id));
					}
				} else {
					if (rs->rqe_avail && wc.status == IBV_WC_SUCCESS &&
						ds_valid_recv(qp, &wc)) {
						rs->rqe_avail--;
						rmsg = &rs->dmsg[rs->rmsg_tail];
						rmsg->qp = qp;
						rmsg->offset = rs_wr_data(wc.wr_id);
						rmsg->length = wc.byte_len - sizeof(struct ibv_grh);
						if (++rs->rmsg_tail == rs->rq_size + 1)
							rs->rmsg_tail = 0;
					} else {
						ds_post_recv(rs, qp, rs_wr_data(wc.wr_id));
					}
				}
			} else if (rs_wr_is_forward(wc.wr_id)) {
#ifdef VERBOSE
				fprintf(stderr, "ds_poll_cqs qp: %p handling forward\n", qp);
#endif
				// forward from UDP. Only with SENDBUF_GPU
				// TODO: the CPU memory mapped should be also registration cached
				//       reg.cache will be implemented after checking gpunet func.
				fmsg = (struct ds_smsg *) (rs->fbuf + rs_wr_data(wc.wr_id));
				fmsg->next = rs->fmsg_free;
				rs->fmsg_free = fmsg;
				rs->sqe_avail++;
			} else {
#ifdef VERBOSE
				fprintf(stderr, "ds_poll_cqs qp: %p handling send\n", qp);
#endif

				if (wc.status != IBV_WC_SUCCESS) {
					fprintf(stderr, "ERROR: send %s\n", ibv_wc_status_str(wc.status));
					exit(1);
				}
#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
				send_count++;
#endif
				
				smsg = (struct ds_smsg *) (rs->sbuf + rs_wr_data(wc.wr_id));
				if (rs->opts & RS_OPT_SENDBUF_GPU) {
					// this message on gpu has been sent. so this is available!
					int sbuf_index = rs_wr_data(wc.wr_id) / RS_SNDLOWAT;
					rs->hostptr_dev_sbuf_free_queue[rs->sbuf_free_queue_back] = sbuf_index;
					__sync_synchronize();
					
					if (++rs->sbuf_free_queue_back == rs->sq_size + 1) {
						rs->sbuf_free_queue_back = 0;
					}
					*(rs->hostptr_dev_sbuf_free_queue_back) = rs->sbuf_free_queue_back;
				} else {
#ifdef PROFILE_SEND_COMPLETION
					int sbuf_index = rs_wr_data(wc.wr_id) / RS_SNDLOWAT;
					struct timeval tv, tv_diff;
					gettimeofday(&tv, NULL);
					timersub(&tv, &rs->send_start_tvs[sbuf_index], &tv_diff);
					rs->send_interval_sum += tv_diff.tv_sec * 1000.0 + tv_diff.tv_usec / 1000.0;
					if (++rs->send_interval_count == 10000) {
						fprintf(stderr, "send average latency: %.2f us\n", rs->send_interval_sum / rs->send_interval_count);
						rs->send_interval_count = 0;
						rs->send_interval_sum = 0.0;
					}
#endif

					smsg->next = rs->smsg_free;
					rs->smsg_free = smsg;

				}
				rs->sqe_avail++;
			}

			qp = ds_next_qp(qp);
			if (!rs->rqe_avail && rs->sqe_avail) {
				rs->qp_list = qp;
				return;
			}
			cnt++;
		} while (qp != rs->qp_list);
	} while (cnt);
}

static void ds_req_notify_cqs(struct rsocket *rs)
{
	struct ds_qp *qp;

	if (!(qp = rs->qp_list))
		return;

	do {
		if (!qp->cq_armed) {
			ibv_req_notify_cq(qp->cm_id->recv_cq, 0);
			qp->cq_armed = 1;
		}
		qp = ds_next_qp(qp);
	} while (qp != rs->qp_list);
}

static int ds_get_cq_event(struct rsocket *rs)
{
	struct epoll_event event;
	struct ds_qp *qp;
	struct ibv_cq *cq;
	void *context;
	int ret;

	if (!rs->cq_armed)
		return 0;

	ret = epoll_wait(rs->epfd, &event, 1, -1);
	if (ret <= 0)
		return ret;

	qp = event.data.ptr;
	ret = ibv_get_cq_event(qp->cm_id->recv_cq_channel, &cq, &context);
	if (!ret) {
		ibv_ack_cq_events(qp->cm_id->recv_cq, 1);
		qp->cq_armed = 0;
		rs->cq_armed = 0;
	}

	return ret;
}

static int ds_process_cqs(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	int ret = 0;

	fastlock_acquire(&rs->cq_lock);
	do {
		ds_poll_cqs(rs);
		if (test(rs)) {
			ret = 0;
			break;
		} else if (nonblock) {
			ret = ERR(EWOULDBLOCK);
		} else if (!rs->cq_armed) {
			ds_req_notify_cqs(rs);
			rs->cq_armed = 1;
		} else {
			fastlock_acquire(&rs->cq_wait_lock);
			fastlock_release(&rs->cq_lock);

			ret = ds_get_cq_event(rs);
			fastlock_release(&rs->cq_wait_lock);
			fastlock_acquire(&rs->cq_lock);
		}
	} while (!ret);

	fastlock_release(&rs->cq_lock);
	return ret;
}

static int ds_get_comp(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	struct timeval s, e;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = ds_process_cqs(rs, 1, test);
		if (!ret || nonblock || errno != EWOULDBLOCK)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	ret = ds_process_cqs(rs, 0, test);
	return ret;
}

static int rs_nonblocking(struct rsocket *rs, int flags)
{
	return (rs->fd_flags & O_NONBLOCK) || (flags & MSG_DONTWAIT);
}

static int rs_is_cq_armed(struct rsocket *rs)
{
	return rs->cq_armed;
}

static int rs_poll_all(struct rsocket *rs)
{
	return 1;
}

static int ds_can_send(struct rsocket *rs)
{
	return rs->sqe_avail;
}

static int ds_all_sends_done(struct rsocket *rs)
{
	return rs->sqe_avail == rs->sq_size;
}

static int rs_have_rdata(struct rsocket *rs)
{
	if (rs->opts & RS_OPT_RECVBUF_GPU) {
		// *rs->rbuf_recved_back is rmsg_tail
		// rs->rbuf_ack_queue_front is rmsg_head
		return (rs->rbuf_recved_back != (rs->rbuf_ack_queue_front + 1)) &&
			(rs->rbuf_ack_queue_front != rs->rbuf_recved_back + rs->rq_size);
	} else {
		return (rs->rmsg_head != rs->rmsg_tail);
	}
}

static void ds_set_src(struct sockaddr *addr, socklen_t *addrlen,
		       struct ds_header *hdr)
{
	union socket_addr sa;

	memset(&sa, 0, sizeof sa);
	
#ifdef VERBOSE
	fprintf(stderr, "ds_set_src hdr version: %d\n", hdr->version);
#endif

	if (hdr->version == 4) {
		if (*addrlen > sizeof(sa.sin))
			*addrlen = sizeof(sa.sin);

		sa.sin.sin_family = AF_INET;
		sa.sin.sin_port = hdr->port;
		sa.sin.sin_addr.s_addr =  hdr->addr.ipv4;
	} else {
		if (*addrlen > sizeof(sa.sin6))
			*addrlen = sizeof(sa.sin6);

		sa.sin6.sin6_family = AF_INET6;
		sa.sin6.sin6_port = hdr->port;
		sa.sin6.sin6_flowinfo = hdr->addr.ipv6.flowinfo;
		memcpy(&sa.sin6.sin6_addr, &hdr->addr.ipv6.addr, 16);
	}
	memcpy(addr, &sa, *addrlen);
}

static ssize_t ds_recvfrom(struct rsocket *rs, void *buf, size_t len, int flags,
			   struct sockaddr *src_addr, socklen_t *addrlen)
{
	struct ds_rmsg *rmsg;
	struct ds_header *hdr;
	int ret;

	if (!(rs->state & rs_readable))
		return ERR(EINVAL);

	if (!rs_have_rdata(rs)) {
		fastlock_release(&rs->rlock);

#ifdef VERBOSE
		fprintf(stderr, "ds_recvfrom() no received data! calling ds_get_comp \n");
#endif
        do {
            fastlock_acquire(&rs->slock);
            ret = ds_get_comp(rs, 1, rs_have_rdata);
            fastlock_release(&rs->slock);
        } while (ret != 0 && !rs_nonblocking(rs, flags) && (errno == EWOULDBLOCK));
		
		fastlock_acquire(&rs->rlock);
		if (ret)
			return ret;
	}

	rmsg = &rs->dmsg[rs->rmsg_head];
	hdr = (struct ds_header *) (rmsg->qp->rbuf + rmsg->offset);
	if (len > rmsg->length - hdr->length)
		len = rmsg->length - hdr->length;

	memcpy(buf, (void *) hdr + hdr->length, len);
	if (addrlen)
		ds_set_src(src_addr, addrlen, hdr);

	if (!(flags & MSG_PEEK)) {
		ds_post_recv(rs, rmsg->qp, rmsg->offset);
		if (++rs->rmsg_head == rs->rq_size + 1)
			rs->rmsg_head = 0;
		rs->rqe_avail++;
	}

	return len;
}

/*
 * Continue to receive any queued data even if the remote side has disconnected.
 */
ssize_t rrecv(int socket, void *buf, size_t len, int flags)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	fastlock_acquire(&rs->rlock);
	ret = ds_recvfrom(rs, buf, len, flags, NULL, 0);
	fastlock_release(&rs->rlock);
	return ret;
}

ssize_t rrecvfrom(int socket, void *buf, size_t len, int flags,
		  struct sockaddr *src_addr, socklen_t *addrlen)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	fastlock_acquire(&rs->rlock);
	ret = ds_recvfrom(rs, buf, len, flags, src_addr, addrlen);
	fastlock_release(&rs->rlock);

	return ret;
}

ssize_t rread(int socket, void *buf, size_t count)
{
	return rrecv(socket, buf, count, 0);
}

static ssize_t ds_sendv_udp(struct rsocket *rs, const struct iovec *iov,
			    int iovcnt, int flags, uint8_t op)
{
	struct ds_udp_header hdr;
	struct msghdr msg;
	struct iovec miov[8];
	ssize_t ret;

	if (iovcnt > 8)
		return ERR(ENOTSUP);

	hdr.tag = htonl(DS_UDP_TAG);
	hdr.version = rs->conn_dest->qp->send_hdr->version;
	hdr.op = op;
	hdr.reserved = 0;
	hdr.qpn = htonl(rs->conn_dest->qp->cm_id->qp->qp_num & 0xFFFFFF);
	if (rs->conn_dest->qp->send_hdr->version == 4) {
		hdr.length = DS_UDP_IPV4_HDR_LEN;
		hdr.addr.ipv4 = rs->conn_dest->qp->send_hdr->addr.ipv4;
	}
	/*
	  sk: let's not support IPv6 for now.
	  else {
		hdr.length = DS_UDP_IPV6_HDR_LEN;
		memcpy(hdr.addr.ipv6, &rs->conn_dest->qp->send_hdr->addr.ipv6, 16);
	}
	*/

	miov[0].iov_base = &hdr;
	miov[0].iov_len = hdr.length;
	if (iov && iovcnt)
		memcpy(&miov[1], iov, sizeof *iov * iovcnt);

	memset(&msg, 0, sizeof msg);
	msg.msg_name = &rs->conn_dest->addr;
	msg.msg_namelen = ucma_addrlen(&rs->conn_dest->addr.sa);
	msg.msg_iov = miov;
	msg.msg_iovlen = iovcnt + 1;
	ret = sendmsg(rs->udp_sock, &msg, flags);
	return ret > 0 ? ret - hdr.length : ret;
}

static ssize_t ds_send_udp(struct rsocket *rs, const void *buf, size_t len,
			   int flags, uint8_t op)
{
	struct iovec iov;
	if (buf && len) {
		iov.iov_base = (void *) buf;
		iov.iov_len = len;
		return ds_sendv_udp(rs, &iov, 1, flags, op);
	} else {
		return ds_sendv_udp(rs, NULL, 0, flags, op);
	}
}

static ssize_t dsend(struct rsocket *rs, const void *buf, size_t len, int flags)
{
	struct ds_smsg *msg;
	struct ibv_sge sge;
	uint64_t offset;
	int ret = 0;

	struct ds_dest *dest = (rs->mcast_dest ? rs->mcast_dest : rs->conn_dest);

	if (!dest->ah)
		return ds_send_udp(rs, buf, len, flags, RS_OP_DATA);

	if (!ds_can_send(rs)) {
		ret = ds_get_comp(rs, rs_nonblocking(rs, flags), ds_can_send);
		if (ret)
			return ret;
	}

	msg = rs->smsg_free;
	rs->smsg_free = msg->next;

	memcpy((void *) msg, dest->qp->send_hdr, dest->qp->send_hdr->length);
	memcpy((void *) msg + dest->qp->send_hdr->length, buf, len);
	rs->sqe_avail--;

	sge.addr = (uintptr_t) msg;
	sge.length = dest->qp->send_hdr->length + len;
	sge.lkey = dest->qp->smr->lkey;
	offset = (uint8_t *) msg - rs->sbuf;

#ifdef PROFILE_SEND_COMPLETION
	gettimeofday(&rs->send_start_tvs[offset / RS_SNDLOWAT], NULL);
#endif


	ret = ds_post_send(rs, &sge, 1, offset);
	return ret ? ret : len;
}


ssize_t rsend(int socket, const void *buf, size_t len, int flags)
{
	struct rsocket *rs;
	int ret = 0;

	rs = idm_at(&idm, socket);
	fastlock_acquire(&rs->slock);
	ret = dsend(rs, buf, len, flags);
	fastlock_release(&rs->slock);
	return ret;
}

int ds_send_udp_handler(struct cu_memcpy_async_req* req);

static ssize_t __dsendto_gpu(struct rsocket *rs, void *gpu_buf, size_t len,
							 const struct sockaddr *dest_addr, socklen_t addrlen) {
	struct ibv_sge sge[2];
	uint64_t offset;
	int ret = 0;

	if (!rs->conn_dest || ds_compare_addr(dest_addr, &rs->conn_dest->addr)) {
		ret = ds_get_dest(rs, dest_addr, addrlen, &rs->conn_dest);
		if (ret) {
			fprintf(stderr, "__dsendto_gpu() ds_get_dest ret: %d\n", ret);
			return ret;
		}
	}

	if (!rs->conn_dest->ah) {
		assert(rs->conn_dest);
		// udp cannot access gpu buffer, so use bounce buffer
		// TODO: check if the hostptr is the right one to use for async copy.
		uint8_t *cpu_buf = rs->hostptr_host_sbuf_bounce + ((uint8_t*)gpu_buf - rs->sbuf);
		register_cu_memcpy_req(rs, MEMCPY_TYPE_GPU_TO_CPU,
							   gpu_buf, cpu_buf, len, ds_send_udp_handler);
		return len;
	}
	
	if (!ds_can_send(rs)) {
		ret = ds_get_comp(rs, rs_nonblocking(rs, 0), ds_can_send);
		if (ret)
			return ret;
	}

	rs->sqe_avail--;
	
	sge[0].addr = (uintptr_t)rs->conn_dest->qp->send_hdr;
	sge[0].length = rs->conn_dest->qp->send_hdr->length;
	sge[0].lkey = rs->conn_dest->qp->send_hdr_mr->lkey;
	sge[1].addr = (uintptr_t)gpu_buf;
	sge[1].length = len;
	sge[1].lkey = rs->conn_dest->qp->smr->lkey;

#ifdef VERBOSE
	fprintf(stderr, "__dsendto_gpu posting buf: %p, len: %d\n", gpu_buf, (int)len);
#endif

	offset = (uint8_t *) gpu_buf - rs->sbuf;

	ret = ds_post_send(rs, sge, 2, offset);

#ifdef VERBOSE
	fprintf(stderr, "__dsendto_gpu RDMA send posted. ret: %d hdr version: %d, length: %d, gpu_buf: %p\n", ret, rs->conn_dest->qp->send_hdr->version, (int)len, gpu_buf);
#endif

	return ret ? ret : len;
}

ssize_t rsendto(int socket, const void *buf, size_t len, int flags,
		const struct sockaddr *dest_addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	
	if (rs->state == rs_init) {
		ret = ds_init_ep(rs);
		if (ret) {
			fprintf(stderr, "ds_init_ep error\n");
			return ret;
		}
	}

	fastlock_acquire(&rs->slock);
	if (!rs->mcast_dest) {
		if (!rs->conn_dest || ds_compare_addr(dest_addr, &rs->conn_dest->addr)) {
			ret = ds_get_dest(rs, dest_addr, addrlen, &rs->conn_dest);
			if (ret) {
				fprintf(stderr, "ds_get_dest error\n");
				goto out;
			}
		}
	}

	ret = dsend(rs, buf, len, flags);
out:
	fastlock_release(&rs->slock);
	return ret;
}

ssize_t rwrite(int socket, const void *buf, size_t count)
{
	return rsend(socket, buf, count, 0);
}

static struct pollfd *rs_fds_alloc(nfds_t nfds)
{
	static __thread struct pollfd *rfds;
	static __thread nfds_t rnfds;

	if (nfds > rnfds) {
		if (rfds)
			free(rfds);

		rfds = malloc(sizeof *rfds * nfds);
		rnfds = rfds ? nfds : 0;
	}

	return rfds;
}

static int rs_poll_rs(struct rsocket *rs, int events,
		      int nonblock, int (*test)(struct rsocket *rs))
{
	short revents;

	ds_process_cqs(rs, nonblock, test);
	
	revents = 0;
	if ((events & POLLIN) && rs_have_rdata(rs))
		revents |= POLLIN;
	if ((events & POLLOUT) && ds_can_send(rs))
		revents |= POLLOUT;
	
	return revents;
}

static int rs_poll_check(struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		rs = idm_lookup(&idm, fds[i].fd);
		if (rs)
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 1, rs_poll_all);
		else
			poll(&fds[i], 1, 0);

		if (fds[i].revents)
			cnt++;
	}
	return cnt;
}

static int rs_poll_arm(struct pollfd *rfds, struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i;

	for (i = 0; i < nfds; i++) {
		rs = idm_lookup(&idm, fds[i].fd);
		if (rs) {
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 0, rs_is_cq_armed);
			if (fds[i].revents)
				return 1;

			rfds[i].fd = rs->epfd;
			rfds[i].events = POLLIN;
		} else {
			rfds[i].fd = fds[i].fd;
			rfds[i].events = fds[i].events;
		}
		rfds[i].revents = 0;
	}
	return 0;
}

static int rs_poll_events(struct pollfd *rfds, struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		if (!rfds[i].revents)
			continue;

		rs = idm_lookup(&idm, fds[i].fd);
		if (rs) {
			fastlock_acquire(&rs->cq_wait_lock);
			ds_get_cq_event(rs);
			fastlock_release(&rs->cq_wait_lock);
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 1, rs_poll_all);
		} else {
			fds[i].revents = rfds[i].revents;
		}
		if (fds[i].revents)
			cnt++;
	}
	return cnt;
}

/*
 * We need to poll *all* fd's that the user specifies at least once.
 * Note that we may receive events on an rsocket that may not be reported
 * to the user (e.g. connection events or credit updates).  Process those
 * events, then return to polling until we find ones of interest.
 */
int rpoll(struct pollfd *fds, nfds_t nfds, int timeout)
{
	struct timeval s, e;
	struct pollfd *rfds;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = rs_poll_check(fds, nfds);
		if (ret || !timeout)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	rfds = rs_fds_alloc(nfds);
	if (!rfds)
		return ERR(ENOMEM);

	do {
		ret = rs_poll_arm(rfds, fds, nfds);
		if (ret)
			break;

		ret = poll(rfds, nfds, timeout);
		if (ret <= 0)
			break;

		ret = rs_poll_events(rfds, fds, nfds);
	} while (!ret);

	return ret;
}

static struct pollfd *
rs_select_to_poll(int *nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	struct pollfd *fds;
	int fd, i = 0;

	fds = calloc(*nfds, sizeof *fds);
	if (!fds)
		return NULL;

	for (fd = 0; fd < *nfds; fd++) {
		if (readfds && FD_ISSET(fd, readfds)) {
			fds[i].fd = fd;
			fds[i].events = POLLIN;
		}

		if (writefds && FD_ISSET(fd, writefds)) {
			fds[i].fd = fd;
			fds[i].events |= POLLOUT;
		}

		if (exceptfds && FD_ISSET(fd, exceptfds))
			fds[i].fd = fd;

		if (fds[i].fd)
			i++;
	}

	*nfds = i;
	return fds;
}

static int
rs_poll_to_select(int nfds, struct pollfd *fds, fd_set *readfds,
		  fd_set *writefds, fd_set *exceptfds)
{
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		if (readfds && (fds[i].revents & (POLLIN | POLLHUP))) {
			FD_SET(fds[i].fd, readfds);
			cnt++;
		}

		if (writefds && (fds[i].revents & POLLOUT)) {
			FD_SET(fds[i].fd, writefds);
			cnt++;
		}

		if (exceptfds && (fds[i].revents & ~(POLLIN | POLLOUT))) {
			FD_SET(fds[i].fd, exceptfds);
			cnt++;
		}
	}
	return cnt;
}

static int rs_convert_timeout(struct timeval *timeout)
{
	return !timeout ? -1 :
		timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
}

int rselect(int nfds, fd_set *readfds, fd_set *writefds,
	    fd_set *exceptfds, struct timeval *timeout)
{
	struct pollfd *fds;
	int ret;

	fds = rs_select_to_poll(&nfds, readfds, writefds, exceptfds);
	if (!fds)
		return ERR(ENOMEM);

	ret = rpoll(fds, nfds, rs_convert_timeout(timeout));

	if (readfds)
		FD_ZERO(readfds);
	if (writefds)
		FD_ZERO(writefds);
	if (exceptfds)
		FD_ZERO(exceptfds);

	if (ret > 0)
		ret = rs_poll_to_select(nfds, fds, readfds, writefds, exceptfds);

	free(fds);
	return ret;
}

/*
 * For graceful disconnect, notify the remote side that we're
 * disconnecting and wait until all outstanding sends complete, provided
 * that the remote side has not sent a disconnect message.
 */
int rshutdown(int socket, int how)
{
	struct rsocket *rs;
	int ret = 0;

	rs = idm_at(&idm, socket);

	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, 0);
	
	if ((rs->fd_flags & O_NONBLOCK) && (rs->state & rs_connected))
		rs_set_nonblocking(rs, rs->fd_flags);

	return ret;
}

static void ds_shutdown(struct rsocket *rs)
{
	if (rs->opts & RS_OPT_SVC_ACTIVE)
		rs_notify_svc(&udp_svc, rs, RS_SVC_REM_DGRAM);

	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, 0);

	rs->state &= ~(rs_readable | rs_writable);
	ds_process_cqs(rs, 0, ds_all_sends_done);

	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, rs->fd_flags);
}

int rclose(int socket)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	ds_shutdown(rs);

	rs_free(rs);
	return 0;
}

int rgetpeername(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	return getpeername(rs->udp_sock, addr, addrlen);
}

int rgetsockname(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	return getsockname(rs->udp_sock, addr, addrlen);
}

int ipv4_is_multicast(uint32_t addr) {
	return (addr & htonl(0xf0000000)) == htonl(0xe0000000);
}

int rsetsockopt(int socket, int level, int optname,
		const void *optval, socklen_t optlen)
{
	struct rsocket *rs;
	int ret, opt_on = 0;
	uint64_t *opts = NULL;

	ret = ERR(ENOTSUP);
	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_DGRAM && (level != SOL_RDMA && level != IPPROTO_IP)) {
		ret = setsockopt(rs->udp_sock, level, optname, optval, optlen);
		if (ret)
			return ret;
	}

	switch (level) {
	case SOL_RDMA:
		if (rs->state >= rs_opening) {
			ret = ERR(EINVAL);
			break;
		}

		switch (optname) {
		case RDMA_SQSIZE:
			rs->sq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			ret = 0;
			break;
		case RDMA_RQSIZE:
			rs->rq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			ret = 0;
			break;
		case RDMA_INLINE:
			rs->sq_inline = min(*(uint32_t *) optval, RS_QP_MAX_SIZE);
			if (rs->sq_inline < RS_MIN_INLINE)
				rs->sq_inline = RS_MIN_INLINE;
			ret = 0;
			break;
		case RDMA_ROUTE:
			if ((rs->optval = calloc(optlen, 1))) {
				memcpy(rs->optval, optval, optlen);
				rs->optlen = optlen;
				ret = 0;
			} else {
				ret = ERR(ENOMEM);
			}
			break;

        case RDMA_GPU_ID:
            rs->gpu_dev_id = *(int*)optval;
            ret = 0;
            break;

        case RDMA_SEND_GPUBUF:
            if (*(int*)optval)
                rs->opts |= RS_OPT_SENDBUF_GPU;
            else
                rs->opts &= ~RS_OPT_SENDBUF_GPU;
            ret = 0;
            break;

        case RDMA_RECV_GPUBUF:
            if (*(int*)optval)
                rs->opts |= RS_OPT_RECVBUF_GPU;
            else
                rs->opts &= ~RS_OPT_RECVBUF_GPU;
            ret = 0;
            break;

        case RDMA_SEND_GPUBUF_BOUNCE:
            if (*(int*)optval)
                rs->opts |= RS_OPT_SENDBUF_BOUNCE;
            else
                rs->opts &= ~RS_OPT_SENDBUF_BOUNCE;
            ret = 0;
            break;

        case RDMA_RECV_GPUBUF_BOUNCE:
            if (*(int*)optval)
                rs->opts |= RS_OPT_RECVBUF_BOUNCE;
            else
                rs->opts &= ~RS_OPT_RECVBUF_BOUNCE;
            ret = 0;
            break;

		default:
			break;
		}
		break;
	case IPPROTO_IP:
		// For documentation, run 'man -s 7 ip'
		switch (optname) {
			
		case IP_ADD_MEMBERSHIP:
		{
			// sk: we only support old way of multicasting.
			// TODO: support ip_mreqn
			struct ip_mreq *mreq;

			if (optlen < sizeof(struct ip_mreq)) {
				ret = ERR(EPROTO);
				break;
			}

			mreq = (struct ip_mreq*)optval;

			if (!ipv4_is_multicast(mreq->imr_multiaddr.s_addr)) {
				fprintf(stderr, "ERROR: address is not multicast address\n");
				ret = ERR(EINVAL);
				break;
			}

			// TODO: multiple multicast addresses for a socket

			// now setup the multicasting.
			// this is like binding the source addr and then adding
			// another destination and a qp for it.

			if (!rs->mcast_dest) {
				struct sockaddr_in mcast_src, mcast_dst;
				struct rdma_cm_id *cm_id;

				memset(&mcast_dst, 0, sizeof(mcast_dst));
				memset(&mcast_src, 0, sizeof(mcast_src));
				
				mcast_src.sin_family = AF_INET;
				mcast_src.sin_addr.s_addr = mreq->imr_interface.s_addr;

				mcast_dst.sin_family = AF_INET;
				mcast_dst.sin_addr.s_addr = mreq->imr_multiaddr.s_addr;

				//
				// get the qp for the address
				// 
				ret = ds_get_dest_mcast(rs, (struct sockaddr*)&mcast_dst,
										sizeof(struct sockaddr_in),
										(struct sockaddr*)&mcast_src, &rs->mcast_dest);
				if (ret) {
					fprintf(stderr, "IP_ADD_MEMBERSHIP ds_get_dest error\n");
					break;
				}

				cm_id = rs->mcast_dest->qp->cm_id;

				ret = rdma_resolve_addr(cm_id, (struct sockaddr*)&mcast_src,
										(struct sockaddr*)&mcast_dst, 2000);
				if (ret) {
					fprintf(stderr, "IP_ADD_MEMBERSHIP rdma_resolve_addr error\n");
					break;
				}

				if(cm_id->event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
					fprintf(stderr, "event: %d expected: %d\n", cm_id->event->event, RDMA_CM_EVENT_ADDR_RESOLVED);
					exit(1);
				}
				
				ret = rdma_join_multicast(cm_id, (struct sockaddr*)&mcast_dst, NULL);
				if (ret) {
					fprintf(stderr, "IP_ADD_MEMBERSHIP rdma_join_multicast error\n");
					break;
				}

				if(cm_id->event->event != RDMA_CM_EVENT_MULTICAST_JOIN) {
					fprintf(stderr, "ERROR: NOT MUSTICAST JOIN%d \n", cm_id->event->event);
					exit(1);
				}

				if (cm_id->event->status) {
					fprintf(stderr, "ERROR: rdma_get_cm_event status: %d \n", cm_id->event->status);
					exit(1);
				}

				
				struct rdma_ud_param *ud_param = &cm_id->event->param.ud;
#ifdef VERBOSE
				char buf[40];
				inet_ntop(AF_INET6, ud_param->ah_attr.grh.dgid.raw, buf, 40);
				printf("rsocket: joined dgid: %s mlid 0x%x sl %d\n", buf,
					   ud_param->ah_attr.dlid, ud_param->ah_attr.sl);
#endif

				rs->mcast_dest->qpn = ud_param->qp_num;
				//rs->mcast_dest->qkey = ud_param->qkey; (RDMA_UDP_QKEY)
				rs->mcast_dest->ah = ibv_create_ah(cm_id->pd, &ud_param->ah_attr);

				ret = 0;
			}

			break;
		}

		case IP_DROP_MEMBERSHIP:
		{
			struct ip_mreqn *mreq;
			struct sockaddr_in addr_dst;

			if (optlen != sizeof(struct ip_mreqn) || optlen != sizeof(struct ip_mreq)) {
				ret = ERR(EINVAL);
				break;
			}

			if (!rs->mcast_dest) {
				ret = ERR(EINVAL);
				break;
			}

			mreq = (struct ip_mreqn*)optval;

			if (((struct sockaddr_in*)&rs->mcast_dest->addr)->sin_addr.s_addr != mreq->imr_multiaddr.s_addr) {
				ret = ERR(EINVAL);
				break;
			}

			addr_dst.sin_family = AF_INET;
			addr_dst.sin_addr.s_addr = ((struct sockaddr_in*)&rs->mcast_dest->addr)->sin_addr.s_addr;
			ret = rdma_leave_multicast(rs->mcast_dest->qp->cm_id, (struct sockaddr*)&addr_dst);
			break;
		}

		// unsupported ones, but higher priority
		case IP_MULTICAST_IF:
		case IP_MULTICAST_LOOP:
		case IP_MULTICAST_TTL:
			
		// unsupported ones with lower priority
		case IP_ADD_SOURCE_MEMBERSHIP:
		case IP_DROP_SOURCE_MEMBERSHIP:
		case IP_BLOCK_SOURCE:
		case IP_UNBLOCK_SOURCE:
		case IP_MSFILTER:
		case IP_MULTICAST_ALL:
		default:
			break;
		}
		break;
	default:
		break;
	}

	if (!ret && opts) {
		if (opt_on)
			*opts |= (1 << optname);
		else
			*opts &= ~(1 << optname);
	}

	return ret;
}

int rgetsockopt(int socket, int level, int optname,
		void *optval, socklen_t *optlen)
{
	struct rsocket *rs;
	int ret = 0;

	rs = idm_at(&idm, socket);
	switch (level) {
	case SOL_SOCKET:
		switch (optname) {
		case SO_REUSEADDR:
		case SO_KEEPALIVE:
		case SO_OOBINLINE:
			*((int *) optval) = !!(rs->so_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		case SO_RCVBUF:
			*((int *) optval) = rs->rbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_SNDBUF:
			*((int *) optval) = rs->sbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_LINGER:
			/* Value is inverted so default so_opt = 0 is on */
			((struct linger *) optval)->l_onoff =
					!(rs->so_opts & (1 << optname));
			((struct linger *) optval)->l_linger = 0;
			*optlen = sizeof(struct linger);
			break;
		case SO_ERROR:
			*((int *) optval) = rs->err;
			*optlen = sizeof(int);
			rs->err = 0;
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case IPPROTO_TCP:
		switch (optname) {
		case TCP_KEEPCNT:
		case TCP_KEEPINTVL:
			*((int *) optval) = 1;   /* N/A */
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case IPPROTO_IPV6:
		switch (optname) {
		case IPV6_V6ONLY:
			*((int *) optval) = !!(rs->ipv6_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case SOL_RDMA:
		switch (optname) {
		case RDMA_SQSIZE:
			*((int *) optval) = rs->sq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_RQSIZE:
			*((int *) optval) = rs->rq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_INLINE:
			*((int *) optval) = rs->sq_inline;
			*optlen = sizeof(int);
			break;

        case RDMA_GPU_ID:
            rs->gpu_dev_id = *(int*)optval;
            ret = 0;
            break;

        case RDMA_SEND_GPUBUF:
            if (*(int*)optval)
                rs->opts |= RS_OPT_SENDBUF_GPU;
            else
                rs->opts &= ~RS_OPT_SENDBUF_GPU;
            ret = 0;
            break;

        case RDMA_RECV_GPUBUF:
            if (*(int*)optval)
                rs->opts |= RS_OPT_RECVBUF_GPU;
            else
                rs->opts &= ~RS_OPT_RECVBUF_GPU;
            ret = 0;
            break;

        case RDMA_SEND_GPUBUF_BOUNCE:
            if (*(int*)optval)
                rs->opts |= RS_OPT_SENDBUF_BOUNCE;
            else
                rs->opts &= ~RS_OPT_SENDBUF_BOUNCE;
            ret = 0;
            break;

        case RDMA_RECV_GPUBUF_BOUNCE:
            if (*(int*)optval)
                rs->opts |= RS_OPT_RECVBUF_BOUNCE;
            else
                rs->opts &= ~RS_OPT_RECVBUF_BOUNCE;
            ret = 0;
            break;

		default:
			ret = ENOTSUP;
			break;
		}
		break;
	default:
		ret = ENOTSUP;
		break;
	}

	return rdma_seterrno(ret);
}

int rfcntl(int socket, int cmd, ... /* arg */ )
{
	struct rsocket *rs;
	va_list args;
	long param;
	int ret = 0;

	rs = idm_at(&idm, socket);
	va_start(args, cmd);
	switch (cmd) {
	case F_GETFL:
		ret = (int) rs->fd_flags;
		break;
	case F_SETFL:
		param = va_arg(args, long);
		if (param & O_NONBLOCK)
			ret = rs_set_nonblocking(rs, O_NONBLOCK);

		if (!ret)
			rs->fd_flags |= param;
		break;
	default:
		ret = ERR(ENOTSUP);
		break;
	}
	va_end(args);
	return ret;
}

/****************************************************************************
 * Service Processing Threads
 ****************************************************************************/

static int rs_svc_grow_sets(struct rs_svc *svc, int grow_size)
{
	struct rsocket **rss;
	void *set, *contexts;

	set = calloc(svc->size + grow_size, sizeof(*rss) + svc->context_size);
	if (!set)
		return ENOMEM;

	svc->size += grow_size;
	rss = set;
	contexts = set + sizeof(*rss) * svc->size;
	if (svc->cnt) {
		memcpy(rss, svc->rss, sizeof(*rss) * (svc->cnt + 1));
		memcpy(contexts, svc->contexts, svc->context_size * (svc->cnt + 1));
	}

	free(svc->rss);
	svc->rss = rss;
	svc->contexts = contexts;
	return 0;
}

/*
 * Index 0 is reserved for the service's communication socket.
 */
static int rs_svc_add_rs(struct rs_svc *svc, struct rsocket *rs)
{
	int ret;

	if (svc->cnt >= svc->size - 1) {
		ret = rs_svc_grow_sets(svc, 80);
		if (ret)
			return ret;
	}

	svc->rss[++svc->cnt] = rs;
	return 0;
}

static int rs_svc_rm_rs(struct rs_svc *svc, struct rsocket *rs)
{
	int i;

	for (i = 1; i <= svc->cnt; i++) {
		if (svc->rss[i] == rs) {
			svc->cnt--;
			svc->rss[i] = svc->rss[svc->cnt];
			memcpy(svc->contexts + i * svc->context_size,
			       svc->contexts + svc->cnt * svc->context_size,
			       svc->context_size);
			return 0;
		}
	}
	return EBADF;
}

static void udp_svc_process_sock(struct rs_svc *svc)
{
	struct rs_svc_msg msg;

	read(svc->sock[1], &msg, sizeof msg);
	switch (msg.cmd) {
	case RS_SVC_ADD_DGRAM:
		msg.status = rs_svc_add_rs(svc, msg.rs);
		if (!msg.status) {
			msg.rs->opts |= RS_OPT_SVC_ACTIVE;
			udp_svc_fds = svc->contexts;
			udp_svc_fds[svc->cnt].fd = msg.rs->udp_sock;
			udp_svc_fds[svc->cnt].events = POLLIN;
			udp_svc_fds[svc->cnt].revents = 0;
		}
		break;
	case RS_SVC_REM_DGRAM:
		msg.status = rs_svc_rm_rs(svc, msg.rs);
		if (!msg.status)
			msg.rs->opts &= ~RS_OPT_SVC_ACTIVE;
		break;
	case RS_SVC_NOOP:
		msg.status = 0;
		break;
	default:
		break;
	}

	write(svc->sock[1], &msg, sizeof msg);
}

static uint8_t udp_svc_sgid_index(struct ds_dest *dest, union ibv_gid *sgid)
{
	union ibv_gid gid;
	int i;

	for (i = 0; i < 16; i++) {
		ibv_query_gid(dest->qp->cm_id->verbs, dest->qp->cm_id->port_num,
			      i, &gid);
		if (!memcmp(sgid, &gid, sizeof gid))
			return i;
	}
	return 0;
}

static uint8_t udp_svc_path_bits(struct ds_dest *dest)
{
	struct ibv_port_attr attr;

	if (!ibv_query_port(dest->qp->cm_id->verbs, dest->qp->cm_id->port_num, &attr))
		return (uint8_t) ((1 << attr.lmc) - 1);
	return 0x7f;
}

static void udp_svc_create_ah(struct rsocket *rs, struct ds_dest *dest, uint32_t qpn)
{
	union socket_addr saddr;
	struct rdma_cm_id *id;
	struct ibv_ah_attr attr;
	int ret;

	if (dest->ah) {
		fastlock_acquire(&rs->slock);
		ibv_destroy_ah(dest->ah);
		dest->ah = NULL;
		fastlock_release(&rs->slock);
	}

	ret = rdma_create_id(NULL, &id, NULL, dest->qp->cm_id->ps);
	if  (ret)
		return;

	memcpy(&saddr, rdma_get_local_addr(dest->qp->cm_id),
	       ucma_addrlen(rdma_get_local_addr(dest->qp->cm_id)));
	if (saddr.sa.sa_family == AF_INET)
		saddr.sin.sin_port = 0;
	else
		saddr.sin6.sin6_port = 0;
	ret = rdma_resolve_addr(id, &saddr.sa, &dest->addr.sa, 2000);
	if (ret)
		goto out;

	ret = rdma_resolve_route(id, 2000);
	if (ret)
		goto out;

	memset(&attr, 0, sizeof attr);
	if (id->route.path_rec->hop_limit > 1) {
		attr.is_global = 1;
		attr.grh.dgid = id->route.path_rec->dgid;
		attr.grh.flow_label = ntohl(id->route.path_rec->flow_label);
		attr.grh.sgid_index = udp_svc_sgid_index(dest, &id->route.path_rec->sgid);
		attr.grh.hop_limit = id->route.path_rec->hop_limit;
		attr.grh.traffic_class = id->route.path_rec->traffic_class;
	}
	attr.dlid = ntohs(id->route.path_rec->dlid);
	attr.sl = id->route.path_rec->sl;
	attr.src_path_bits = id->route.path_rec->slid & udp_svc_path_bits(dest);
	attr.static_rate = id->route.path_rec->rate;
	attr.port_num  = id->port_num;

	fastlock_acquire(&rs->slock);
	dest->qpn = qpn;
	dest->ah = ibv_create_ah(dest->qp->cm_id->pd, &attr);
	fastlock_release(&rs->slock);
out:
	rdma_destroy_id(id);
}

static int udp_svc_valid_udp_hdr(struct ds_udp_header *udp_hdr,
				 union socket_addr *addr)
{
	return (udp_hdr->tag == ntohl(DS_UDP_TAG)) &&
		((udp_hdr->version == 4 && addr->sa.sa_family == AF_INET &&
		  udp_hdr->length == DS_UDP_IPV4_HDR_LEN) ||
		 (udp_hdr->version == 6 && addr->sa.sa_family == AF_INET6 &&
		  udp_hdr->length == DS_UDP_IPV6_HDR_LEN));
}

static void udp_svc_forward(struct rsocket *rs, void *buf, size_t len,
			    union socket_addr *src)
{
	struct ds_header hdr;
	struct ds_smsg *msg;
	struct ibv_sge sge;
	uint64_t offset;

#ifdef VERBOSE
	fprintf(stderr, "udp_svc_forward forwarding udp\n");
#endif

	if (!ds_can_send(rs)) {
		if (ds_get_comp(rs, 0, ds_can_send))
			return;
	}

	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		msg = rs->fmsg_free;
		rs->fmsg_free = msg->next;
	} else {
		msg = rs->smsg_free;
		rs->smsg_free = msg->next;
	}

	
	ds_format_hdr(&hdr, src);

	memcpy((void *) msg, &hdr, hdr.length);
	memcpy((void *) msg + hdr.length, buf, len);
	rs->sqe_avail--;

	sge.addr = (uintptr_t) msg;
	sge.length = hdr.length + len;
	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		sge.lkey = rs->conn_dest->qp->fmr->lkey;
	} else {
		sge.lkey = rs->conn_dest->qp->smr->lkey;
	}
	
	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		offset = (uint8_t *) msg - rs->fbuf;
		ds_post_forward(rs, &sge, offset);
	} else {
		offset = (uint8_t *) msg - rs->sbuf;
		ds_post_send(rs, &sge, 1, offset);
	}
}

static void udp_svc_process_rs(struct rsocket *rs)
{
	static uint8_t buf[RS_SNDLOWAT];
	struct ds_dest *dest, *cur_dest;
	struct ds_udp_header *udp_hdr;
	union socket_addr addr;
	socklen_t addrlen = sizeof addr;
	int len, ret;

#ifdef VERBOSE
	fprintf(stderr, "receiving from udp socket %d ", rs->udp_sock);
#endif
	
	ret = recvfrom(rs->udp_sock, buf, sizeof buf, 0, &addr.sa, &addrlen);
	if (ret < DS_UDP_IPV4_HDR_LEN)
		return;

#ifdef VERBOSE
	fprintf(stderr, "from %d.%d.%d.%d:%d\n",
			(int)((struct sockaddr_in*)(&addr.sa))->sin_addr.s_addr&0xFF,
			((int)((struct sockaddr_in*)(&addr.sa))->sin_addr.s_addr&0xFF00)>>8,
			((int)((struct sockaddr_in*)(&addr.sa))->sin_addr.s_addr&0xFF0000)>>16,
			((int)((struct sockaddr_in*)(&addr.sa))->sin_addr.s_addr&0xFF000000)>>24,
			(int)(ntohs(((struct sockaddr_in*)(&addr.sa))->sin_port)));
#endif

	udp_hdr = (struct ds_udp_header *) buf;
	if (!udp_svc_valid_udp_hdr(udp_hdr, &addr))
		return;

	len = ret - udp_hdr->length;
	udp_hdr->tag = ntohl(udp_hdr->tag);
	udp_hdr->qpn = ntohl(udp_hdr->qpn) & 0xFFFFFF;
	ret = ds_get_dest(rs, &addr.sa, addrlen, &dest);
	if (ret)
		return;
	
	if (udp_hdr->op == RS_OP_DATA) {
#ifdef VERBOSE
		fprintf(stderr, "udp_svc_process_rs rs: %d RS_OP_DATA\n", rs->index);
#endif
		fastlock_acquire(&rs->slock);
		cur_dest = rs->conn_dest;
		rs->conn_dest = dest;
		
		// we got data through udp, so send back the necessary data for creating ah
		ds_send_udp(rs, NULL, 0, 0, RS_OP_CTRL);
		
		rs->conn_dest = cur_dest;
		fastlock_release(&rs->slock);
	}
#ifdef VERBOSE
	else {
		fprintf(stderr, "udp_svc_process_rs rs: %d RS_OP_CTRL\n", rs->index);
		struct sockaddr_in *addr = (struct sockaddr_in*)&dest->addr.sa;

		fprintf(stderr, "sending to %d.%d.%d.%d:%d\n",
				(int)(addr->sin_addr.s_addr&0xFF),
				(int)((addr->sin_addr.s_addr&0xFF00)>>8),
				(int)((addr->sin_addr.s_addr&0xFF0000)>>16),
				(int)((addr->sin_addr.s_addr&0xFF000000)>>24),
				(int)(ntohs(addr->sin_port)));
		
		addr = (struct sockaddr_in*)&dest->qp->dest.addr.sa;
		fprintf(stderr, "from %d.%d.%d.%d:%d\n",
				(int)(addr->sin_addr.s_addr&0xFF),
				(int)((addr->sin_addr.s_addr&0xFF00)>>8),
				(int)((addr->sin_addr.s_addr&0xFF0000)>>16),
				(int)((addr->sin_addr.s_addr&0xFF000000)>>24),
				(int)(ntohs(addr->sin_port)));

	}
#endif

	if (!dest->ah || (dest->qpn != udp_hdr->qpn))
		udp_svc_create_ah(rs, dest, udp_hdr->qpn);

	/* to do: handle when dest local ip address doesn't match udp ip */
	if (udp_hdr->op == RS_OP_DATA && len != 0) {
		fastlock_acquire(&rs->slock);
		cur_dest = rs->conn_dest;
		rs->conn_dest = &dest->qp->dest;
		udp_svc_forward(rs, buf + udp_hdr->length, len, &addr);
		rs->conn_dest = cur_dest;
		fastlock_release(&rs->slock);
	}
}

static void *udp_svc_run(void *arg)
{
	struct rs_svc *svc = arg;
	struct rs_svc_msg msg;
	int i, ret;

	ret = rs_svc_grow_sets(svc, 80);
	
	if (ret) {
		msg.status = ret;
		write(svc->sock[1], &msg, sizeof msg);
		return (void *) (uintptr_t) ret;
	}

	udp_svc_fds = svc->contexts;
	udp_svc_fds[0].fd = svc->sock[1];
	udp_svc_fds[0].events = POLLIN;
	do {
		for (i = 0; i <= svc->cnt; i++)
			udp_svc_fds[i].revents = 0;

		poll(udp_svc_fds, svc->cnt + 1, -1);
		if (udp_svc_fds[0].revents)
			udp_svc_process_sock(svc);

		for (i = 1; i <= svc->cnt; i++) {
			if (udp_svc_fds[i].revents)
				udp_svc_process_rs(svc->rss[i]); 
		}
	} while (svc->cnt >= 1);

	return NULL;
}

// GPUnet-related

int get_bufs_gpu(int socket, void** dev_sbuf,
				 int *sbuf_size, int *rbuf_size,
				 uintptr_t *dev_sbuf_free_queue_back,
				 uintptr_t *dev_sbuf_free_queue,
				 uintptr_t *host_sbuf_req_queue,
				 uintptr_t *dev_sbuf_req_back,
				 uintptr_t *host_sbuf_req_back,
				 uintptr_t *dev_rbuf_recved_queue,
				 uintptr_t *dev_rbuf_recved_back,
				 uintptr_t *host_rbuf_ack_queue_back) {
    struct rsocket *rs;
    int ret = 0;

    rs = idm_at(&idm, socket);
    
    fastlock_acquire(&rs->rlock);

#define set_output(_name_) *_name_ = (uintptr_t)rs->devptr_##_name_;
    if (rs->opts & RS_OPT_RECVBUF_GPU) {
		// rbuf is not rs one. it is qp-related.
		// however, dmsg is the rs resource.
		*rbuf_size = rs->rbuf_size;
		set_output(dev_rbuf_recved_queue);		
		set_output(dev_rbuf_recved_back);
		set_output(host_rbuf_ack_queue_back);
    } else {
        fprintf(stderr, "ERROR: get_rbuf_gpu: recv buffer not on GPU\n");
        exit(1);
    }

    fastlock_release(&rs->rlock);

    fastlock_acquire(&rs->slock);
    if (rs->opts & RS_OPT_SENDBUF_GPU) {
        *dev_sbuf = (void*)rs->sbuf;

		*sbuf_size = rs->sbuf_size;
		set_output(dev_sbuf_free_queue);		
		set_output(dev_sbuf_free_queue_back);
		set_output(host_sbuf_req_queue);
		set_output(dev_sbuf_req_back);
		set_output(host_sbuf_req_back);
    } else {
        fprintf(stderr, "ERROR: get_bufs_gpu: send buffer not on GPU\n");
        exit(1);
    }

    fastlock_release(&rs->slock);
    
    return ret;
}

int poll_sendbuf(struct rsocket *rs) {
	int ret = 0;

	int sbuf_req_back = *rs->hostptr_host_sbuf_req_back;

	// if empty
	if ((sbuf_req_back == (rs->sbuf_req_front + 1)) ||
		(rs->sbuf_req_front == sbuf_req_back + rs->sq_size)) {

		if (!(rs->state & rs_error)) {
			// TODO: handle error better
			fastlock_acquire(&rs->slock);
			ds_process_cqs(rs, 1, rs_poll_all);
			fastlock_release(&rs->slock);
		} else {
			ret = rs->err;
		}
		return ret;
	}
		
	if (rs->opts & RS_OPT_SENDBUF_GPU) {
		// consume a send request
		int i_consume = rs->sbuf_req_front + 1;
		if (i_consume == rs->sq_size + 1)
			i_consume = 0;

		// TODO: check if use of hostptr is right
		struct ds_smsg_info *msg = &rs->hostptr_host_sbuf_req_queue[i_consume];

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
		struct timeval tv1, tv2, tv_diff;
		gettimeofday(&tv1, NULL);
#endif

		fastlock_acquire(&rs->slock);
		ret = __dsendto_gpu(rs, rs->sbuf + RS_SNDLOWAT * msg->msg_index, msg->length,
							(struct sockaddr*)msg->addr, msg->addrlen);
		fastlock_release(&rs->slock);

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
		gettimeofday(&tv2, NULL);
		timersub(&tv2, &tv1, &tv_diff);
		
		rs->dsend_gpu_total += (double)(tv_diff.tv_sec * 1000.0 + tv_diff.tv_usec / 1000.0);
		if (++rs->dsend_gpu_count == 1000) {
			fprintf(stderr, "soc: %d avg dsend time: %.2f us, ah: %p\n", rs->index, rs->dsend_gpu_total*1000.0/rs->dsend_gpu_count, rs->conn_dest->ah);
			rs->dsend_gpu_count = 0;
			rs->dsend_gpu_total = 0.0;
		}
#endif
		if (ret > 0) {
			// happens after the send request (not necessary but just to make sure)
			rs->sbuf_req_front = i_consume;
		} else if (ret < 0 && errno != EWOULDBLOCK) {
			fprintf(stderr, "__dsend_gpu returns ret: %d\n", ret);
			exit(1);
		} else {
			// TODO: handle error better
			fastlock_acquire(&rs->slock);
			ds_process_cqs(rs, 1, rs_poll_all);
			fastlock_release(&rs->slock);
		}
	}
	
	return ret;
}

static int poll_recvbuf(struct rsocket *rs) {
	int ret = 0;

	if (!(rs->opts & RS_OPT_RECVBUF_GPU)) {
		return 0;
	}

	// handle completion as soon as possible
    if (!rs_have_rdata(rs)) {
        do {
            fastlock_acquire(&rs->slock);
            ret = ds_get_comp(rs, 1, rs_have_rdata);
            fastlock_release(&rs->slock);
        } while (ret != 0 && !rs_nonblocking(rs, 0) && (errno == EWOULDBLOCK));
		
		if (ret)
			return ret;
    }

	// handle rbuf_ack_queue from GPU to free the buffer

	// while ack queue is not empty
	int ack_queue_back = *rs->hostptr_host_rbuf_ack_queue_back;
	int ack_queue_front = rs->rbuf_ack_queue_front;
	
	while ((ack_queue_back != ack_queue_front + 1) &&
		   (ack_queue_front != ack_queue_back + rs->rq_size)) {
		struct ds_rmsg *rmsg;

		if (++rs->rbuf_ack_queue_front == rs->rq_size + 1)
			rs->rbuf_ack_queue_front = 0;

		rmsg = &rs->dmsg[rs->rmsg_head];
		// rmsg->qp is the reason we keep rmsg_head/rmsg_tail
		ds_post_recv(rs, rmsg->qp, rmsg->offset);
		if (++rs->rmsg_head == rs->rq_size + 1)
			rs->rmsg_head = 0;

		rs->rqe_avail++;
		
		ack_queue_back = *rs->hostptr_host_rbuf_ack_queue_back;
		ack_queue_front = rs->rbuf_ack_queue_front;
	}
	
    return ret;
}

int poll_cu_memcpy_reqs();

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
int sum_count = 0;
double poll_time_sum = 0.0;
double poll_time_max = 0.0;
#endif

int poll_backend_bufs() {
	struct list_head *pos, *tmp;

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
	struct timeval t1, t2, t3;
	gettimeofday(&t1, NULL);
#endif

	list_for_each_safe(pos, tmp, &alloc_socks) {
		struct rsocket *rs = container_of(pos, struct rsocket, alloc_list);
		if (!(rs->state & rs_error)) {
			if (rs->state & rs_writable) {
				poll_sendbuf(rs);
			}
			if (rs->state & rs_readable) {
				poll_recvbuf(rs);
			}
		}
	}

	poll_cu_memcpy_reqs();

#if defined(GPUNET_PROFILE) && defined(GPUNET_LOOP_PROFILE)
	gettimeofday(&t2, NULL);

	timersub(&t2, &t1, &t3);

	if (sum_count++ >= 10) {
		double __t = (t3.tv_sec * 1000.0 + t3.tv_usec / 1000.0);

		poll_time_sum += __t;
		if (poll_time_max < __t) {
			poll_time_max = __t;
		}
	}

	if (sum_count == 10000) {
		fprintf(stderr, "avg loop time: %f ms, max: %f ms, send cnt: %d, recv cnt: %d\n", poll_time_sum / (sum_count - 10), poll_time_max, send_count, recv_count);
		sum_count = 0;
		poll_time_sum = 0.0;
		poll_time_max = 0.0;
		send_count = 0;
		recv_count = 0;
	}
#endif

	return 0;
}

int ds_send_udp_handler(struct cu_memcpy_async_req* req) {
	int ret;
	struct rsocket *rs = req->rs;
	
	if (req->type == MEMCPY_TYPE_GPU_TO_CPU) {
		fastlock_acquire(&rs->slock);
		ret = ds_send_udp(rs, req->cpu_buf, req->len, 0, RS_OP_DATA);
		fastlock_release(&rs->slock);

		// free send buffer (just like send completion)
		int sbuf_index = ((uint8_t*)req->gpu_buf - rs->sbuf) / RS_SNDLOWAT;
		rs->hostptr_dev_sbuf_free_queue[rs->sbuf_free_queue_back] = sbuf_index;
		
		if (++rs->sbuf_free_queue_back == rs->sq_size + 1) {
			rs->sbuf_free_queue_back = 0;
		}
		*(rs->hostptr_dev_sbuf_free_queue_back) = rs->sbuf_free_queue_back;
	} else {
		assert(false);
	}
	return ret;
}

LIST_HEAD(cu_memcpy_reqs);

int register_cu_memcpy_req(struct rsocket* rs, int type, void* gpu_buf, void* cpu_buf, size_t len, cu_memcpy_req_comp handler) {
	struct cu_memcpy_async_req *req = (struct cu_memcpy_async_req*)calloc(1, sizeof(*req));
	if (!req) {
		perror("register_cu_memcpy_req calloc");
		exit(1);
	}

#ifdef VERBOSE_BOUNCE
	fprintf(stderr, "register_cu_memcpy_req gpu_buf: %lx len: %lx\n", (uintptr_t)gpu_buf, len);
#endif
	req->type = type;
	req->gpu_buf = gpu_buf;
	req->cpu_buf = cpu_buf;
	
	req->len = len;
	req->rs = rs;
	req->comp_handler = handler;

	if (rs->g2c_stream_refcount++ == 0) {
		req->stream = rs->g2c_stream = get_next_memstream();
	} else {
		req->stream = rs->g2c_stream;
	}
	list_add_tail(&req->reqs, &cu_memcpy_reqs);

	if (type == MEMCPY_TYPE_GPU_TO_CPU) {
		gpu_cpy_dev_to_host_async2(cpu_buf, gpu_buf, len, req->stream);
#ifdef GPUNET_PROFILE_BOUNCE_GPU_TO_CPU
		log_req(rs, len, gpu_buf, cpu_buf);
#endif
	} else if (type == MEMCPY_TYPE_CPU_TO_GPU) {
		gpu_cpy_host_to_dev_async2(gpu_buf, cpu_buf, len, req->stream);
#ifdef GPUNET_PROFILE_BOUNCE_GPU_TO_CPU
		log_req(rs, len, cpu_buf, gpu_buf);
#endif
	} else {
		assert(false);
	}
#ifdef VERBOSE_BOUNCE
	fprintf(stderr, "register_cu_memcpy_req gpu_buf: %lx len: %lx, req: %p done\n", (uintptr_t)gpu_buf, len, req);
#endif
	
	req->ev = event_record(req->stream);
	
	return 0;
}

int poll_cu_memcpy_reqs() {
	struct list_head *pos, *tmp;

	list_for_each_safe(pos, tmp, &cu_memcpy_reqs) {
		struct cu_memcpy_async_req *req = container_of(pos, struct cu_memcpy_async_req, reqs);
#ifdef VERBOSE_BOUNCE
		fprintf(stderr, "== poll_cu_memcpy_reqs req: %p\n", req);
#endif

		if (event_is_done(req->ev)) {
			req->comp_handler(req);
			event_free(req->ev);
			--req->rs->g2c_stream_refcount;
			list_del(&req->reqs);
			free(req);
		}

#ifdef VERBOSE_BOUNCE
		fprintf(stderr, "== poll_cu_memcpy_reqs req: %p finished\n\n", req);
#endif
	}
	return 0;
}
