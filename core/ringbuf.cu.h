#ifndef __RINGBUF_CU_H__
#define __RINGBUF_CU_H__

#include "util.cu.h"
#include <stdint.h>
#include <stdio.h>

typedef uint64_t rb_size_t;
typedef uint64_t rb_off_t;

#define MAX_RB_OFF_T (0xffffffffffffffffUL)

inline __device__ rb_size_t min(rb_size_t v1, rb_size_t v2) {
	return (v1 <= v2) ? v1 : v2;
}

struct g_ringbuf {

	struct ringbuf {
		uint8_t volatile* _buf;
		volatile rb_off_t _head;
		volatile rb_off_t _tail;
		volatile rb_size_t _size;
	};

	typedef struct ringbuf *ringbuf_t;

#define __ringbuf_max_size(rb_size) (rb_size)

	static __forceinline__ __device__
	rb_size_t ringbuf_max_size(const struct ringbuf* rb) {
		return rb->_size;
	}

	static __forceinline__ __device__
	rb_off_t __ringbuf_head_offset(rb_off_t head, rb_size_t size) {
		return (head % size);
	}

	static __forceinline__ __device__
	rb_off_t ringbuf_head_offset(const struct ringbuf* rb) {
		return (rb->_head % ringbuf_max_size(rb));
	}

	static __device__
	void* ringbuf_head(const struct ringbuf* rb) {
		return (void*)(rb->_buf + ringbuf_head_offset(rb));
	}

	static 	__device__
	void* __ringbuf_head(uint8_t *buf, rb_off_t head, rb_size_t size) {
		return (void*)(buf + __ringbuf_head_offset(head, size));
	}

	static __forceinline__ __device__
	rb_off_t ringbuf_tail_offset(const struct ringbuf* rb) {
		return (rb->_tail % ringbuf_max_size(rb));
	}

	static __forceinline__ __device__
	rb_off_t __ringbuf_tail_offset(rb_off_t tail, rb_size_t size) {
		return (tail % size);
	}

	static __device__
	void* ringbuf_tail(const struct ringbuf* rb) {
		return (void*)(rb->_buf + ringbuf_tail_offset(rb));
	}

	static 	__device__
	void* __ringbuf_tail(uint8_t *buf, rb_off_t tail, rb_size_t size) {
		return (void*)(buf + __ringbuf_tail_offset(tail, size));
	}

	static __forceinline__ __device__
	void* ringbuf_addr(const struct ringbuf* rb) {
		return (void*)rb->_buf;
	}

	static rb_off_t __device__ ringbuf_offset(ringbuf_t rb, void* ptr) {
		return (((uint8_t*)ptr) - (uint8_t*)rb->_buf);
	}

	static  volatile __device__
	void* ringbuf_end(const struct ringbuf* rb) {
		return (volatile void*)(rb->_buf + ringbuf_max_size(rb));
	}


	static __forceinline__ __device__
	bool ringbuf_is_empty(const struct ringbuf* rb) {
		return (rb->_head == rb->_tail);
	}

	// both for producer and consumer.
	// producer: stale _tail. may return true when actually not full.
	// consumer: stale _head. may return false when actually full.
	static __forceinline__ __device__
	bool ringbuf_is_full(const struct ringbuf* rb) {
		// handles overflow gracefully
		return (rb->_head == (rb->_tail + ringbuf_max_size(rb))) ;
	}

	static __forceinline__ __device__
	void ringbuf_reset(ringbuf_t rb) {
		rb->_head = rb->_tail = 0U;
	}

#define _ringbuf_capacity(rb) ((rb)->_size)
#define __ringbuf_capacity(rb_size) (rb_size)

	static __forceinline__ __device__
	rb_size_t ringbuf_capacity(const struct ringbuf* rb) {
		return _ringbuf_capacity(rb);
	}

	/* do synchronization between consumer/producer, or allow stale value */

	// producer: stale _tail. may see larger value than actual one.
	// consumer: stale _head. may see smaller value than actual one.
	static __forceinline__ __device__
	rb_size_t ringbuf_bytes_used(ringbuf_t rb) {
		// this handles overflow well
		return (rb->_head - rb->_tail);
	}

	static __forceinline__ __device__
	rb_size_t __ringbuf_bytes_used(rb_off_t rb_head, rb_off_t rb_tail) {
		// this handles overflow well..
		return (rb_head - rb_tail);
	}

	/* do synchronization between consumer/producer, or allow stale value */

	// producer: stale _tail. may see smaller value than actual one.
	// consumer: stale _head. may see larger value than actual one.
	static __forceinline__ __device__
	rb_size_t ringbuf_bytes_free(ringbuf_t rb) {
		return (_ringbuf_capacity(rb) - ringbuf_bytes_used(rb));
	}

	static __forceinline__ __device__
	rb_size_t __ringbuf_bytes_free(rb_off_t rb_head, rb_off_t rb_tail, rb_off_t rb_size) {
		return (__ringbuf_capacity(rb_size) - __ringbuf_bytes_used(rb_head, rb_tail));
	}

	/*
	 * Given a ring buffer rb and a pointer to a location within its
	 * contiguous buffer, return the a pointer to the next logical
	 * location in the ring buffer.
	 */
	static __forceinline__ __device__ volatile
	uint8_t *ringbuf_nextp(struct ringbuf* rb, const uint8_t *p) {
		/*
		 * The assert guarantees the expression (++p - rb->buf) is
		 * non-negative; therefore, the modulus operation is safe and
		 * portable.
		 */
		//assert((p >= rb->_buf) && (p < (uint8_t*)ringbuf_end(rb)));
		return (volatile uint8_t*)(rb->_buf + ((++p - rb->_buf) % ringbuf_max_size(rb)));
	}

	// producer-side
	static __forceinline__ __device__ volatile
	void* ringbuf_produce(ringbuf_t rb, rb_size_t len) {
		// ringbuf_bytes_free() returns smaller value than the actual one
		// hence more restrictive.
		if (len > ringbuf_bytes_free(rb)) {
			return NULL;
		}

		__threadfence();
		rb->_head = rb->_head + len;
		__threadfence();
		// change to head will be visible

		return (volatile void*)ringbuf_head(rb);
	}

	static __forceinline__ __device__
	void __ringbuf_produce(ringbuf_t rb, rb_off_t rb_head, rb_off_t rb_tail, rb_size_t rb_size, rb_size_t len) {
		if (len > __ringbuf_bytes_free(rb_head, rb_tail, rb_size)) {
			return;
		}

		__threadfence();
		rb->_head = rb_head + len;
		__threadfence();
	}

	// consumer-side
	static __forceinline__ __device__ volatile
	void* ringbuf_consume(ringbuf_t rb, rb_size_t len) {
		// ringbuf_bytes_used() returns smaller value than the actual one
		// hence more restrictive.
		if (len > ringbuf_bytes_used(rb)) {
			return NULL;
		}

		__threadfence();
		rb->_tail = rb->_tail + len;
		__threadfence();
		// change to tail will be visible

		return (volatile void*)ringbuf_tail(rb);
	}

	static __forceinline__ __device__
	void __ringbuf_consume(ringbuf_t rb, rb_off_t rb_head, rb_off_t rb_tail, rb_size_t len) {
		if (len > __ringbuf_bytes_used(rb_head, rb_tail)) {
			return ;
		}

		__threadfence();
		rb->_tail = rb_tail + len;
		__threadfence();
	}

	static __forceinline__ __device__
	void memcpy_thread(volatile char* dst, volatile char*src, rb_size_t size) {
		for(int i=0;i<size>>2;i++){
			((volatile int*)dst)[i]=((volatile int*)(src))[i];
		}
	}

	template <int THREADS>
	static __forceinline__ __device__
	void memcpy_warp(volatile char* dst, volatile char*src, rb_size_t size) {
		int len = (int)(size >> 2);
		for (int i = threadIdx.x; i < len; i += THREADS) {
			((volatile int*)dst)[i]=((volatile int*)(src))[i];
		}
	}

	static __forceinline__ __device__
	void ringbuf_memcpy_into(struct ringbuf* dst, const void *src,
							 rb_size_t count, rb_size_t *nb_copied, int produce) {
		//    ringbuf_check(dst);
		__shared__ rb_size_t nb_copy, dst_size;
		__shared__ uint8_t *u8dst;
		__shared__ rb_off_t dst_head, dst_tail;

		if (FIRST_THREAD_IN_BLOCK()) {
			dst_head = dst->_head;
			dst_tail = dst->_tail;
			dst_size = ringbuf_max_size(dst);

			nb_copy = min(__ringbuf_bytes_free(dst_head, dst_tail, dst_size), count);
			
			u8dst = (uint8_t*)__ringbuf_head((uint8_t*)ringbuf_addr(dst), dst_head, dst_size);
		}

		__syncthreads();

		if (nb_copy == 0) return;

		if (__ringbuf_head_offset(dst_head, dst_size) + nb_copy > dst_size) {
			// memcpys over the end
			rb_size_t nr_write = dst_size - __ringbuf_head_offset(dst_head, dst_size);
			
			copy_block_dst_volatile((volatile uchar*)u8dst, (uchar*)src, nr_write);
			u8dst = (uint8_t*)ringbuf_addr(dst);
			copy_block_dst_volatile((volatile uchar*)u8dst, (uchar*)(src) + nr_write, nb_copy - nr_write);
		} else {
			// single memcpy
			copy_block_dst_volatile((volatile uchar*)u8dst, (uchar*) src, nb_copy);
		}

		if (FIRST_THREAD_IN_BLOCK()) {
			// update _head
			if (produce) {
				__ringbuf_produce(dst, dst_head, dst_tail, dst_size, nb_copy);
			}
			//    ringbuf_check(dst);

			*nb_copied = nb_copy;
		}
		__syncthreads();
	}

	// consumer-side
	// CPU "recv" uses this for CPU mem
	__forceinline__
	static  __device__
	void ringbuf_memcpy_from(void *dst, struct ringbuf* src,
							 rb_size_t count, rb_size_t* nb_copied) {

		uint8_t *u8dst = (uint8_t*)dst;
		__shared__ rb_size_t nb_copy, src_size;
		__shared__ uint8_t *src_buf;
		__shared__ rb_off_t src_head, src_tail, src_tail_offset;
		
		//ringbuf_check(src);
		if (FIRST_THREAD_IN_BLOCK()) {
			src_head = src->_head;
			src_tail = src->_tail;
			src_size = ringbuf_max_size(src);
			src_tail_offset = __ringbuf_tail_offset(src_tail, src_size);
			src_buf = (uint8_t*)ringbuf_addr(src);
			
			nb_copy = min(__ringbuf_bytes_used(src_head, src_tail), count);

			/* printf("ringbuf_memcpy_from src_head: %ld, src_tail: %ld, nb_copy: %d\n", */
			/*        src_head, src_tail, nb_copy); */

			*nb_copied = nb_copy;
		}
		__syncthreads();

		if (nb_copy == 0) return;

		if (src_tail_offset + nb_copy > src_size) {
			// memcpys over the end
			rb_size_t written = src_size - src_tail_offset;
			copy_block_src_volatile((uchar*)u8dst, (volatile uchar*)src_buf + src_tail_offset, written);
			copy_block_src_volatile((uchar*)(u8dst + written), (volatile uchar*)src_buf, nb_copy - written);
		} else {
			copy_block_src_volatile((uchar*)u8dst, (volatile uchar*)src_buf + src_tail_offset, nb_copy);
		}

		if (FIRST_THREAD_IN_BLOCK()) {
			// update _tail
			__ringbuf_consume(src, src_head, src_tail, nb_copy);

			/* printf("used:: %d, tail: %ld, nb_copy: %d \n", __ringbuf_bytes_used(src_head, src_tail, src_size), src->_tail, nb_copy); */

			//   ringbuf_check(src);
			//*nb_copied = nb_copy;  // already done
		}
	}

	__forceinline__
	static  __device__
	void ringbuf_memcpy_from_zerocopy(void *dst, struct ringbuf* src,
									  rb_size_t count, rb_size_t* nb_copied) {

		__shared__ rb_size_t nb_copy;
		__shared__ rb_off_t src_head, src_tail;

		//ringbuf_check(src);
		if (FIRST_THREAD_IN_BLOCK()) {
			src_head = src->_head;
			src_tail = src->_tail;
			nb_copy = min((int)__ringbuf_bytes_used(src_head, src_tail), (int)count);

			/* if (nb_copy > 0) */
			/*     printf("ringbuf_memcpy_from_zerocopy src_head: %ld, src_tail: %ld, nb_copy: %d\n", */
			/*            src_head, src_tail, nb_copy); */

			*nb_copied = nb_copy;
		}

		if (nb_copy == 0) return;

		// update _tail
		__ringbuf_consume(src, src_head, src_tail, nb_copy);

		/* if (FIRST_THREAD_IN_BLOCK()) */
		/*     printf("used: %d, tail: %ld, nb_copy: %d \n", */
		/*            __ringbuf_bytes_used(src_head, src_tail, src_size), */
		/*            src->_tail, nb_copy); */

		//   ringbuf_check(src);
		//*nb_copied = nb_copy;  // already done
	}

}; // end of struct g_ringbuf

#endif