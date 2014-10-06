#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/param.h>

#include <rdma/gpu.h>
#include <rdma/ringbuf.h>
#include <rdma/log.h>

#ifdef NDEBUG
#define ringbuf_check(rb)
#else
void ringbuf_check(struct ringbuf* rb) {
    if (rb->_head < 0) {
        fprintf(stderr, "ringbuf_check head too small. head: %ld, size: %ld",
                rb->_head, rb->_size);
        assert(false);
    }

    if (rb->_tail < 0) {
        fprintf(stderr, "ringbuf_check tail too small. tail: %ld, size: %ld",
                rb->_tail, rb->_size);
        assert(false);
    }

    if (ringbuf_head_offset(rb) >= rb->_size) {
        fprintf(stderr, "ringbuf_check head too large. head: %ld, size: %ld",
                rb->_head, rb->_size);
        assert(false);
    }

    if (ringbuf_tail_offset(rb) >= rb->_size) {
        fprintf(stderr, "ringbuf_check tail too large. tail: %ld, size: %ld",
                rb->_tail, rb->_size);
        assert(false);
    }
}
#endif

ringbuf_t
ringbuf_new(rb_size_t size) {
    struct ringbuf* rb = (struct ringbuf*)calloc(1, sizeof(*rb));
    rb->_size = size;
    rb->_buf = (uint8_t*)calloc(1, size);
    ringbuf_reset(rb);
    return rb;
}

ringbuf_t
ringbuf_gpu_new(struct cuda_dev* dev, rb_size_t size, devptr* pdevptr) {
    devptr pdev_rb, devptr;
    ringbuf_t rb;

    gpu_shmem_alloc(dev, sizeof(struct ringbuf), (void**)&rb, &pdev_rb);

    if (rb) {
        rb->_size = size;
        devptr = gpu_malloc(dev, rb->_size);
        if (!devptr) {
            return NULL;
        }

        rb->_buf = devptr;
        ringbuf_reset(rb);
    } else {
        fprintf(stderr, "ringbuf_gpu_new error: %p\n", rb);
    }

    if (pdevptr)
        *pdevptr = pdev_rb;

    return rb;
}

ringbuf_t
ringbuf_gpu_meta_new(struct cuda_dev* dev, rb_size_t size, devptr* pdevptr) {
    devptr pdev_rb;
    ringbuf_t rb;

    gpu_shmem_alloc(dev, sizeof(struct ringbuf), (void**)&rb, &pdev_rb);

    if (rb) {
        rb->_size = size;
        rb->_buf = NULL;
        ringbuf_reset(rb);
    } else {
        fprintf(stderr, "ringbuf_gpu_meta_new error: %p\n", rb);
    }

    if (pdevptr)
        *pdevptr = pdev_rb;

    return rb;
}

// producer: sets _head. may see stale _tail.
// consumer: sets _tail. may see stale _head.

// both for producer and consumer.
// producer: stail _tail. may return false when actually empty.
// consumer: stail _head. may return true when actually not empty.
bool ringbuf_is_empty(struct ringbuf* rb) {
    return (rb->_head == rb->_tail);
}

// both for producer and consumer.
// producer: stale _tail. may return true when actually not full.
// consumer: stale _head. may return false when actually full.
bool ringbuf_is_full(struct ringbuf* rb) {
    return (rb->_head == (ringbuf_max_size(rb) + rb->_tail)) ;
}

void ringbuf_reset(ringbuf_t rb) {
    rb->_head = rb->_tail = 0;
}

void ringbuf_free(ringbuf_t rb) {
    assert(rb);
    free((void*)rb->_buf);
    free((void*)rb);
}

void ringbuf_gpu_free(ringbuf_t rb) {
    assert(rb);
    gpu_free((devptr)rb->_buf);
    gpu_shmem_free(rb);
}

void ringbuf_gpu_meta_free(ringbuf_t rb) {
    gpu_shmem_free(rb);
}


#define _ringbuf_capacity(rb) ((rb)->_size)

rb_size_t ringbuf_capacity(const struct ringbuf* rb) {
    return _ringbuf_capacity(rb);
}

/* do synchronization between consumer/producer, or allow stale value */

// producer: stale _tail. may see larger value than actual one.
// consumer: stale _head. may see smaller value than actual one.
rb_size_t ringbuf_bytes_used(ringbuf_t rb) {
    // use consistent head/tail for the whole function
    // stale values are ok, but use of inconsistenv values is not
	return (rb->_head - rb->_tail);
}

/* do synchronization between consumer/producer, or allow stale value */

// producer: stale _tail. may see smaller value than actual one.
// consumer: stale _head. may see larger value than actual one.
rb_size_t ringbuf_bytes_free(ringbuf_t rb) {
    return (_ringbuf_capacity(rb) - (rb->_head - rb->_tail));
}

/*
 * Given a ring buffer rb and a pointer to a location within its
 * contiguous buffer, return the a pointer to the next logical
 * location in the ring buffer.
 */
uint8_t *ringbuf_nextp(struct ringbuf* rb, const uint8_t *p) {
    /*
     * The assert guarantees the expression (++p - rb->buf) is
     * non-negative; therefore, the modulus operation is safe and
     * portable.
     */
    assert((p >= rb->_buf) && (p < (uint8_t*)ringbuf_end(rb)));
    return (uint8_t*)rb->_buf + ((++p - rb->_buf) % ringbuf_max_size(rb));
}

// producer-side
void* ringbuf_produce(ringbuf_t rb, rb_size_t len) {
    // ringbuf_bytes_free() returns smaller value than the actual one
    // hence more restrictive.
    if (len > ringbuf_bytes_free(rb)) {
        return NULL;
    }

    __sync_synchronize();
    rb->_head = rb->_head + len;
    __sync_synchronize();
    // change to head will be visible

    return (void*)ringbuf_head(rb);
}

// consumer-side
void* ringbuf_consume(ringbuf_t rb, rb_size_t len) {
    // ringbuf_bytes_used() returns smaller value than the actual one
    // hence more restrictive.
    if (len > ringbuf_bytes_used(rb)) {
        return NULL;
    }

    __sync_synchronize();
    rb->_tail = rb->_tail + len;
    __sync_synchronize();
    // change to tail will be visible

    return (void*)ringbuf_tail(rb);
}

// producer-side
// CPU "send" uses this for CPU mem
void *ringbuf_memcpy_into(struct ringbuf* dst, const void *src, rb_size_t count, rb_size_t *nb_copied, int produce) {

	uint8_t *dst_buf = (uint8_t*)ringbuf_addr(dst);
	rb_off_t off_head = ringbuf_head_offset(dst);
	uint8_t *u8dst = dst_buf + off_head;
	rb_size_t dst_size = ringbuf_max_size(dst);

    ringbuf_check(dst);

    count = MIN(ringbuf_bytes_free(dst), count);

    if (off_head + count > dst_size) {
        // memcpys over the end
        rb_size_t written = dst_size - off_head;
        memcpy(u8dst, src, written);
        u8dst = dst_buf;
        memcpy(u8dst, src + written, count - written);
    } else {
        // single memcpy
        memcpy(u8dst, src, count);
    }

    // update _head
    if (produce)
        ringbuf_produce(dst, count);
    
    ringbuf_check(dst);

    if (nb_copied)
        *nb_copied = count;

    return ringbuf_head(dst);
}

// producer-side
// CPU "send" uses this for GPU mem
void* ringbuf_gpu_memcpy_into(struct ringbuf* dst, const void *src, rb_size_t count, rb_size_t *nb_copied) {
	uint8_t *dst_buf = (uint8_t*)ringbuf_addr(dst);
	rb_off_t off_head = ringbuf_head_offset(dst);
	uint8_t *u8dst = dst_buf + off_head;
	rb_size_t dst_size = ringbuf_max_size(dst);

    ringbuf_check(dst);
    count = MIN(ringbuf_bytes_free(dst), count);

    log_debug("ringbuf_gpu_memcpy_into head: %d, count: %d, _size: %d",
              dst->_head, count, dst->_size);

    if (off_head + count > dst_size) {
        // memcpys over the end
        rb_size_t written = dst_size - off_head;
        gpu_cpy_host_to_dev((devptr)u8dst, src, written);
        u8dst = dst_buf;
        gpu_cpy_host_to_dev((devptr)u8dst, src + written, count - written);
    } else {
        // single memcpy
        log_debug("before single memcpy %p", u8dst);
        gpu_cpy_host_to_dev((devptr)u8dst, src, count);
        log_debug("single memcpy done");
    }

    // update _head
    ringbuf_produce(dst, count);
    log_debug("ringbuf buffer produced");

    ringbuf_check(dst);

    if (nb_copied)
        *nb_copied = count;

    return ringbuf_head(dst);
}

void* ringbuf_gpu_memcpy_into_async(struct ringbuf* dst, const void *src, rb_size_t count, struct cuda_stream *stream) {
	uint8_t *dst_buf = (uint8_t*)ringbuf_addr(dst);
	rb_off_t off_head = ringbuf_head_offset(dst);
	uint8_t *u8dst = dst_buf + off_head;
	rb_size_t dst_size = ringbuf_max_size(dst);

    ringbuf_check(dst);
    count = MIN(ringbuf_bytes_free(dst), count);

    if (off_head + count > dst_size) {
        // memcpys over the end
        rb_size_t written = dst->_size - ringbuf_head_offset(dst);
        gpu_cpy_host_to_dev_async(u8dst, src, written, stream);
        u8dst = dst_buf;
        gpu_cpy_host_to_dev_async(u8dst, src + written, count - written, stream);
    } else {
        // single memcpy
        log_debug("before single memcpy %p, head: %x", u8dst, dst->_head);
        gpu_cpy_host_to_dev_async(u8dst, src, count, stream);
        log_debug("single memcpy done");
    }

    return ringbuf_head(dst);
}

// consumer-side
// CPU "recv" uses this for CPU mem
void * ringbuf_memcpy_from(void *dst, struct ringbuf* src, rb_size_t count, rb_size_t* nb_copied) {
    uint8_t *u8dst = (uint8_t*)dst;
	rb_off_t src_tail_offset = ringbuf_tail_offset(src);
	rb_size_t src_size = ringbuf_max_size(src);
	uint8_t *u8src = ringbuf_addr(src);

    ringbuf_check(src);

    count = MIN(ringbuf_bytes_used(src), count);

    if (src_tail_offset + count > src_size) {
        // memcpys over the end
        rb_size_t written = src_size - src_tail_offset;
        memcpy(u8dst, u8src + src_tail_offset, written);
        memcpy(u8dst + written, u8src, count - written);
    } else {
        memcpy(u8dst, u8src + src_tail_offset, count);
    }

    // update _tail
    ringbuf_consume(src, count);

    ringbuf_check(src);

    if (nb_copied)
        *nb_copied = count;

    return dst;
}

// consumer-side
// CPU "recv" uses this for GPU mem
void * ringbuf_gpu_memcpy_from(void *dst, struct ringbuf* src, rb_size_t count, rb_size_t* nb_copied) {
    uint8_t *u8dst = (uint8_t*)dst;
	rb_off_t src_tail_offset = ringbuf_tail_offset(src);
	rb_size_t src_size = ringbuf_max_size(src);
	uint8_t *u8src = ringbuf_addr(src);

    ringbuf_check(src);

    count = MIN(ringbuf_bytes_used(src), count);

    if (src_tail_offset + count > src_size) {
        // memcpys over the end
        rb_size_t written = src_size - src_tail_offset;
        gpu_cpy_dev_to_host(u8dst, (devptr)(u8src + src_tail_offset), written);
        gpu_cpy_dev_to_host(u8dst + written, (devptr)u8src, count - written);
    } else {
        gpu_cpy_dev_to_host(u8dst, (devptr)(u8src + src_tail_offset), count);
    }

    // update _tail
    ringbuf_consume(src, count);

    ringbuf_check(src);

    if (nb_copied)
        *nb_copied = count;

    return dst;
}

void * ringbuf_gpu_memcpy_from_async(void *dst, struct ringbuf* src, rb_size_t count, struct cuda_stream *stream) {
    uint8_t *u8dst = (uint8_t*)dst;
	rb_off_t src_tail_offset = ringbuf_tail_offset(src);
	rb_size_t src_size = ringbuf_max_size(src);
	uint8_t *u8src = ringbuf_addr(src);

    ringbuf_check(src);

    count = MIN(ringbuf_bytes_used(src), count);

    if (src_tail_offset + count > src_size) {
        // memcpys over the end
        rb_size_t written = src_size - src_tail_offset;
        gpu_cpy_dev_to_host_async(u8dst, (devptr)(u8src + src_tail_offset),
								  written, stream);
        gpu_cpy_dev_to_host_async(u8dst + written, (devptr)u8src,
								  count - written, stream);
    } else {
        gpu_cpy_dev_to_host_async(u8dst, (devptr)(u8src + src_tail_offset),
								  count, stream);
    }

    return dst;
}

