#ifndef __RINGBUF_H__
#define __RINGBUF_H__

#include <rdma/gpu.h>
#include <stdint.h>

typedef uint64_t rb_size_t;
typedef uint64_t rb_off_t;

struct ringbuf {
    uint8_t volatile* _buf;
    volatile rb_off_t _head;
    volatile rb_off_t _tail;
    rb_size_t _size;
};

typedef struct ringbuf *ringbuf_t;

ringbuf_t ringbuf_new(rb_size_t size);
ringbuf_t ringbuf_gpu_new(struct cuda_dev* dev, rb_size_t size, devptr *pdevptr);
ringbuf_t ringbuf_gpu_meta_new(struct cuda_dev* dev, rb_size_t size, devptr* pdevptr);

void ringbuf_reset(ringbuf_t rb);

__attribute__((used))
static
rb_size_t ringbuf_max_size(const struct ringbuf* rb) {
    return rb->_size;
}

__attribute__((used))
static rb_off_t ringbuf_head_offset(const struct ringbuf* rb)  {
    return (rb->_head % ringbuf_max_size(rb));
}

__attribute__((used))
static
void* ringbuf_head(const struct ringbuf* rb) {
    return (void*)(rb->_buf + ringbuf_head_offset(rb));
}

__attribute__((used))
static rb_off_t ringbuf_tail_offset(const struct ringbuf* rb) {
    return (rb->_tail % ringbuf_max_size(rb));
}

__attribute__((used))
static void* ringbuf_tail(const struct ringbuf* rb) {
    return (void*)(rb->_buf + ringbuf_tail_offset(rb));
}

__attribute__((used))
static void* ringbuf_addr(const struct ringbuf* rb) {
    return (void*)rb->_buf;
}

__attribute__((used))
static rb_off_t ringbuf_offset(const struct ringbuf* rb, void* ptr) {
    return (rb_off_t)(((uint8_t*)ptr) - (uint8_t*)ringbuf_addr(rb));
}

static
void* ringbuf_end(const struct ringbuf* rb) {
    return (((uint8_t*)ringbuf_addr(rb)) + ringbuf_max_size(rb));
}

rb_size_t ringbuf_bytes_free(ringbuf_t rb);
rb_size_t ringbuf_bytes_used(ringbuf_t rb);

void *ringbuf_memcpy_into(struct ringbuf* dst, const void *src, rb_size_t count, rb_size_t *nb_copied, int produce);
void* ringbuf_gpu_memcpy_into(struct ringbuf* dst, const void *src, rb_size_t count, rb_size_t *nb_copied);

void* ringbuf_gpu_memcpy_into_async(struct ringbuf* dst, const void *src, rb_size_t count, struct cuda_stream *stream);

void * ringbuf_memcpy_from(void *dst, struct ringbuf* src, rb_size_t count, rb_size_t *nb_copied);
void * ringbuf_gpu_memcpy_from(void *dst, struct ringbuf* src, rb_size_t count, rb_size_t* nb_copied);
void * ringbuf_gpu_memcpy_from_async(void *dst, struct ringbuf* src, rb_size_t count, struct cuda_stream* stream);

void ringbuf_free(ringbuf_t rb);
void ringbuf_gpu_free(ringbuf_t rb);
void ringbuf_gpu_meta_free(ringbuf_t rb);

void* ringbuf_produce(ringbuf_t rb, rb_size_t len);
void* ringbuf_consume(ringbuf_t rb, rb_size_t len);

static inline
bool is_addr_in_buf(ringbuf_t rb, uint8_t* addr) {
    return ((addr >= rb->_buf) && (addr < (uint8_t*)ringbuf_end(rb)));
}

#endif
