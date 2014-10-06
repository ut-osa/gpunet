#ifndef __GPUDIRECT_H__
#define __GPUDIRECT_H__

#include <stdlib.h>
#include <stdbool.h>
#include <cuda_runtime.h>

typedef void* devptr;

/* possible parameters to gpudirect_init */
#define ANY_GPU  (-1)
#define BEST_GPU (-2)

struct cuda_dev {
    int cuda_dev_id;
};

struct cuda_stream {
    cudaStream_t stream;
};

typedef cudaEvent_t cuda_event;


/* if devices is null, init all devices */
void gpu_ctx_init(int* devices, int nr_dev);

struct cuda_dev* gpu_init(int dev_id);

/*
  dev[in]       CUDA device
  len[in]       size of the buffer to be allocated

  physaddr[out] GPUDirect physical addr. NULL means no GPUDirect handling.
  devptr[out]   GPU device pointer
*/
void* gpu_malloc(struct cuda_dev* dev, size_t len);
void gpu_free(devptr devptr);

void gpu_cpy_dev_to_host(void* dst, devptr src, size_t len);
void gpu_cpy_dev_to_host_async(void* dst, devptr src, size_t len, struct cuda_stream* stream);
void gpu_cpy_dev_to_host_async2(void* dst, devptr src, size_t len, cudaStream_t stream);

void gpu_cpy_host_to_dev(devptr dst, const void* src, size_t len);
void gpu_cpy_host_to_dev_async(devptr dst, const void* src, size_t len, struct cuda_stream* stream);
void gpu_cpy_host_to_dev_async2(devptr dst, const void* src, size_t len, cudaStream_t stream);

void gpu_cpy_dev_to_dev(devptr dst, devptr src, size_t len);

#define CudaSafeCall( err ) __cudaSafeCall( err, __FILE__, __LINE__ )
#define CudaCheckError()    __cudaCheckError( __FILE__, __LINE__ )

static inline
void __cudaSafeCall(cudaError_t err, const char *file, const int line) {
    if ( cudaSuccess != err )
    {
        fprintf( stderr, "cudaSafeCall() failed at %s:%i : %s\n",
                 file, line, cudaGetErrorString( err ) );
        exit( -1 );
    }
    return;
}

static inline
void __cudaCheckError(const char *file, const int line) {
#ifdef CUDA_ERROR_CHECK
    cudaError err = cudaGetLastError();
    if ( cudaSuccess != err )
    {
        fprintf( stderr, "cudaCheckError() failed at %s:%i : %s\n",
                 file, line, cudaGetErrorString( err ) );
        exit( -1 );
    }

    // More careful checking. However, this will affect performance.
    // Comment away if needed.
    err = cudaDeviceSynchronize();
    if( cudaSuccess != err )
    {
        fprintf( stderr, "cudaCheckError() with sync failed at %s:%i : %s\n",
                 file, line, cudaGetErrorString( err ) );
        exit( -1 );
    }
#endif

    return;
}

void gpu_shmem_alloc(struct cuda_dev* dev, size_t len, void** pptr, devptr* pdevptr);
void gpu_shmem_free(void *ptr);

void gpu_host_alloc(struct cuda_dev* dev, size_t len, void** pptr);
void gpu_host_free(void* ptr);

struct cuda_stream* stream_new();
void stream_free(struct cuda_stream* stream);
bool stream_is_done(struct cuda_stream *stream);
void wait_for_stream(struct cuda_stream *stream);

cuda_event event_record(cudaStream_t stream);
bool event_is_done(cuda_event ev);
void event_free(cuda_event ev);

int gpu_get_device_id();

void* gpu_mmap(void* dev_ptr, size_t len);
int gpu_munmap(void *addr, size_t len);

int gpu_device_count();

typedef cudaStream_t (*mem_stream_func)(int);
void install_memstream_provider(mem_stream_func func, int nr_streams);
cudaStream_t get_next_memstream();
#endif

/* Local Variables:              */
/* mode: c                       */
/* c-basic-offset: 4             */
/* End:                          */
/* vim: set et:ts=4              */
