#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <cuda.h>

#include <rdma_dgram/gpu.h>
#include <rdma_dgram/gpu_usermap_abi.h>


/* _ConvertSMVer2CoresDRV() from CUDA Toolkit 5
 * (samples/0_Simple/vectorAddDrv/vectorAddRv.cpp) */
inline int _ConvertSMVer2Cores(int major, int minor) {
    // Defines for GPU Architecture types (using the SM version to determine the # of cores per SM
    typedef struct {
        int SM; // 0xMm (hexidecimal notation), M = SM Major version, and m = SM minor version
        int Cores;
    } sSMtoCores;

    sSMtoCores nGpuArchCoresPerSM[] = {
        { 0x10,  8 }, // Tesla Generation (SM 1.0) G80 class
        { 0x11,  8 }, // Tesla Generation (SM 1.1) G8x class
        { 0x12,  8 }, // Tesla Generation (SM 1.2) G9x class
        { 0x13,  8 }, // Tesla Generation (SM 1.3) GT200 class
        { 0x20, 32 }, // Fermi Generation (SM 2.0) GF100 class
        { 0x21, 48 }, // Fermi Generation (SM 2.1) GF10x class
        { 0x30, 192}, // Kepler Generation (SM 3.0) GK10x class
        { 0x35, 192}, // Kepler Generation (SM 3.5) GK11x class
    };

    int index = 0;

    while (nGpuArchCoresPerSM[index].SM != -1) {
        if (nGpuArchCoresPerSM[index].SM == ((major << 4) + minor)) {
            return nGpuArchCoresPerSM[index].Cores;
        }

        index++;
    }

    // If we don't find the values, we default use the previous one to run properly
    printf("MapSMtoCores for SM %d.%d is undefined.  Default to use %d Cores/SM\n", major, minor, nGpuArchCoresPerSM[7].Cores);
    return nGpuArchCoresPerSM[7].Cores;
}

int gpu_device_count() {
	int device_count = 0;
	CudaSafeCall(cudaGetDeviceCount(&device_count));
	return device_count;
}

/* gpuGetMaxGflopsDeviceId() from gpu-fs */
int gpuGetMaxGflopsDeviceId()
{
    int current_device   = 0, sm_per_multiproc = 0;
    int max_compute_perf = 0, max_perf_device  = 0;
    int device_count     = 0, best_SM_arch     = 0;
    struct cudaDeviceProp deviceProp;

    device_count = gpu_device_count();

    // Find the best major SM Architecture GPU device
    while ( current_device < device_count ) {
        cudaGetDeviceProperties( &deviceProp, current_device );
        if (deviceProp.major > 0 && deviceProp.major < 9999) {
            best_SM_arch = MAX(best_SM_arch, deviceProp.major);
        }
        current_device++;
    }

    // Find the best CUDA capable GPU device
    current_device = 0;
    while( current_device < device_count ) {
        cudaGetDeviceProperties( &deviceProp, current_device );
        if (deviceProp.major == 9999 && deviceProp.minor == 9999) {
            sm_per_multiproc = 1;
        } else {
            sm_per_multiproc = _ConvertSMVer2Cores(deviceProp.major, deviceProp.minor);
        }

        int compute_perf  = deviceProp.multiProcessorCount * sm_per_multiproc * deviceProp.clockRate;
        if( compute_perf  > max_compute_perf ) {
            // If we find GPU with SM major > 2, search only these
            if ( best_SM_arch > 2 ) {
                // If our device==dest_SM_arch, choose this, or else pass
                if (deviceProp.major == best_SM_arch) {
                    max_compute_perf  = compute_perf;
                    max_perf_device   = current_device;
                }
            } else {
                max_compute_perf  = compute_perf;
                max_perf_device   = current_device;
            }
        }
        ++current_device;
    }
    return max_perf_device;
}

static bool init_cuda_device(int devid) {
    cudaError_t err;
    struct cudaDeviceProp prop;

    err = cudaSetDevice(devid);
    CudaSafeCall(err);

    err = cudaFree(0);
    CudaSafeCall(err);

    cudaGetDeviceProperties(&prop, 0);
    if (!prop.canMapHostMemory)
        return false;
    cudaSetDeviceFlags(cudaDeviceMapHost);

    return true;
}

/* if devices is null, init all devices */
void gpu_ctx_init(int* devices, int nr_dev) {
    /* eager initiailzation of cuda devices */
    int i;
    cudaError_t err;
    int lowest_avail_dev = nr_dev;

    if (devices == NULL) {
        err = cudaGetDeviceCount(&nr_dev);
        CudaSafeCall(err);

        lowest_avail_dev = nr_dev;

        for (i = 0; i < nr_dev; i++) {
            if (init_cuda_device(i) && lowest_avail_dev == nr_dev)
                lowest_avail_dev = i;
        }

        cudaSetDevice(lowest_avail_dev);
        return;
    }

    for (i = 0; i< nr_dev; i++) {
        if (init_cuda_device(devices[i]) && lowest_avail_dev == nr_dev)
            lowest_avail_dev = i;
    }

    cudaSetDevice(devices[lowest_avail_dev]);
}

struct cuda_dev* gpu_init(int dev_id) {
    cudaError_t err;
    //struct cudaDeviceProp deviceProp;

    struct cuda_dev *dev;
	struct cudaDeviceProp devProp;

    if (dev_id == BEST_GPU) {
        dev_id = gpuGetMaxGflopsDeviceId();
    }
    else if (dev_id != ANY_GPU) {
        cudaSetDevice(dev_id);
    }

    err = cudaGetDevice(&dev_id);
    CudaSafeCall(err);

	cudaGetDeviceProperties(&devProp, dev_id);
	fprintf(stderr, "Device Name: %s\n",  devProp.name);

	/* sk: on tesla03, weirdly, cudaGetDeviceProperties overflows the stack. I am not sure what is wrong here.
	  
    err = cudaGetDeviceProperties(&deviceProp, dev_id);
    CudaSafeCall(err);

    if (deviceProp.computeMode == cudaComputeModeProhibited) {
        fprintf(stderr, "Error: device is running in <Compute Mode Prohibited>, no threads can use ::cudaSetDevice().\n");
        exit(1);
    }

    printf("GPU Device %d: \"%s\" with compute capability %d.%d\n\n", dev_id, deviceProp.name, deviceProp.major, deviceProp.minor);
	*/

    dev = (struct cuda_dev*)malloc(sizeof(*dev));
    if (!dev) {
        fprintf(stderr, "gpu_init: malloc returned null\n");
        exit(1);
    }
    dev->cuda_dev_id = dev_id;

    return dev;
}

void* gpu_malloc(struct cuda_dev* dev, size_t len) {
    void* ptr;
    cudaError_t err;

    err = cudaMalloc(&ptr, len);
    CudaSafeCall(err);
#ifdef DEBUG
    fprintf(stderr, "gpu_malloc ptr: %p, err: %d len: %d\n", ptr, err, len);
#endif

    return ptr;
}

void gpu_free(void* devptr) {
    cudaError_t err;
    err = cudaFree(devptr);
    CudaSafeCall(err);
}

#define zero_len_check(__len) do { if ((__len) == 0) { fprintf(stderr, "ERROR: %s -- len is zero\n", __func__); exit(1); } } while(0)

void gpu_cpy_dev_to_host(void* dst, devptr src, size_t len) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaMemcpy(dst, src, len, cudaMemcpyDeviceToHost);
    CudaSafeCall(err);
}

void gpu_cpy_dev_to_host_async(void* dst, devptr src, size_t len, struct cuda_stream* stream) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaMemcpyAsync(dst, src, len, cudaMemcpyDeviceToHost, stream->stream);
    CudaSafeCall(err);
}

void gpu_cpy_dev_to_host_async2(void* dst, devptr src, size_t len, cudaStream_t stream) {
    cudaError_t err;
    zero_len_check(len);

    err = cudaMemcpyAsync(dst, src, len, cudaMemcpyDeviceToHost, stream);
    CudaSafeCall(err);
}

void gpu_cpy_host_to_dev(devptr dst, const void* src, size_t len) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaMemcpy(dst, src, len, cudaMemcpyHostToDevice);
    CudaSafeCall(err);
}

void gpu_cpy_host_to_dev_async(devptr dst, const void* src, size_t len, struct cuda_stream* stream) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaMemcpyAsync(dst, src, len, cudaMemcpyHostToDevice, stream->stream);
    CudaSafeCall(err);
}

void gpu_cpy_host_to_dev_async2(devptr dst, const void* src, size_t len, cudaStream_t stream) {
    cudaError_t err;
    zero_len_check(len);

    err = cudaMemcpyAsync(dst, src, len, cudaMemcpyHostToDevice, stream);
    CudaSafeCall(err);
}

void gpu_cpy_dev_to_dev(devptr dst, devptr src, size_t len) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaMemcpy(dst, src, len, cudaMemcpyDeviceToDevice);
    CudaSafeCall(err);
}

void gpu_shmem_alloc(struct cuda_dev* dev, size_t len, void** pptr, devptr* pdevptr) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaHostAlloc(pptr, len, cudaHostAllocMapped);
    CudaSafeCall(err);

    err = cudaHostGetDevicePointer(pdevptr, *pptr, 0);
    CudaSafeCall(err);
}

void gpu_shmem_free(void *ptr) {
    cudaError_t err;

    err = cudaFreeHost(ptr);
    CudaSafeCall(err);
}

void gpu_host_alloc(struct cuda_dev* dev, size_t len, void** pptr) {
    cudaError_t err;

    zero_len_check(len);

    err = cudaHostAlloc(pptr, len, cudaHostAllocDefault);
    CudaSafeCall(err);
}

void gpu_host_free(void* ptr) {
    cudaError_t err;

    err = cudaFreeHost(ptr);
    CudaSafeCall(err);
}

struct cuda_stream* stream_new() {
    struct cuda_stream* stream =
        (struct cuda_stream*)malloc(sizeof(struct cuda_stream));
    CudaSafeCall(cudaStreamCreate(&stream->stream));
    return stream;
}

void stream_free(struct cuda_stream* stream) {
    CudaSafeCall(cudaStreamDestroy(stream->stream));
    free(stream);
}

bool stream_is_done(struct cuda_stream *stream) {
    cudaError_t err;

    err = cudaStreamQuery(stream->stream);
    if ( err == cudaSuccess ) {
        return true;
    }

    if (err != cudaErrorNotReady && err != cudaSuccess) {
        CudaSafeCall(err);
    }

    return false;
}

void wait_for_stream(struct cuda_stream *stream) {
    CudaSafeCall(cudaStreamSynchronize(stream->stream));
}

cuda_event event_record(cudaStream_t stream) {
	cuda_event ev;
	CudaSafeCall(cudaEventCreate(&ev));
	CudaSafeCall(cudaEventRecord(ev, stream));
	return ev;
}

bool event_is_done(cuda_event ev) {
	return (cudaEventQuery(ev) == cudaSuccess);
}

void event_free(cuda_event ev) {
	CudaSafeCall(cudaEventDestroy(ev));
}

int gpu_get_device_id() {
    int ret = -1;
    CudaSafeCall(cudaGetDevice(&ret));
    return ret;
}

#define DEV_PATH "/dev/gpu_usermap"

#define GPU_BOUND_SHIFT 16
#define GPU_BOUND_SIZE (1UL << GPU_BOUND_SHIFT)
#define GPU_BOUND_OFFSET (GPU_BOUND_SIZE-1)
#define GPU_BOUND_MASK (~GPU_BOUND_OFFSET)

void* gpu_mmap(void* dev_ptr, size_t len) {
	int fd = open(DEV_PATH, O_RDWR);
	struct gpu_usermap_req req;
	int ret;
	CUDA_POINTER_ATTRIBUTE_P2P_TOKENS tokens;
	void* ptr;
	uint8_t *end = (uint8_t*)(((((uintptr_t)dev_ptr) + len) + GPU_BOUND_OFFSET) & GPU_BOUND_MASK);
	uint8_t *begin = (uint8_t*)((uintptr_t)dev_ptr & GPU_BOUND_MASK);

	if (fd < 0) {
		return MAP_FAILED;
	}

	CudaSafeCall(cuPointerGetAttribute(&tokens,
									   CU_POINTER_ATTRIBUTE_P2P_TOKENS,
									   (CUdeviceptr)dev_ptr));

	req.magic = GUSERMAP_MAGIC;
	req.gpu_addr = (uintptr_t)begin;
	req.len = (end - begin);
	req.p2p_token = tokens.p2pToken;
	req.va_space_token = tokens.vaSpaceToken;

	ret = write(fd, &req, sizeof(req));
	if (ret != sizeof(req)) {
		return MAP_FAILED;
	}

	ptr = mmap(NULL, (end - begin), PROT_WRITE | PROT_READ,
			   MAP_SHARED, fd, 0);
	if (ptr == (void*)MAP_FAILED) {
		return MAP_FAILED;
	}
	close(fd);

	return (void*)((char*)ptr + (((uintptr_t)dev_ptr) & GPU_BOUND_OFFSET));
}

int gpu_munmap(void *addr, size_t len) {
	return munmap(addr, len);
}

mem_stream_func g_stream_func;
int g_nr_streams;
int last_memstream = 0;

void install_memstream_provider(mem_stream_func func, int nr_streams) {
	g_stream_func = func;
	g_nr_streams = nr_streams;
}

cudaStream_t get_next_memstream() {
	cudaStream_t ret = g_stream_func(last_memstream);
	last_memstream = (last_memstream + 1) % g_nr_streams;
	return ret;
}



/* Local Variables:              */
/* mode: c                       */
/* c-basic-offset: 4             */
/* End:                          */
/* vim: set et:ts=4              */
