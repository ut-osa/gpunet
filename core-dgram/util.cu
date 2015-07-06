#include "util.cu.h"

template<typename T>
__device__ void inline aligned_copy(uchar* dst, volatile uchar* src, int newsize, int tid)
{
	while(tid<newsize){
		((T*)dst)[tid]=*(((T*)src)+tid);
		tid+=blockDim.x*blockDim.y*blockDim.z;
	}
}

__device__ __forceinline__ void valcpy_128(double2* dst, double2* src){
	asm volatile ("{.reg .f64 t1;\n\t"
				  ".reg .f64 t2;\n\t"
				  "ld.cv.v2.f64 {t1,t2}, [%1]; \n\t"
				  "st.wt.v2.f64 [%0],{t1,t2};}"  : : "l"(dst),"l"(src):"memory");
}

__device__ __forceinline__ void valcpy_256_interleave(  double2* dst, double2* src){

	double2* d1=dst+blockDim.x*blockDim.y*blockDim.z;
	double2* s1=src+blockDim.x*blockDim.y*blockDim.z;

	asm volatile ("{.reg .f64 t1;\n\t"
				  ".reg .f64 t2;\n\t"
				  ".reg .f64 t3;\n\t"
				  ".reg .f64 t4;\n\t"
				  "ld.cv.v2.f64 {t1,t2}, [%2]; \n\t"
				  "ld.cv.v2.f64 {t3,t4}, [%3]; \n\t"
				  "st.wt.v2.f64 [%0],{t1,t2};\n\t"
				  "st.wt.v2.f64 [%1],{t3,t4};}"  : : "l"(dst),"l"(d1),"l"(src),"l"(s1):"memory");
}

__device__
void __forceinline__ copy_block_large(char* dst, char* src, uint32_t len) {
	int i = TID;
	
	len /= sizeof(double2);
	
	while (i + blockDim.x*blockDim.y*blockDim.z < len) {
		valcpy_256_interleave(((double2*)dst) + i, ((double2*)src) + i);
		i += 2 * blockDim.x*blockDim.y*blockDim.z;
	}
}

__device__
void __forceinline__ copy_block_16(char* dst, char* src, uint32_t len) {
	const int block_size = blockDim.x * blockDim.y * blockDim.z;
	const int chunk_size =  (2 * sizeof(double2) * block_size);
	const int chunk_len = chunk_size * (len / chunk_size);
	int i;
	if (len >= chunk_size) {
		copy_block_large(dst, src, chunk_len);
	}

	dst += chunk_len;
	src += chunk_len;
	len -= chunk_len;
	for (i = TID; i < (len >> 4); i += block_size) {
		valcpy_128(((double2*)dst) + i, ((double2*)src) + i);
	}
	
	__syncthreads();
}

__device__ void copy_block(uchar* dst, volatile uchar*src, int size)
{
	int tid=TID;
	int newsize;
	// get the alignment
	int shift;

#ifdef COPY_PROFILE
	long long t1, t2;

	if (FIRST_THREAD_IN_BLOCK()) {
		t1 = clock64();
	}
#endif

	// checking whether the src/dst is 8/4/2 byte aligned
	if ((((long)dst)&0xf) == 0 && (((long)src)&0xf) == 0) {
		shift = 4;
		newsize=size>>shift;
		copy_block_16((char*)dst,(char*)src,size);
	} else if ((((long)dst)&0x7) == 0 && (((long)src)&0x7) == 0) {
		shift=3;
		newsize=size>>shift;
		aligned_copy<double>(dst,src,newsize,tid);
	} else if ((((long)dst)&0x3) == 0 && (((long)src)&0x3) == 0) {
		shift=2;
		newsize=size>>shift;
		aligned_copy<float>(dst,src,newsize,tid);
	} else if ((((long)dst)&0x1) == 0 && (((long)src)&0x1) == 0) {
		shift=1;
		newsize=size>>shift;
		aligned_copy<char2>(dst,src,newsize,tid);
	} else {
		shift=0;
		newsize=size;
		aligned_copy<char>(dst,src,newsize,tid);
	}

	newsize=newsize<<shift;
	__syncthreads();

	// copying remainders with single thread
	if (FIRST_THREAD_IN_BLOCK()){
		while(newsize<size){
			char2 r=readNoCache(src+newsize);
			dst[newsize]=r.x;newsize++;
			if(newsize<size) dst[newsize]=r.y;
			newsize++;
		}

	}
	__syncthreads();

#ifdef COPY_PROFILE
	if (FIRST_THREAD_IN_BLOCK()) {
		t2 = clock64();
		printf("copy_block clocks %ld, size: %d, src: %p, dst: %p, shift: %d\n", t2-t1, size, src, dst, shift);
	}
#endif
}
template<typename T>
__device__ void inline aligned_copy_srcv(uchar* dst, volatile uchar* src, int newsize, int tid)
{
	while(tid<newsize){
      ((T*)dst)[tid]=*(volatile T*)(((T*)src)+tid);
		tid+=blockDim.x*blockDim.y*blockDim.z;
	}
}

template<typename T>
__device__ void inline aligned_copy_dstv(volatile uchar* dst, uchar* src, int newsize, int tid)
{
	while(tid<newsize){
      ((volatile T*)dst)[tid]=*(((T*)src)+tid);
		tid+=blockDim.x*blockDim.y*blockDim.z;
	}
}

__device__ void copy_block_src_volatile(uchar* dst, volatile uchar* src, int size)
{
	int tid=TID;
	int newsize;
	// get the alignment
	int shift;

#ifdef COPY_PROFILE
	long long t1, t2;

	if (FIRST_THREAD_IN_BLOCK()) {
		t1 = clock64();
	}
#endif

	// checking whether the src/dst is 16/8/4/2 byte aligned
	if ((((long)dst)&0xf) == 0 && (((long)src)&0xf) == 0) {
		shift = 4;
		newsize=size>>shift;
		copy_block_16((char*)dst,(char*)src,size);
	} else if ((((long)dst)&0x7) == 0 && (((long)src)&0x7) == 0) {
		shift=3;
		newsize=size>>shift;
		aligned_copy_srcv<double>(dst,src,newsize,tid);
	} else if ((((long)dst)&0x3) == 0 && (((long)src)&0x3) == 0) {
		shift=2;
		newsize=size>>shift;
		aligned_copy_srcv<int>(dst,src,newsize,tid);
	} else {
		shift=0;
		newsize=size;
		aligned_copy_srcv<char>(dst,src,newsize,tid);
	}

	newsize=newsize<<shift;
	__syncthreads();

	// copying remainders with single thread
	if (FIRST_THREAD_IN_BLOCK()){
		while(newsize<size){
			char2 r=readNoCache(src+newsize);
			dst[newsize]=r.x;newsize++;
			if(newsize<size) dst[newsize]=r.y;
			newsize++;
		}
	}
	__syncthreads();

#ifdef COPY_PROFILE
	if (FIRST_THREAD_IN_BLOCK()) {
		t2 = clock64();
		printf("copy_block clocks %ld, size: %d, src: %p, dst: %p, shift: %d\n", t2-t1, size, src, dst, shift);
	}
#endif
}

__device__ void copy_block_dst_volatile(volatile uchar* dst, uchar* src, int size)
{
	int tid=TID;
	int newsize;
	// get the alignment
	int shift;

#ifdef COPY_PROFILE
	long long t1, t2;

	if (FIRST_THREAD_IN_BLOCK()) {
		t1 = clock64();
	}
#endif

	// checking whether the src/dst is 16/8/4/2 byte aligned
	if ((((long)dst)&0xf) == 0 && (((long)src)&0xf) == 0) {
		copy_block_16((char*)dst,(char*)src,size);
		shift = 4;
		newsize=size>>shift;

	} else if ((((long)dst)&0x7) == 0 && (((long)src)&0x7) == 0) {
		shift=3;
		newsize=size>>shift;
		aligned_copy_dstv<double>(dst,src,newsize,tid);
	} else if ((((long)dst)&0x3) == 0 && (((long)src)&0x3) == 0) {
		shift=2;
		newsize=size>>shift;
		aligned_copy_dstv<int>(dst,src,newsize,tid);
	} else {
		shift=0;
		newsize=size;
		aligned_copy_dstv<char>(dst,src,newsize,tid);
	}

	newsize=newsize<<shift;
	__syncthreads();

	// copying remainders with single thread
	if (FIRST_THREAD_IN_BLOCK()){
		while(newsize<size){
			char2 r=readNoCache(src+newsize);
			dst[newsize]=r.x;newsize++;
			if(newsize<size) dst[newsize]=r.y;
			newsize++;
		}

	}
	__syncthreads();

#ifdef COPY_PROFILE
	if (FIRST_THREAD_IN_BLOCK()) {
		t2 = clock64();
		printf("copy_block clocks %ld, size: %d, src: %p, dst: %p, shift: %d\n", t2-t1, size, src, dst, shift);
	}
#endif
}

