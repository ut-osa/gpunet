#ifndef LIBGPUNET_CRC_H
#define LIBGPUNET_CRC_H

#include <stdint.h>
#include <assert.h>
#include "crctable.out"

typedef unsigned int crc_t;

#ifdef __CUDACC__
#define crctable __constant__ gcrctable
#include "crctable.out"
#undef crctable

namespace {
	struct UByte {
		typedef unsigned char type; 
		static crc_t __device__ crc(const unsigned char* ptr, crc_t r)
		{
			r = gcrctable[(r ^ *ptr) & 0xFF] ^ (r >> 8);
			return r;
		}
		static const uintptr_t align_mask = 0;
	};

#define ACCUMULATE_REDUNDANCY(value, bits)			\
	do {							\
		unsigned char byte = ((value)>>(bits)) & 0xFF;	\
		r = gcrctable[(r ^ byte) & 0xFF] ^ (r >> 8);	\
	} while (0)

	struct UInt {
		typedef unsigned int type; 
		static crc_t __device__ crc(const type* ptr, crc_t r)
		{
			unsigned int value = *ptr; // Memory access
			ACCUMULATE_REDUNDANCY(value, 0);
			ACCUMULATE_REDUNDANCY(value, 8);
			ACCUMULATE_REDUNDANCY(value, 16);
			ACCUMULATE_REDUNDANCY(value, 24);
			return r;
		}
		static const uintptr_t align_mask = (4 - 1);
	};

	struct UIntV4 {
		typedef uint4 type;
		static crc_t __device__ crc(const type* ptr, crc_t r)
		{
			type value = *ptr;
			ACCUMULATE_REDUNDANCY(value.x, 0);
			ACCUMULATE_REDUNDANCY(value.x, 8);
			ACCUMULATE_REDUNDANCY(value.x, 16);
			ACCUMULATE_REDUNDANCY(value.x, 24);
			ACCUMULATE_REDUNDANCY(value.y, 0);
			ACCUMULATE_REDUNDANCY(value.y, 8);
			ACCUMULATE_REDUNDANCY(value.y, 16);
			ACCUMULATE_REDUNDANCY(value.y, 24);
			ACCUMULATE_REDUNDANCY(value.z, 0);
			ACCUMULATE_REDUNDANCY(value.z, 8);
			ACCUMULATE_REDUNDANCY(value.z, 16);
			ACCUMULATE_REDUNDANCY(value.z, 24);
			ACCUMULATE_REDUNDANCY(value.w, 0);
			ACCUMULATE_REDUNDANCY(value.w, 8);
			ACCUMULATE_REDUNDANCY(value.w, 16);
			ACCUMULATE_REDUNDANCY(value.w, 24);
			return r;
		}
		static const uintptr_t align_mask = (16 - 1);
	};
#undef ACCUMULATE_REDUNDANCY
}

/*
 * Rewritten from the painless guide
 */
__global__ void crc32b(unsigned char* p, size_t s, crc_t* ret)
{
	crc_t r = 0;
	for(size_t i = 0; i < s; i++) {
		r = gcrctable[(r ^ p[i]) & 0xFF] ^ (r >> 8);
	}
	*ret = r;
}

template<typename T>
__device__ crc_t dev_crc(const void* p, size_t s)
{
	crc_t r = 0;
	const void* terminal = (const char*)p + s;
	const unsigned char* ptr = (const unsigned char*)p;
	while (((T::align_mask) & ((uintptr_t)ptr)) != 0 && ptr < terminal) {
		r = UByte::crc(ptr, r);
		ptr++;
	}
	const typename T::type *aligned_ptr = (const typename T::type*)ptr;
	while (terminal >= (void*)(aligned_ptr+1)) {
		r = T::crc(aligned_ptr, r);
		aligned_ptr++;
	}
	ptr = (unsigned char*)aligned_ptr;
	while (ptr < terminal) {
		r = UByte::crc(ptr, r);
		ptr++;
	}
	return r;
}

template<typename T>
__global__ void crc32t(const void* p, size_t s, crc_t *ret)
{
	//printf("----------\n");
	*ret = dev_crc<T>(p, s);
	//printf("\n");
}
#endif

#define ACCUMULATE_REDUNDANCY(value, bits)			\
	do {							\
		unsigned char byte = ((value)>>(bits)) & 0xFF;	\
		r = crctable[(r ^ byte) & 0xFF] ^ (r >> 8);	\
	} while (0)

inline crc_t crc32(const void* _p, size_t s, crc_t initial)
{
	crc_t r = initial;
	const unsigned char* p = (const unsigned char*)_p;
	size_t i = 0;
	uintptr_t pv = (uintptr_t)&p[i];
	for(;(pv & (sizeof(unsigned long)-1)) != 0; pv++, i++)
		r = crctable[(r ^ p[i]) & 0xFF] ^ (r >> 8);
	for(; i + sizeof(unsigned long) < s; i += sizeof(unsigned long)) {
		unsigned long value = *(unsigned long*)(&p[i]);
		ACCUMULATE_REDUNDANCY(value, 0);
		ACCUMULATE_REDUNDANCY(value, 8);
		ACCUMULATE_REDUNDANCY(value, 16);
		ACCUMULATE_REDUNDANCY(value, 24);
		assert(sizeof(unsigned long) == 8);
		ACCUMULATE_REDUNDANCY(value, 32 + 0);
		ACCUMULATE_REDUNDANCY(value, 32 + 8);
		ACCUMULATE_REDUNDANCY(value, 32 + 16);
		ACCUMULATE_REDUNDANCY(value, 32 + 24);
	}
	for(; i < s; i++)
		r = crctable[(r ^ p[i]) & 0xFF] ^ (r >> 8);
	return r;
}
#undef ACCUMULATE_REDUNDANCY

#endif
