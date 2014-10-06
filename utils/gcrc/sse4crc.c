#include <smmintrin.h>
#include "sse4crc.h"

uint32_t sse42_crc32(const void* _p, size_t s, uint32_t initial)
{
	uint64_t r = initial;
	const unsigned char* p = (const unsigned char*)_p;
	size_t i = 0;
	uintptr_t pv = (uintptr_t)&p[i];
	for(;(pv & (sizeof(uint64_t)-1)) != 0; pv++, i++)
		r = _mm_crc32_u8(r, p[i]);
	const uint64_t* p64 = (const uint64_t*)&p[i];
	for(; i + sizeof(uint64_t) < s; i += sizeof(uint64_t))
		r = _mm_crc32_u64(r, *p64++);
	for(; i < s; i++)
		r = _mm_crc32_u8(r, p[i]);
	return r;
}
