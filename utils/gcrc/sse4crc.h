#ifndef SSE4CRC_H
#define SSE4CRC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint32_t sse42_crc32(const void* _p, size_t s, uint32_t initial);

#ifdef __cplusplus
};
#endif

#endif
