#ifndef UTIL_HH_
#define UTIL_HH_

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdarg.h>
#include <time.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <sys/time.h>

#define likely(x)    __builtin_expect(!!(x), 1)
#define unlikely(x)  __builtin_expect(!!(x), 0)

#define RAISE_SIGTRAP   raise(SIGTRAP)

#if defined __cplusplus
#define BEGIN_C_DECLS extern "C" {
#define END_C_DECLS }
#else
#define BEGIN_C_DECLS
#define END_C_DECLS
#endif

#if defined __cplusplus
#define ALIGN(x,a)              __ALIGN_MASK(x,(typeof(x))(a)-1)
#else
#define ALIGN(x,a)              __ALIGN_MASK(x,(typeof(x))(a)-1)
#endif
#define __ALIGN_MASK(x,mask)    (((x)+(mask))&~(mask))

/*
 * call stack trace fn
 */

inline void __ASSERT(char const * const func, const char * const file,
                     const int line, const char * const format, ...) {
	char buf[4096] = { 0 };
	char msg[4096] = { 0 };

	sprintf(buf, "\n");
	sprintf(buf, "%s\t%s:%d\n", buf, file, line);
	sprintf(buf, "%s\tAssertion '%s' failed.\n", buf, func);
	va_list args;
	va_start(args, format);
	vsprintf(msg, format, args);
	va_end(args);

	fflush(stdout);
	fflush(stderr);

	fprintf(stderr, "%s\t%s", buf, msg);

	fflush(stdout);
	fflush(stderr);

	RAISE_SIGTRAP;
	abort();
}

#define ASSERT(val)								\
    ASSERT_FMT_MSG(val, "%s", "")

#define ASSERT_ERRNO(val)												\
    if(unlikely((val))) {__ASSERT(#val, __FILE__, __LINE__, "errno = %3d : %s\n", errno, strerror(errno));}

#define ASSERT_CUDA(val)												\
    if(unlikely((val))) {__ASSERT(#val, __FILE__, __LINE__, "errno = %3d : %s\n", static_cast<int>(val), cudaGetErrorString(val));}

#define ASSERT_MSG(val, msg)					\
    ASSERT_FMT_MSG(val, "%s", msg)

#define ASSERT_FMT_MSG(val, fmt, ...)									\
    if(unlikely(!(val))) {__ASSERT(#val, __FILE__, __LINE__, fmt, __VA_ARGS__);}

#define NORMAL_COLOR           "\033[0m"
#define RED_COLOR              "\033[1;31m"
#define GREEN_COLOR            "\033[1;32m"
#define YELLOW_COLOR           "\033[1;33m"

#define DBG(fmt, ...)													\
    printf(RED_COLOR "%s " GREEN_COLOR "%s " YELLOW_COLOR "%04d " NORMAL_COLOR ": " fmt, __FILE__, __func__, __LINE__, __VA_ARGS__);


#if defined(__CUDACC__)
#define __ALIGN(n) __align__(n)
#elif defined(__GNUC__)
#define __ALIGN(n) __attribute__((aligned(n)))
#else
#error "Can't defined ALIGN(n) macro."
#endif

#define ALIGN_32  __ALIGN(32)
#define ALIGN_64  __ALIGN(64)
#define ALIGN_128 __ALIGN(128)
#define ALIGN_DEFAULT ALIGH_64

#ifndef CLOCK_MONOTONIC_RAW
#define CLOCK_MONOTONIC_RAW 1
#endif

#endif /* UTIL_HH_ */
