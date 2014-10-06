#ifndef __COMMON_H__
#define __COMMON_H__

#include "assert.h"

#define BUF_SIZE 65536
#define NR_MSG   60000

#define MSG_SIZE BUF_SIZE

#define MSG_SIZE_LAT 64
#define NR_MSG_LAT 30000

#define NR_CLIENT_TB 4
#define NR_SERVER_TB 5
#define THREADS_PER_TB 256

#include <rdma/rsocket.h>

#define VERIFY_LIGHT

template <int LEN_MSG, int NR_ITERATION>
int bench_recv(int sock, char* buf) {
  int i;
  
  for (i = 0; i < NR_ITERATION; i++) {
    int ret, recved = 0;
	
    do {
      if( (ret = rrecv(sock, buf + recved, LEN_MSG - recved, 0)) < 0)
      {
        perror("rrecv failed");
        return -1;
      } else {
        recved += ret;
      }
    } while (recved < LEN_MSG);

#ifdef VERIFY_LIGHT
	assert(((int*)buf)[i % (LEN_MSG/sizeof(int))] == i);
#endif
  }
  return 0;
}

template <int LEN_MSG, int NR_ITERATION>
int bench_send(int sock, char* buf) {
  int i;
  
  for (i = 0; i < NR_ITERATION; i++) {
    int ret, sent = 0;
#ifdef VERIFY_LIGHT
	((int*)buf)[i % (LEN_MSG/sizeof(int))] = i;
#endif

    do {
      if( (ret = rsend(sock, buf + sent, LEN_MSG - sent, 0)) < 0)
      {
        perror("rsend failed");
        return -1;
      } else {
        sent += ret;
      }
    } while (sent < LEN_MSG);


  }
  return 0;
}

template <int LEN_MSG>
int prepare_recv(int sock, char *buf) {
  int ret = 0;
  if (ret = bench_send<LEN_MSG, 1>(sock, buf))
    return ret;
    
  ret = bench_recv<LEN_MSG, 1>(sock, buf);

  return ret;
}

template <int LEN_MSG>
int prepare_send(int sock, char *buf) {
  int ret = 0;
  if (ret = bench_recv<LEN_MSG, 1>(sock, buf))
    return ret;

  fprintf(stderr, "prepare_send recved\n");
  
  ret = bench_send<LEN_MSG, 1>(sock, buf);

  fprintf(stderr, "prepare_send sent\n");

  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
int bench_recv_send_bw(int sock) {
  char message[LEN_MSG];

  int ret;
  
  struct timeval tv1, tv2, tv3;

  double t_in_ms;


  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_recv_send prepare_recv");
    return ret;
  }

  fprintf(stderr, "recv length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);
  
  gettimeofday(&tv1, NULL);

  if (ret = bench_recv<LEN_MSG, NR_ITERATION>(sock, message)) {
    perror("bench_recv_send bench_recv");
    return ret;
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "recv time: %f ms, bw: %f MB/s\n", t_in_ms, (double)(NR_MSG - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0);

  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_recv_send prepare_send");
    return ret;
  }
  fprintf(stderr, "send length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);
  
  gettimeofday(&tv1, NULL);
  
  if (ret = bench_send<LEN_MSG, NR_ITERATION>(sock, message)) {
    perror("bench_recv_send bench_send");
    return ret;
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "send time: %f ms, bw: %f MB/s\n", t_in_ms, (double)(NR_ITERATION - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0);

  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }
  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }


  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
int bench_send_recv_bw(int sock) {
  char message[LEN_MSG];

  struct timeval tv1, tv2, tv3;

  double t_in_ms;
  int ret;

  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }

  fprintf(stderr, "send length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);

  gettimeofday(&tv1, NULL);

  if (ret = bench_send<LEN_MSG, NR_ITERATION>(sock, message)) {
    perror("bench_send_recv bench_send");
    return ret;
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "send time: %f ms, bw: %f MB/s\n", t_in_ms, (double)(NR_ITERATION - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0);

  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_recv");
    return ret;
  }

  fprintf(stderr, "recv length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);
  
  gettimeofday(&tv1, NULL);

  if (ret = bench_recv<LEN_MSG, NR_ITERATION>(sock, message)) {
    perror("bench_send_recv bench_recv");
    return ret;
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "recv time: %f ms, bw: %f MB/s\n", t_in_ms, (double)(NR_MSG - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0);

  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }

  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }

  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
int bench_send_recv_lat(int sock) {
  char message[LEN_MSG];

  struct timeval tv1, tv2, tv3;

  double t_in_ms;
  int ret;

  fprintf(stderr, "bench_recv_send_lat preparation\n");
  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }
  fprintf(stderr, "bench_recv_send_lat preparation done\n");

  fprintf(stderr, "length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);
  
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < NR_ITERATION; i++) {
    if (ret = bench_send<LEN_MSG, 1>(sock, message)) {
      perror("bench_send_recv bench_send");
      return ret;
    }
    
    if (ret = bench_recv<LEN_MSG, 1>(sock, message)) {
      perror("bench_send_recv bench_send");
      return ret;
    }
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "total time: %f ms, RTT: %f us\n", t_in_ms, t_in_ms * 1000.0 / NR_ITERATION);

  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }
  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }


  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
int bench_recv_send_lat(int sock) {
  char message[LEN_MSG];

  struct timeval tv1, tv2, tv3;

  double t_in_ms;
  int ret;

  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }

  fprintf(stderr, "length: %d, iteration: %d\n",
		  LEN_MSG, NR_ITERATION);
  
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < NR_ITERATION; i++) {
    if (ret = bench_recv<LEN_MSG, 1>(sock, message)) {
      perror("bench_send_recv bench_send");
      return ret;
    }
    
    if (ret = bench_send<LEN_MSG, 1>(sock, message)) {
      perror("bench_send_recv bench_send");
      return ret;
    }
  }

  gettimeofday(&tv2, NULL);

  timersub(&tv2, &tv1, &tv3);

  t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);

  fprintf(stderr, "total time: %f ms, RTT: %f us\n", t_in_ms, t_in_ms * 1000.0 / NR_ITERATION);

  if (ret = prepare_recv<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }
  if (ret = prepare_send<LEN_MSG>(sock, message)) {
    perror("bench_send_recv prepare_send");
    return ret;
  }

  return ret;
}

#ifdef __CUDACC__

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send_recv_bw(int sock);

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv_send_bw(int sock);


template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send_recv_lat(int sock);

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv_send_lat(int sock);

__device__ char g_message[512][MSG_SIZE];

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv(int sock, char* buf);

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send(int sock, char* buf);
#endif

#ifdef __CUDA_ARCH__

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv(int sock, char* buf) {
  int i;
  
  for (i = 0; i < NR_ITERATION; i++) {
    int ret, recved = 0;
#ifdef VERBOSE
	BEGIN_SINGLE_THREAD_PART {
		gprintf4_single("gbench_recv sock: %d, %d out of %d.",
						sock, i, NR_ITERATION, 0);
	} END_SINGLE_THREAD_PART;
#endif
	
    do {
      if( (ret = grecv(sock, (uchar*)buf + recved, LEN_MSG - recved)) < 0)
      {
        printf("grecv failed ret: %d\n", ret);
        return -1;
      } else {
        recved += ret;
      }
    } while (recved < LEN_MSG);

#ifdef VERIFY_LIGHT
	if(((int*)buf)[i % (LEN_MSG/sizeof(int))] != i) {
		BEGIN_SINGLE_THREAD_PART {
			gprintf4_single("gbench_recv failed %d (expected %d)",
							((int*)buf)[i % (LEN_MSG/sizeof(int))],
							i, 0, 0);
		} END_SINGLE_THREAD_PART;
		assert(false);
	}
#endif
	
#ifdef VERBOSE
	BEGIN_SINGLE_THREAD_PART {
		gprintf4_single("gbench_recv sock: %d, %d out of %d RECVED.",
						sock, i, NR_ITERATION, 0);
	} END_SINGLE_THREAD_PART;
#endif
  }
  return 0;
}

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send(int sock, char* buf) {
  int i;
  
  for (i = 0; i < NR_ITERATION; i++) {
    int ret, sent = 0;
#ifdef VERIFY_LIGHT
	((int*)buf)[i % (LEN_MSG/sizeof(int))] = i;
#endif
	 
#ifdef VERBOSE
	BEGIN_SINGLE_THREAD_PART {
		gprintf4_single("gbench_send sock: %d, %d out of %d.",
						sock, i, NR_ITERATION, 0);
	} END_SINGLE_THREAD_PART;
#endif
    do {
      if( (ret = gsend(sock, (uchar*)buf + sent, LEN_MSG - sent)) < 0)
      {
        printf("gsend failed ret: %d\n", ret);
        return -1;
      } else {
        sent += ret;
      }
    } while (sent < LEN_MSG);

#ifdef VERBOSE
	BEGIN_SINGLE_THREAD_PART {
		gprintf4_single("gbench_send sock: %d, %d out of %d SENT.",
						sock, i, NR_ITERATION, 0);
	} END_SINGLE_THREAD_PART;
#endif

  }
  return 0;
}

template <int LEN_MSG>
__device__ inline int gprepare_recv(int sock, char *buf) {
  int ret = 0;

  BEGIN_SINGLE_THREAD_PART {
	  gprintf4_single("gprepare_recv sock %d before send\n", sock, 0, 0, 0);
  } END_SINGLE_THREAD_PART;

  if (ret = gbench_send<LEN_MSG, 1>(sock, buf))
    return ret;

  BEGIN_SINGLE_THREAD_PART {
	  gprintf4_single("gprepare_recv sock %d send done. before recv\n", sock, 0, 0, 0);
  } END_SINGLE_THREAD_PART;
    
  ret = gbench_recv<LEN_MSG, 1>(sock, buf);

  return ret;
}

template <int LEN_MSG>
__device__ inline int gprepare_send(int sock, char *buf) {
  int ret = 0;
  if (ret = gbench_recv<LEN_MSG, 1>(sock, buf))
    return ret;
  
  ret = gbench_send<LEN_MSG, 1>(sock, buf);

  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv_send_bw(int sock) {

  int ret;
  
  struct gtimeval tv1, tv2;

  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_recv_send gprepare_recv ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);

  if (ret = gbench_recv<LEN_MSG, NR_ITERATION>(sock, g_message[blockIdx.x])) {
    printf("gbench_recv_send gbench_recv ret: %d\n", ret);
    return ret;
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {
    
    double t_in_ms;
    struct gtimeval tv3;

    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    gprintf4_single("recv time: %d ms, bw: %d MB/s\n", (int)t_in_ms, (int)((double)(NR_ITERATION - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0), 0, 0);
    
  } END_SINGLE_THREAD_PART;
    
  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_recv_send gprepare_send ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);
  
  if (ret = gbench_send<LEN_MSG, NR_ITERATION>(sock, g_message[blockIdx.x])) {
    printf("gbench_recv_send gbench_send ret: %d\n", ret);
    return ret;
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {
    
    double t_in_ms;
    struct gtimeval tv3;
    
    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    
    gprintf4_single("send time: %d ms, bw: %d MB/s\n", (int)t_in_ms, (int)((double)(NR_ITERATION - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0), 0, 0);
  } END_SINGLE_THREAD_PART;

  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }


  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send_recv_bw(int sock) {

  struct gtimeval tv1, tv2;
  int ret;

  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);

  if (ret = gbench_send<LEN_MSG, NR_ITERATION>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gbench_send ret: %d\n", ret);
    return ret;
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {

    double t_in_ms;
    struct gtimeval tv3;
    
    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    
    gprintf4_single("send time: %d ms, bw: %d MB/s\n", (int)t_in_ms, (int)((double)(NR_ITERATION - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0), 0, 0);
    
  } END_SINGLE_THREAD_PART;

  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_recv ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);

  if (ret = gbench_recv<LEN_MSG, NR_ITERATION>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gbench_recv ret: %d\n", ret);
    return ret;
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {
    
    double t_in_ms;
    struct gtimeval tv3;

    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    gprintf4_single("recv time: %d ms, bw: %d MB/s\n", (int)t_in_ms, (int)((double)(NR_MSG - 1) * LEN_MSG * 1000.0 / t_in_ms / 1024.0 / 1024.0), 0, 0);

  } END_SINGLE_THREAD_PART;

  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }


  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_send_recv_lat(int sock) {
  
  struct gtimeval tv1, tv2;
  int ret;
  
  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);

  for (int i = 0; i < NR_ITERATION; i++) {
    if (ret = gbench_send<LEN_MSG, 1>(sock, g_message[blockIdx.x])) {
      printf("bench_send_recv bench_send ret: %d\n", ret);
      return ret;
    }
    
    if (ret = gbench_recv<LEN_MSG, 1>(sock, g_message[blockIdx.x])) {
      printf("bench_send_recv bench_send ret: %d\n", ret);
      return ret;
    }
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {
    double t_in_ms;
    struct gtimeval tv3;
    
    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    gprintf4_single("total time: %d ms, RTT: %d us\n", (int)t_in_ms, (int)(t_in_ms * 1000.0 / NR_ITERATION), 0, 0);
  } END_SINGLE_THREAD_PART;

  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }

  return ret;
}

template <int LEN_MSG, int NR_ITERATION>
__device__ inline int gbench_recv_send_lat(int sock) {
  struct gtimeval tv1, tv2;
  int ret;
  
  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_recv ret: %d\n", ret);
    return ret;
  }
  
  ggettimeofday(&tv1);

  for (int i = 0; i < NR_ITERATION; i++) {
    if (ret = gbench_recv<LEN_MSG, 1>(sock, g_message[blockIdx.x])) {
      printf("gbench_send_recv gbench_recv ret: %d\n", ret);
      return ret;
    }
    
    if (ret = gbench_send<LEN_MSG, 1>(sock, g_message[blockIdx.x])) {
      printf("gbench_send_recv gbench_send ret: %d\n", ret);
      return ret;
    }
  }

  ggettimeofday(&tv2);

  BEGIN_SINGLE_THREAD_PART {
    double t_in_ms;
    struct gtimeval tv3;
    
    gtimersub(&tv2, &tv1, &tv3);
    t_in_ms = ((double)tv3.tv_sec * 1000.0) + ((double)tv3.tv_usec / 1000.0);
    gprintf4_single("total time: %d ms, RTT: %d us\n", (int)t_in_ms, (int)(t_in_ms * 1000.0 / NR_ITERATION), 0, 0);
  } END_SINGLE_THREAD_PART;

  if (ret = gprepare_recv<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }
  if (ret = gprepare_send<LEN_MSG>(sock, g_message[blockIdx.x])) {
    printf("gbench_send_recv gprepare_send ret: %d\n", ret);
    return ret;
  }


  return ret;
}


#endif


#endif
