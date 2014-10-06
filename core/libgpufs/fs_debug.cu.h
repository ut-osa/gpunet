#ifndef FS_DEBUG_CU_H
#define FS_DEBUG_CU_H


#if 0
#define DEBUG_BUF_SIZE 10240

__device__ char debug_buffer[DEBUG_BUF_SIZE];
__device__ int debug_buffer_ptr;

#define INIT_DEBUG debug_buffer_ptr=0; debug_buffer[0]='\0';

#define WRITE_DEBUG(file,line)  { int l=line; int old=atomicAdd(&debug_buffer_ptr,20); \
						   for( int i=0;i<5;i++,old++)\
						   {debug_buffer[old]=file[i];}\
						    debug_buffer[old]='\0';\
						    for (int i=0;i<14&&line>0;i++,old++){\
							debug_buffer[old]=(l&0x1?'1':'0');\
							l=l>>1;  }\
						        debug_buffer[old]='\n';\
							__threadfence();}

#define PRINT_DEBUG  unsigned char tmp[DEBUG_BUF_SIZE]={0}; void *buf_ptr; \
		      cudaGetSymbolAddress(&buf_ptr,debug_buffer);\
		      cudaMemcpy((void*)tmp,buf_ptr,sizeof(char)*DEBUG_BUF_SIZE,cudaMemcpyDeviceToHost);\
			     fprintf(stderr,"%s \n", tmp);
#endif


#define PRINT_STATS(SYMBOL) { unsigned int tmp;\
			     cudaMemcpyFromSymbol(&tmp,SYMBOL,sizeof(int),0,cudaMemcpyDeviceToHost);\
			     fprintf(stderr,"%s %u\n", #SYMBOL, tmp);}
		 

#define INIT_STATS(SYMBOL) SYMBOL=0;
/*** malloc stats****/
#ifdef MALLOC_STATS
extern __device__ unsigned int numMallocs;
extern __device__ unsigned int numFrees;
extern __device__ unsigned int numPageAllocRetries;
extern __device__ unsigned int numLocklessSuccess;
extern __device__ unsigned int numWrongFileId;


extern __device__ unsigned int numRtMallocs;
extern __device__ unsigned int numRtFrees;
extern __device__ unsigned int numHT_Miss;
extern __device__ unsigned int numHT_Hit;
extern __device__ unsigned int numPreclosePush;
extern __device__ unsigned int numPrecloseFetch;

extern __device__ unsigned int numFlushedWrites;
extern __device__ unsigned int numFlushedReads;
extern __device__ unsigned int numTrylockFailed;

extern __device__ unsigned int numKilledBufferCache;

#define INIT_MALLOC INIT_STATS(numMallocs); INIT_STATS(numFrees); INIT_STATS(numPageAllocRetries); INIT_STATS(numLocklessSuccess); INIT_STATS(numWrongFileId);
#define FREE atomicAdd(&numFrees,1);
#define MALLOC atomicAdd(&numMallocs,1);
#define PAGE_ALLOC_RETRIES atomicAdd(&numPageAllocRetries,1);
#define LOCKLESS_SUCCESS atomicAdd(&numLocklessSuccess,1);
#define WRONG_FILE_ID atomicAdd(&numWrongFileId,1);

#define PRINT_MALLOC PRINT_STATS(numMallocs);
#define PRINT_FREE PRINT_STATS(numFrees);
#define PRINT_PAGE_ALLOC_RETRIES PRINT_STATS(numPageAllocRetries);
#define PRINT_LOCKLESS_SUCCESS PRINT_STATS(numLocklessSuccess);
#define PRINT_WRONG_FILE_ID  PRINT_STATS(numWrongFileId);

#define INIT_RT_MALLOC INIT_STATS(numRtMallocs); INIT_STATS(numRtFrees);
#define RT_FREE atomicAdd(&numRtFrees,1);
#define RT_MALLOC atomicAdd(&numRtMallocs,1);

#define PRINT_RT_MALLOC PRINT_STATS(numRtMallocs);
#define PRINT_RT_FREE PRINT_STATS(numRtFrees);

#define INIT_HT_STAT INIT_STATS(numHT_Miss); INIT_STATS(numHT_Hit);INIT_STATS(numPreclosePush);INIT_STATS(numPrecloseFetch);
#define HT_MISS atomicAdd(&numHT_Miss,1);
#define HT_HIT atomicAdd(&numHT_Hit,1);
#define PRECLOSE_PUSH atomicAdd(&numPreclosePush,1);
#define PRECLOSE_FETCH atomicAdd(&numPrecloseFetch,1);

#define PRINT_PRECLOSE_PUSH PRINT_STATS(numPreclosePush);
#define PRINT_PRECLOSE_FETCH PRINT_STATS(numPrecloseFetch);
#define PRINT_HT_HIT PRINT_STATS(numHT_Hit);
#define PRINT_HT_MISS PRINT_STATS(numHT_Miss);


#define INIT_SWAP_STAT INIT_STATS(numFlushedWrites); INIT_STATS(numFlushedReads); INIT_STATS(numTrylockFailed); INIT_STATS(numKilledBufferCache);
#define FLUSHED_WRITE atomicAdd(&numFlushedWrites,1);
#define FLUSHED_READ atomicAdd(&numFlushedReads,1);
#define TRY_LOCK_FAILED atomicAdd(&numTrylockFailed,1);
#define KILL_BUFFER_CACHE atomicAdd(&numKilledBufferCache,1);



#define PRINT_FLUSHED_WRITE PRINT_STATS(numFlushedWrites);
#define PRINT_FLUSHED_READ PRINT_STATS(numFlushedReads);
#define PRINT_TRY_LOCK_FAILED PRINT_STATS(numTrylockFailed);
#define PRINT_KILL_BUFFER_CACHE PRINT_STATS(numKilledBufferCache);

#else

#define INIT_MALLOC 
#define FREE 
#define MALLOC 
#define PAGE_ALLOC_RETRIES 
#define LOCKLESS_SUCCESS 
#define WRONG_FILE_ID 

#define PRINT_MALLOC 
#define PRINT_FREE 
#define PRINT_PAGE_ALLOC_RETRIES 
#define PRINT_LOCKLESS_SUCCESS 
#define PRINT_WRONG_FILE_ID  

#define INIT_RT_MALLOC 
#define RT_FREE 
#define RT_MALLOC 

#define PRINT_RT_MALLOC 
#define PRINT_RT_FREE 

#define INIT_HT_STAT 
#define HT_MISS 
#define HT_HIT
#define PRECLOSE_PUSH 
#define PRECLOSE_FETCH 

#define PRINT_PRECLOSE_PUSH 
#define PRINT_PRECLOSE_FETCH 
#define PRINT_HT_HIT 
#define PRINT_HT_MISS 


#define INIT_SWAP_STAT 
#define FLUSHED_WRITE 
#define FLUSHED_READ
#define TRY_LOCK_FAILED 


#define PRINT_FLUSHED_WRITE 
#define PRINT_FLUSHED_READ 
#define PRINT_TRY_LOCK_FAILED 
#define PRINT_KILL_BUFFER_CACHE

#endif

#define INIT_ALL_STATS { INIT_MALLOC; INIT_RT_MALLOC; INIT_HT_STAT; INIT_SWAP_STAT; }
#endif

