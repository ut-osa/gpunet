
/*** malloc stats****/
#ifdef MALLOC_STATS
__device__ unsigned int numMallocs;
__device__ unsigned int numFrees;
__device__ unsigned int numPageAllocRetries;
__device__ unsigned int numLocklessSuccess;
__device__ unsigned int numWrongFileId;


__device__ unsigned int numRtMallocs;
__device__ unsigned int numRtFrees;
__device__ unsigned int numHT_Miss;
__device__ unsigned int numHT_Hit;
__device__ unsigned int numPreclosePush;
__device__ unsigned int numPrecloseFetch;

__device__ unsigned int numFlushedWrites;
__device__ unsigned int numFlushedReads;
__device__ unsigned int numTrylockFailed;
__device__ unsigned int numKilledBufferCache;

#endif

