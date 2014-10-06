#define CUDA_SAFE_CALL(x) if((x)!=cudaSuccess) { fprintf(stderr,"CUDA ERROR %s: %d %s\n",__FILE__, __LINE__, cudaGetErrorString(cudaGetLastError())); exit(-1); }

#include <cuda.h>
#include <cuda_runtime.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "fs_constants.h"
#include "fs_debug.cu.h"
#include "util.cu.h"
#include "timer.h"
//DEBUG


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include "bigmatrix_nofiles.cu"

void unmap_close_file(int fd, void* ptr,int len)
{

        if(munmap(ptr,len)) { perror("unmap"); return;}
        close(fd);
}

#include <assert.h>

// size of the output used for data staging
int output_size=FS_BLOCKSIZE;
#define TRIALS 1.0
int main( int argc, char** argv)
{

	
	if(argc<7) {
		fprintf(stderr,"<kernel_iterations> <blocks> <threads> <vector> <matrix> <output> <chunksize>\n\n");
		return -1;
	}
	int trials=atoi(argv[1]);
	int nblocks=atoi(argv[2]);
	int nthreads=atoi(argv[3]);
	size_t data_per_chunk=atol(argv[7]);
	assert(data_per_chunk%2==0);
	data_per_chunk/=2;
	
	assert(data_per_chunk!=0);

	while (data_per_chunk> 1024*1024*1024*2L) data_per_chunk/=2;
	
	fprintf(stderr," iterations: %d blocks %d threads %d\n",trials, nblocks, nthreads);	

	

	double total_time=0;
//	int scratch_size=128*1024*1024*4;

for(int t=0;t<trials+1;t++){


	


	
	int fd_m;
	size_t size_m;
	
	char* h_matrix=(char*)open_map_file(argv[5], &fd_m, &size_m, O_RDONLY);
	assert(h_matrix);
	float* h_d_matrix[2];
	float* d_matrix[2];
        CUDA_SAFE_CALL(cudaHostAlloc(&h_d_matrix[0],data_per_chunk,  cudaHostAllocDefault));
        CUDA_SAFE_CALL(cudaHostAlloc(&h_d_matrix[1],data_per_chunk,  cudaHostAllocDefault));
        CUDA_SAFE_CALL(cudaMalloc(&d_matrix[0],data_per_chunk));
        CUDA_SAFE_CALL(cudaMalloc(&d_matrix[1],data_per_chunk));
	
	assert(size_m%data_per_chunk==0);
	
	int fd_v;
	size_t size_v;

	char* h_vector=(char*)open_map_file(argv[4],&fd_v,&size_v,O_RDONLY);
	assert(h_vector);
	float* h_d_vector;
	float* d_vector;
        CUDA_SAFE_CALL(cudaHostAlloc(&h_d_vector,size_v,  cudaHostAllocDefault));
        CUDA_SAFE_CALL(cudaMalloc(&d_vector,size_v));
	
	assert(data_per_chunk/size_v/nblocks>0);
	
	int fd_v_out;
	size_t size_v_out=size_m/size_v*sizeof(float);
	assert(size_v_out);
	
	char* h_v_out=(char*)open_map_file(argv[6], &fd_v_out, &size_v_out, O_RDWR);
	assert(h_v_out);
	float* h_d_v_out;
	float* d_v_out;
        CUDA_SAFE_CALL(cudaHostAlloc(&h_d_v_out,size_v_out, cudaHostAllocDefault));
        CUDA_SAFE_CALL(cudaMalloc(&d_v_out,size_v_out));
	
	fprintf(stderr,"using: %s for matrix of size %lu, %s for vector of size %lu, %s for output of size %lu, data per chunk %lu\n",
				argv[4], size_m,argv[5],size_v,argv[6],size_v_out,data_per_chunk);
	
	
       	cudaStream_t s[2];
        CUDA_SAFE_CALL(cudaStreamCreate(&s[0]));
        CUDA_SAFE_CALL(cudaStreamCreate(&s[1]));
	
        
	double time_before_mem=_timestamp();
	double total_mem_time=0;
        
	int c=0;
	pread(fd_v, h_d_vector,size_v,0);


	memcpy(h_d_vector,h_vector,size_v);
	CUDA_SAFE_CALL(cudaMemcpy(d_vector,h_d_vector,size_v,cudaMemcpyHostToDevice));
        double time_before=_timestamp();
        if (t==0) time_before=0;
	
	for(size_t i=0 ;i<size_m;i+=data_per_chunk)
	{
		fprintf(stderr,"chunk %lu\n",i);
	
		CUDA_SAFE_CALL(cudaStreamSynchronize(s[c]));
		time_before_mem=_timestamp();
		//
		size_t num_read=pread(fd_m,h_d_matrix[c],data_per_chunk,i);
		assert(num_read==data_per_chunk);
		memcpy(h_d_matrix[c],h_matrix+i,data_per_chunk);
		total_mem_time+=_timestamp()-time_before_mem;;
		CUDA_SAFE_CALL(cudaMemcpyAsync(d_matrix[c],h_d_matrix[c],data_per_chunk,cudaMemcpyHostToDevice,s[c]));
 		bigmatrix_nofiles<<<nblocks,nthreads,0,s[c]>>>(d_matrix[c],d_vector,d_v_out,i/size_v,
								data_per_chunk/(sizeof(float)), size_v/(sizeof(float)));
		c=c^0x1;
	}			
	time_before_mem=_timestamp();
	//CUDA_SAFE_CALL(cudaDeviceSynchronize());
	CUDA_SAFE_CALL(cudaMemcpy(h_d_v_out,d_v_out,size_v_out,cudaMemcpyDeviceToHost));
	double time_after=_timestamp();
	if(!t) time_after=0;
	total_time+=(time_after-time_before);
	memcpy(h_v_out,h_d_v_out,size_v_out);
	unmap_close_file(fd_v_out,h_v_out,size_v_out);

	unmap_close_file(fd_m,h_matrix,size_m);
	unmap_close_file(fd_v,h_vector,size_v);
	
	fprintf(stderr,"total_mem_time %0.f  total_out_copy %.0f\n",total_mem_time,_timestamp()-time_before_mem);

        cudaFreeHost(h_d_v_out);
        cudaFreeHost(h_d_vector);
        cudaFreeHost(h_d_matrix);

	cudaFree(d_v_out);
	cudaFree(d_vector);
	cudaFree(d_matrix);

    cudaError_t error = cudaDeviceSynchronize();

    //Check for errors and failed asserts in asynchronous kernel launch.
    if(error != cudaSuccess )
    {
        printf("Device failed, CUDA error message is: %s\n\n", cudaGetErrorString(error));
    }
	

//	cudaFree(d_output);	
	cudaDeviceReset();
	if(error) break;

}

	fprintf(stderr,"Performance: %.3f usec CHUNK size %lu \n",total_time/trials,data_per_chunk);
//((double)output_size*(double)nblocks*(double)read_count)/(total_time/TRIALS)/1e3 );
	return 0;
}



