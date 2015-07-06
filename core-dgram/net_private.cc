#include <cuda_runtime.h>
#include <stdio.h>
#include "net_private.h"

gpunet_private::gpunet_private()
	:last_(0)
{
	for(int i = 0; i < SOC_TABLE_SIZE; i++) {
		cudaStreamCreateWithFlags(&stream_array_[i], cudaStreamNonBlocking);
		possible_busy_[i] = false;
	}
}

gpunet_private::~gpunet_private()
{
	for(int i = 0; i < SOC_TABLE_SIZE; i++)
		cudaStreamDestroy(stream_array_[i]);
}

int gpunet_private::register_kernel(const char* fname, const void* func)
{
	ktable_[fname] = func;
	return 0;
}

const void* gpunet_private::find_kernel(const char* fname) const
{
	TypeKTable::const_iterator iter = ktable_.find(fname);
	if (iter != ktable_.end())
		return iter->second;
	return NULL;
}

bool gpunet_private::find_stream(cudaStream_t* ps)
{
	for(int i = 0; i < SOC_TABLE_SIZE; i++) {
		int idx = (last_ + i)%SOC_TABLE_SIZE;
		if (cudaStreamQuery(stream_array_[idx]) == cudaSuccess) {
			*ps = stream_array_[idx];
			last_ = (idx + 1)%SOC_TABLE_SIZE;
			possible_busy_[idx] = true;
			return true;
		}
	}
	return false;
}

bool gpunet_private::is_any_stream_busy()
{
	for(int i = 0; i < SOC_TABLE_SIZE; i++) {
		if (possible_busy_[i]) {
			int ret = cudaStreamQuery(stream_array_[i]);
			if (ret != cudaSuccess) {
				return true;
			} else {
				possible_busy_[i] = false;
			}
		}
	}
	fprintf(stderr, "%s: All finished\n", __func__);
	return false;
}
