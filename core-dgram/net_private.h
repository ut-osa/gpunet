#ifndef NET_PRIVATE_H
#define NET_PRIVATE_H

#include <map>
#include <string>
#include "net_globals.h"

using namespace std;

class gpunet_private {
private:
	typedef map<string, const void*> TypeKTable;
	TypeKTable ktable_;
	cudaStream_t stream_array_[SOC_TABLE_SIZE];
	int last_;
	bool possible_busy_[SOC_TABLE_SIZE];
public:
	gpunet_private();
	~gpunet_private();
	int register_kernel(const char* fname, const void* func);
	const void* find_kernel(const char* fname) const;
	bool find_stream(cudaStream_t*);
	bool is_any_stream_busy();
};


#endif
