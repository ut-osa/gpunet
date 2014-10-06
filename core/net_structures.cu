#include "net_structures.cu.h"
#include "net_globals_add.cu.h"
#include "net_constants.h"
#include "util.cu.h"
#include <assert.h>

__device__ void STable::init(){
	lock=0;
	bzero_thread(_sockets,sizeof(gpu_socket)*SOC_TABLE_SIZE);
	for (int i=0;i<SOC_TABLE_SIZE;i++){
		_sockets[i].init(i);
	}
}

__device__ int STable::findNewEntry() {

	int res=E_SOCTABLE_FULL;
	MUTEX_LOCK(lock);
	for(int i=0;i<SOC_TABLE_SIZE;i++){

		if ( _sockets[i].e_flags == EMPTY){
			_sockets[i].allocate();
			__threadfence();
			res=i;
			break;
		}
	}
	MUTEX_UNLOCK(lock);
	return res;
}

__device__ void STable::free_socket(int soc){
	GPU_ASSERT(soc<SOC_TABLE_SIZE && soc>=0);
	_sockets[soc].free();
	__threadfence();
}


__device__ void gpu_socket::init(int slot ){
#ifdef DEFAULT_TO_UNIX_SOCKET
	recv_buffer_offset=CPU_IO_BUF_SIZE*2*slot;
	send_buffer_offset = CPU_IO_BUF_SIZE*(2*slot+1);
#endif
	e_flags=STable::EMPTY;
	cpu_soc=-1;
}

__device__ void gpu_socket::allocate(){
	e_flags=STable::ALLOCATED;
}

__device__ void gpu_socket::free(){
	e_flags=STable::EMPTY;
	cpu_soc=-1;
}


