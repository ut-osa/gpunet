#ifndef CPU_IPC_CU
#define CPU_IPC_CU
#include <assert.h>
#include "fs_constants.h"
#include "util.cu.h"
#include "fs_structures.cu.h"
#include "radix_tree.cu.h"
#include "cpu_ipc.cu.h"
#include "fs_globals.cu.h"
__host__ void CPU_IPC_OPEN_Queue::init_host() volatile
{
	for(int i=0;i<FSTABLE_SIZE;i++)
	{
		entries[i].init_host();
	}
}

__host__ void CPU_IPC_RW_Entry::init_host() volatile
{
	status=CPU_IPC_EMPTY;
	cpu_fd=-1;
	buffer_offset=0;
	file_offset=(uint)-1;
	size=0;
	type=0;
	return_value=0;
}

__host__ void CPU_IPC_RW_Queue::init_host() volatile
{
	for(int i=0;i<RW_IPC_SIZE;i++)
	{
		entries[i].init_host();
	}
}

__host__ void CPU_IPC_OPEN_Entry::init_host() volatile
{
	status=CPU_IPC_EMPTY;
	cpu_fd=-1;
	size=0;
	flush_cache=0;
	drop_residence_inode=0;
	cpu_inode=(unsigned int)-1;
	memset((void*)filename,0,FILENAME_SIZE);
	return_value=0;
}

__device__ void CPU_IPC_OPEN_Entry::clean() volatile
{
	filename[0]='\0';
	cpu_fd=-1;
	cpu_inode=(unsigned)-1;
	size=0;
	flush_cache=0;
	drop_residence_inode=0;
	status=CPU_IPC_EMPTY;
	__threadfence_system();
}

__device__ int CPU_IPC_OPEN_Entry::open(const char* reqFname, int _flags) volatile
{
	
	memcpy_thread(filename,reqFname,FILENAME_SIZE);
	flags=_flags;
	__threadfence_system();
	GPU_ASSERT(status==CPU_IPC_EMPTY);
	status=CPU_IPC_PENDING;
	
	__threadfence_system();

	WAIT_ON_MEM(status,CPU_IPC_READY);

	return  readNoCache(&cpu_fd);
}

__device__ int CPU_IPC_OPEN_Entry::close(int _cpu_fd, unsigned int _drop_residence_inode) volatile
{
	GPU_ASSERT(_cpu_fd<=0 || status==CPU_IPC_READY); // if we dont want to push cpu_fd its OK, but then make sure that its valid
	
	cpu_fd=_cpu_fd;
	drop_residence_inode=_drop_residence_inode;

	status=CPU_IPC_PENDING;
	
	__threadfence_system();

	WAIT_ON_MEM(status,CPU_IPC_READY);

	return readNoCache(&cpu_fd);
}

__device__ void CPU_IPC_RW_Entry::clean() volatile 
{
	cpu_fd=-1;
	status=CPU_IPC_EMPTY;
	file_offset=(uint)-1;
	__threadfence_system();
}
__device__ int CPU_IPC_RW_Entry::ftruncate(int _cpu_fd) volatile
{
	cpu_fd=_cpu_fd;
	type=RW_IPC_TRUNC;

	__threadfence_system();
	GPU_ASSERT(status==CPU_IPC_EMPTY);
	status=CPU_IPC_PENDING;
	__threadfence_system();

	WAIT_ON_MEM(status,CPU_IPC_READY);
	return readNoCache(&return_value);
}
__device__ int CPU_IPC_RW_Entry::read_write(int _cpu_fd, size_t _buffer_offset, size_t _file_offset, uint _size, uint _type ) volatile
{
	
	cpu_fd=_cpu_fd;
	buffer_offset=_buffer_offset;
	size=_size;
	type=_type;
	return_value=-1;
	file_offset=_file_offset;

	__threadfence_system();
	GPU_ASSERT(status==CPU_IPC_EMPTY);
	status=CPU_IPC_PENDING;
	__threadfence_system();

	WAIT_ON_MEM(status,CPU_IPC_READY);
	return  readNoCache(&return_value);
}


__device__ void GPU_IPC_RW_Manager::init_thread() volatile
{
	for(int i=0;i<RW_IPC_SIZE;i++)
	{
		_locker[i]=IPC_MGR_EMPTY;
	}
	_lock=0;
}

__device__ int GPU_IPC_RW_Manager::findEntry() volatile
{
	int res=0;

	while(1)
	{ //
		MUTEX_LOCK(_lock);
		res=RW_IPC_SIZE;
		for(int i=0;i<RW_IPC_SIZE;i++)
		{
			if (readNoCache(&_locker[i]) == IPC_MGR_EMPTY)
			{
				_locker[i]=IPC_MGR_BUSY;
				res=i;
				__threadfence();
				break;
			}
		}
		MUTEX_UNLOCK(_lock);
		if (res != RW_IPC_SIZE) break;
	}	
	return res;
}			
__device__ void GPU_IPC_RW_Manager::freeEntry(int entry) volatile
{
	_locker[entry]=IPC_MGR_EMPTY;
	__threadfence();
}
	


__device__ int CPU_IPC_RW_Queue::read_write_block(int cpu_fd, size_t _buffer_offset, size_t _file_offset, int size, int type) volatile
{
	int entry=g_ipcRWManager->findEntry();
	int ret_val=entries[entry].read_write(cpu_fd,_buffer_offset,_file_offset,size,type); // this one will wait until done
	entries[entry].clean();
	g_ipcRWManager->freeEntry(entry);
	return ret_val;
}
	

//*****************External function to read data from CPU**//

__device__ int truncate_cpu(int cpu_fd)
{
	GPU_ASSERT(cpu_fd>=0);
	int entry=g_ipcRWManager->findEntry();
	int ret_val=g_cpu_ipcRWQueue->entries[entry].ftruncate(cpu_fd);
	g_cpu_ipcRWQueue->entries[entry].clean();
	g_ipcRWManager->freeEntry(entry);
	return ret_val;
}

__device__ int read_cpu(int cpu_fd, volatile PFrame* frame)
{
	GPU_ASSERT(cpu_fd>=0);
	int ret_val=g_cpu_ipcRWQueue->read_write_block(cpu_fd,frame->rs_offset*FS_BLOCKSIZE,frame->file_offset,FS_BLOCKSIZE,RW_IPC_READ);
	return ret_val;
}

#endif
