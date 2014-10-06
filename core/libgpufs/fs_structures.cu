

#ifndef FS_STRUCTURES_CU
#define FS_STRUCTURES_CU

#include "fs_structures.cu.h"
#include "radix_tree.cu.h"
#include "fs_globals.cu.h"
DEBUG_NOINLINE __device__ void PFrame::clean() volatile
{
	file_id=(uint)-1;
	content_size=-blockIdx.x;
	file_offset=(uint)-1;
	dirty=0;
	fpage=NULL;
	
}

DEBUG_NOINLINE __device__ void PFrame::init_thread(volatile Page* _page, int _rs_offset) volatile
{
	page=_page; rs_offset=_rs_offset;
	file_id=(uint)-1;
	content_size=0;
	file_offset=(uint)-1;
	dirty=false;
}

DEBUG_NOINLINE __device__ void FTable_page_locker::init_thread() volatile
{
	page_state=P_EMPTY;
	rw_counter=0;
	lock=0;
}

// 
DEBUG_NOINLINE __device__ void FTable_page_locker::lock_wait_unlock() volatile
{
	MUTEX_LOCK(lock);
	MUTEX_UNLOCK(lock);
}

DEBUG_NOINLINE __device__ FTable_page_locker::page_states_t FTable_page_locker::try_lock_rw() volatile
{
         FTable_page_locker::page_states_t ret;
	if (MUTEX_WAS_LOCKED(lock)){
		 if(1==1) return P_UNDEFINED;
	}
	whoami=blockIdx.x;
	ret=page_state;
	if ( page_state == P_RW || page_state == P_READY ) {
		rw_counter++;
		ret=P_READY;
		__threadfence();
	}
	MUTEX_UNLOCK(lock);
	return ret;
}
DEBUG_NOINLINE __device__ FTable_page_locker::page_states_t FTable_page_locker::try_lock_init() volatile
{
	if (MUTEX_WAS_LOCKED(lock)) {
		if (1==1) return P_UNDEFINED;
	}
	// locked at this point
	rw_counter++;
	whoami=blockIdx.x;
	if (page_state==P_EMPTY) {
		page_state=P_INIT;
		__threadfence();
		// lock is kept 
		return P_INIT; 
		
	}
	// 
	__threadfence();
	MUTEX_UNLOCK(lock);
	return page_state; // return the real state 
}
	// true if locked for r/w
	// otherwise exclusively locked for init 
DEBUG_NOINLINE __device__ bool FTable_page_locker::lock_init_rw(int blockid) volatile
{	
	MUTEX_LOCK(lock);
	rw_counter++;
	whoami=blockIdx.x;
	if (page_state == P_READY || page_state == P_RW) 
	{
		page_state = P_RW;
		__threadfence();
	}
	
	if (page_state == P_EMPTY) {

		page_state=P_INIT;
		__threadfence();
		// we will not unlock until we init the page and call unlock_init
		return false;
	}
	
	__threadfence();
	MUTEX_UNLOCK(lock);	
	return true;
}
DEBUG_NOINLINE __device__ void FTable_page_locker::unlock_init() volatile
{
	// MUTEX TAKEN HERE
	page_state = P_RW; 
	// the one which is initing is expected 
	// to also read it
	__threadfence();
	MUTEX_UNLOCK(lock);
}

DEBUG_NOINLINE __device__ void FTable_page_locker::unlock_rw() volatile
{
	MUTEX_LOCK(lock);
	rw_counter--;
	GPU_ASSERT(rw_counter>=0);
	if (rw_counter == 0 ) { page_state = P_READY; }
	__threadfence();
	MUTEX_UNLOCK(lock);
}

// true if locked for flush
DEBUG_NOINLINE __device__ bool FTable_page_locker::try_lock_flush() volatile
{
	if (MUTEX_WAS_LOCKED(lock)) 
	{	
		if (1==1) return false;
	}
	whoami=blockIdx.x;

	if (rw_counter == 0 && ( page_state == P_READY) ) 
	{
		// keep locked!
		return true;
	}
	MUTEX_UNLOCK(lock);

	return false;
}
	
DEBUG_NOINLINE __device__ void FTable_page_locker::unlock_flush() volatile
{
	__threadfence();
	MUTEX_UNLOCK(lock);
}

DEBUG_NOINLINE __device__ void FTable_page::init_thread() volatile
{
	locker.init_thread();	
	frame=NULL;
}

DEBUG_NOINLINE __device__ void FTable_page::markDirty() volatile
{
	frame->dirty=1;
}

DEBUG_NOINLINE __device__ void FTable_page::allocPage(int file_id,size_t file_offset) volatile
{
	GPU_ASSERT(locker.lock ==1 );
	volatile PFrame* newframe=g_ppool->allocPage();

	if (newframe == NULL) 
	{ 
		GPU_ASSERT(NULL);
	}
	newframe->file_id=file_id;
	newframe->content_size=0;
	newframe->fpage=this;
//GPU_ASSERT((file_offset)>=0);

	newframe->file_offset=file_offset;
	__threadfence();
	this->frame=newframe;
	__threadfence();
	GPU_ASSERT(locker.lock ==1 );
}

DEBUG_NOINLINE __device__ void FTable_page::freePage(bool locked) volatile
	{
		if (frame) 
		{
			volatile PFrame* old=frame;
			frame=NULL;
			__threadfence();

			GPU_ASSERT(locker.rw_counter==0);
			GPU_ASSERT(locker.page_state == FTable_page_locker::P_READY || 
					locker.page_state == FTable_page_locker::P_FLUSH);
			GPU_ASSERT( locker.lock == 1 );
			g_ppool->freePage(old,locked);
			__threadfence();
			locker.init_thread();
			__threadfence();
		}
	}

DEBUG_NOINLINE __device__ void FTable_entry::init_thread(volatile rtree *file_rtree) volatile{
	pages=file_rtree;
//	file_size=0;
}

DEBUG_NOINLINE __device__ void  FTable::init_thread(volatile rtree * file_rtree) volatile{
	for(int i=0;i<MAX_NUM_FILES;i++)
	{
		files[i].init_thread(&(file_rtree[i]));
	}
}
__device__ void FTable_entry::clean() volatile
{
	if (pages) pages->dirty_tree=0;
}

//******* OPEN/CLOSE *//

DEBUG_NOINLINE __device__ void OTable_entry::init_thread() volatile
{
	status=FSENTRY_EMPTY;
	refCount=0;
	cpu_fd=-1;
	cpu_inode=(unsigned int)-1;
}


DEBUG_NOINLINE __device__ void OTable_entry::init(const volatile char* _filename, int _flags) volatile
{
	memcpy_thread(filename,_filename, FILENAME_SIZE);
	status=FSENTRY_PENDING;
	refCount=0;
	cpu_fd=-1;
	cpu_inode=(unsigned int)-1;
	flags=_flags;
}
DEBUG_NOINLINE __device__ void OTable_entry::notify(int _cpu_fd, unsigned int _cpu_inode, size_t _size) volatile
{
	cpu_fd=_cpu_fd;
	cpu_inode=_cpu_inode;
	size=_size;
	__threadfence();
	status=FSENTRY_OPEN;
	__threadfence();
}
	
DEBUG_NOINLINE __device__ void OTable_entry::wait_open() volatile
{
	WAIT_ON_MEM(status,FSENTRY_OPEN);	
}

DEBUG_NOINLINE __device__ void OTable_entry::clean() volatile
{
	GPU_ASSERT(refCount==0);
	status=FSENTRY_EMPTY;
}	



DEBUG_NOINLINE __device__ void OTable::lock() volatile
{
	MUTEX_LOCK(_lock);
} 
	
DEBUG_NOINLINE __device__ void OTable::unlock() volatile
{
	MUTEX_UNLOCK(_lock);
}

DEBUG_NOINLINE __device__ void OTable::init_thread() volatile
{
	for(int i=0;i<FSTABLE_SIZE;i++)
	{
		entries[i].init_thread();
		_lock=0;
	}
}
DEBUG_NOINLINE __device__ int OTable::findEntry(volatile const char* filename, volatile bool* isNewEntry, int o_flags) volatile
{
	// TODO: this can be sped up by creating a management GPU buffer instead of searching CPU every time

	*isNewEntry=true;
	int found=E_FSTABLE_FULL;

	for(int i=0;i<FSTABLE_SIZE;i++)
	{
		if (entries[i].status==FSENTRY_EMPTY ) 
		{ 
			found=i; 
		}
		else if (!strcmp_thread(filename,entries[i].filename,FILENAME_SIZE))
		{
			// found entry
			found=i; 
			*isNewEntry=false;
			break;
		}
	}
	if (found!= E_FSTABLE_FULL && *isNewEntry )
	{
		entries[found].init(filename,o_flags);
		__threadfence();
	}
	return found;
}
#endif
