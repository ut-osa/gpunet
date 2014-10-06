#ifndef FS_STRUCTURES_CU_H
#define FS_STRUCTURES_CU_H

#include "fs_constants.h"
struct FTable_page;

struct PFrame{
	volatile Page* page;
	volatile uint rs_offset;
	volatile uint file_id;
	volatile size_t file_offset;
	volatile uint content_size;
	volatile uint dirty;
	volatile FTable_page* fpage;
	__device__ void init_thread(volatile Page* _page, int _rs_offset) volatile;
	__device__ void clean() volatile;
};


struct FTable_page_locker
{
	// this is a placeholder for the read/write lock
	// currently state machine
	enum page_states_t {P_EMPTY,P_INIT,P_READY,P_RW,P_FLUSH,P_UNDEFINED};
	int lock;
	volatile int rw_counter;
	volatile page_states_t page_state;
	volatile int whoami;

	__device__ void init_thread() volatile;
	
	// wait until the lock bcomes available and unlock
	__device__ void lock_wait_unlock() volatile;

	// true if locked for r/w
	// otherwise exclusively locked for init 
	__device__ page_states_t try_lock_init() volatile;
	
	__device__ page_states_t try_lock_rw() volatile;

	__device__ bool lock_init_rw(int blockid) volatile;
	
	__device__ void unlock_init() volatile;

	__device__ void unlock_rw() volatile;

	// true if locked for flush
	__device__ bool try_lock_flush() volatile;
	
	__device__ void unlock_flush() volatile;
};

struct FTable_page
{

	volatile FTable_page_locker locker;
 	volatile PFrame  *frame;
	
	__device__ void init_thread() volatile;
	__device__ void allocPage(int fd,size_t file_offset) volatile;
	__device__ void freePage(bool locked) volatile;

	__device__ void markDirty() volatile;
	
};

class rtree;
struct FTable_entry{

	volatile rtree* pages;
	
	
	//uint file_size;

	__device__ void deepcopy(volatile FTable_entry* f) volatile
	{
		pages=f->pages;
	}
	__device__ void init_thread(volatile rtree* pages ) volatile;
	__device__ void clean() volatile;


};

struct FTable{

	volatile FTable_entry files[MAX_NUM_FILES];
	
	__device__ void init_thread(volatile rtree* pages) volatile;
};
//**********open/close**********//

// separate table for open/close operations
struct OTable_entry{
	volatile char filename[FILENAME_SIZE];
	volatile int status;
	volatile int refCount;
	volatile int cpu_fd;
	volatile size_t size;
	volatile int flags;
	volatile unsigned int cpu_inode;

	__device__ void copy_old_data(volatile OTable_entry* n) volatile
	{
		cpu_fd=n->cpu_fd;
		size=n->size;
		flags=n->flags;
		cpu_inode=n->cpu_inode;
	}

	
	__device__ void init_thread() volatile;

	__device__ void init(volatile const char* _filename, int _flags) volatile;

	__device__ void notify(int cpu_fd, unsigned int cpu_inode, size_t size) volatile;
	
	__device__ void wait_open() volatile;

	__device__ void clean() volatile;
};


struct OTable
{

	volatile OTable_entry entries[FSTABLE_SIZE];
	int _lock; 

	
	__device__ void lock() volatile;
	
	__device__ void unlock() volatile;

	__device__ int findEntry(const volatile char* filename, volatile bool* isNewEntry, int o_flags) volatile;

	__device__ void init_thread() volatile;
		
};


#endif
