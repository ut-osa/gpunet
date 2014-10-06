#ifndef FS_CALLS_CU
#define FS_CALLS_CU

#include "radix_tree.cu.h"
#include "fs_constants.h"
#include "fs_debug.cu.h"
#include "util.cu.h"
#include "cpu_ipc.cu.h"
#include "mallocfree.cu.h"
#include "fs_structures.cu.h"
#include "timer.h"
#include "hash_table.cu.h"
#include "swapper.cu.h"
#include "fs_globals.cu.h"
#include "preclose_table.cu.h"
#include "fs_calls.cu.h"
// no reference counting here




DEBUG_NOINLINE __device__ int single_thread_fsync(int fd)
{
	int res=0;
	GPU_ASSERT(fd>=0);
	volatile OTable_entry* e=&g_otable->entries[fd];
	GPU_ASSERT(e->refCount>0);




	volatile FTable_entry* file=&(g_ftable->files[fd]);
	unsigned int inode=g_otable->entries[fd].cpu_inode;
	
	GPU_ASSERT(fd>=0);
	GPU_ASSERT(inode!=(unsigned int)-1);
	// globally locking until everything is flushed
	// this is a slow operation so we don't hold the g_otable lock

	res=flush_cpu(file,e,e->flags);

	if (res<0) {
		// TODO: add error handling
		GPU_ASSERT(NULL);
	}
	
	return res;
}


DEBUG_NOINLINE __device__ int gfsync(int fd){
	__shared__ int ret;
	BEGIN_SINGLE_THREAD
		ret=single_thread_fsync(fd);
	END_SINGLE_THREAD;
	return ret;
}

DEBUG_NOINLINE __device__ int single_thread_ftruncate(int fd, int size)
{
	GPU_ASSERT(size==0);
	GPU_ASSERT(fd>=0);

	volatile OTable_entry* e=&g_otable->entries[fd];
	int res= truncate_cpu(e->cpu_fd)==0;
	if (res==0)
	{
		e->size=0;
		g_ftable->files[fd].pages->lock_for_flush();
		if (g_ftable->files[fd].pages->count !=0)
		{
			g_ftable->files[fd].pages->traverse_all(-1,true,0,0); // kill the tree
		}
		g_ftable->files[fd].pages->unlock_after_flush();
	}
	return res;
}


DEBUG_NOINLINE __device__ int gftruncate(int fd,int size){
	__shared__ int ret;
	BEGIN_SINGLE_THREAD
		ret=single_thread_ftruncate(fd,size);
	END_SINGLE_THREAD;
	return ret;
}

DEBUG_NOINLINE __device__ int single_thread_close(int fd)
{
	GPU_ASSERT(fd>=0);
	g_otable->lock();
	volatile OTable_entry* e=&g_otable->entries[fd];
	e->refCount--; 
	GPU_ASSERT(e->refCount>=0);
	int res=0;

	if (e->refCount>0 || e->status!=FSENTRY_OPEN) { __threadfence(); g_otable->unlock(); return 0;}

	// lock in the opening thread
	e->status=FSENTRY_CLOSING;

	volatile FTable_entry* file=&(g_ftable->files[fd]);
	unsigned int inode=g_otable->entries[fd].cpu_inode;
	
	GPU_ASSERT(fd>=0);
	GPU_ASSERT(inode!=(unsigned int)-1);
	volatile CPU_IPC_OPEN_Entry* cpu_e=&(g_cpu_ipcOpenQueue->entries[fd]);
	
	if (file->pages->dirty_tree)
	{
		/// this file is dirty, so we put it into pre_close. 
		g_preclose_table->lock();
		if( g_preclose_table->add(file,e)) 
			GPU_ASSERT("Pre-close file table is full" == 0);
		g_preclose_table->unlock();
		// we do not close the file on a CPU
	} 
	else{

		// we do close now: we must hold a global lock on the otable 
		//  because otherwise the thread which is opening a file will get 
		// a file handle for a closed file
	

		// first, exchange the page cache for this file
		g_closed_ftable.lock_table(inode);
		// this might be a long because it deallocates and frees the tree
		unsigned int drop_residence_inode=0;
		file->pages=g_closed_ftable.exchange(inode, file->pages,&drop_residence_inode); 
		GPU_ASSERT(file->pages);
		g_closed_ftable.unlock_table(inode);
		
	

		res=cpu_e->close(g_otable->entries[fd].cpu_fd,drop_residence_inode);


		if (res<0) {
		//	GPU_ASSERT(NULL);
		}
	}	

	cpu_e->clean();
	file->clean();
	e->clean();
	__threadfence();
	g_otable->unlock();
	return res;
	
}


DEBUG_NOINLINE __device__ int gclose(int fd){
	__shared__ int ret;
	BEGIN_SINGLE_THREAD
		ret=single_thread_close(fd);
	END_SINGLE_THREAD;
	return ret;
}

DEBUG_NOINLINE __device__ int single_thread_open(char* filename, int flags)
{
/*
Lock ftable
find entry
increase ref-count
Unlock ftable
if not found -> ret E_FTABLE_FULL

if (new_entry) -> send CPU open req
else -> wait on CPU open req

if (req failed) -> 
Lock ftable
 dec ref_count
 if last -> delete entry
unlock ftable
*/


	g_otable->lock();
	bool isNewEntry=false;
	int fd=g_otable->findEntry(filename,&isNewEntry,flags);
	
	GPU_ASSERT(fd>=0);

	if (fd<0) { g_otable->unlock(); return E_FSTABLE_FULL;}
	
	volatile OTable_entry* e=&g_otable->entries[fd];
	e->refCount++;
	__threadfence();
	g_otable->unlock();
	
		
	volatile CPU_IPC_OPEN_Entry* cpu_e=&(g_cpu_ipcOpenQueue->entries[fd]);
	
	if (isNewEntry) 	
	{
		
		g_preclose_table->lock();
		if (g_preclose_table->size!=0) {
			if (g_preclose_table->findEntry(e->filename,&g_ftable->files[fd],e) == 0)
			{
				g_preclose_table->unlock();
				e->notify(e->cpu_fd,e->cpu_inode,e->size);
				return fd;
			}
		}
		g_preclose_table->unlock();
		// fetch the 
		cpu_e->open(filename,flags);
		unsigned int cpu_inode=readNoCache(&cpu_e->cpu_inode);


		int cpu_fd=readNoCache(&cpu_e->cpu_fd);
		
		g_closed_ftable.lock_table(cpu_inode);

		volatile rtree* fpages=g_closed_ftable.get(cpu_inode);
		

		if (fpages!=NULL) 
		{
			volatile rtree* fpages_old=g_ftable->files[fd].pages;
			g_ftable->files[fd].pages=fpages;
			g_closed_ftable.reset(cpu_inode, fpages_old);
				
		
		}else{
			g_ftable->files[fd].pages->file_id=getNewFileId();
		}
	
		g_closed_ftable.unlock_table(cpu_inode);

		// make sure we flush the cache if the owner has changed
		int cpu_flush_cache=readNoCache(&cpu_e->flush_cache);
		if (cpu_flush_cache){
			g_ftable->files[fd].pages->lock_for_flush();
			if (g_ftable->files[fd].pages->count !=0)
			{
				g_ftable->files[fd].pages->traverse_all(-1,true,0,0); // kill the tree

			}
			g_ftable->files[fd].pages->file_id=getNewFileId();
			g_ftable->files[fd].pages->unlock_after_flush();
		}
		
		size_t size=readNoCache(&cpu_e->size);
		e->notify(cpu_fd,cpu_inode,size);
	}
	else {
		e->wait_open();
	}
	
	if (e->cpu_fd < 0)  
	{
		g_otable->lock();
		e->refCount--;
		
		if (e->refCount==0) 
		{
			e->clean();
			cpu_e->clean();
		}
		__threadfence();
		g_otable->unlock();
		return E_IPC_OPEN_ERROR;
	}
	return fd;
}



DEBUG_NOINLINE __device__ int gopen(char* filename, int flags){
	__shared__ int ret;
	BEGIN_SINGLE_THREAD
		ret=single_thread_open(filename,flags);
	END_SINGLE_THREAD;
	return ret;
}

#define READ 0
#define WRITE 1
DEBUG_NOINLINE __device__ volatile FTable_page* getRwLockedPage(volatile FTable_entry* fentry, size_t block_id, int fd, int cpu_fd,int type_req){

	__shared__ volatile  FTable_page* fpage;

	__shared__ FTable_page_locker::page_states_t pstate;
	// try lockless path first

BEGIN_SINGLE_THREAD

	int deadlock=0;
	
	int file_id=fentry->pages->file_id;
	while(1){
		deadlock++;
		GPU_ASSERT(deadlock<200);
		pstate=FTable_page_locker::P_INIT;
		fpage=fentry->pages->getLeaf(block_id,&pstate,0,type_req); // lockless first
		if (fpage && pstate == FTable_page_locker::P_READY) {
			// success?
			GPU_ASSERT(fpage->frame);
			if ((block_id<<FS_LOGBLOCKSIZE) == fpage->frame->file_offset && file_id == fpage->frame->file_id)
			{
				LOCKLESS_SUCCESS;
				break;
			}
			
		}
		fentry->pages->lock_tree();
		fpage=fentry->pages->getLeaf(block_id,&pstate,1,type_req); // locked version - updates all the counters
		fentry->pages->unlock_tree();
		// TODO: handle file size!
		// TODO: add reasonable dirty bitmap update here
	
		// at this point we have 3 options
		// 1. pstate == P_INIT => page is locked and needs to be inited
		// 2. pstate == P_UNDEFINED => page is locked by some other process and we need to
		// retry to getLeaf
		// 3. pstate = P_RW => lock page with lock_init_rw
		if (pstate == FTable_page_locker::P_UNDEFINED ) {
			// we'd better block
			PAGE_ALLOC_RETRIES
			fpage->locker.lock_wait_unlock(); // just wait
			continue;
		}
		
		break;
	}
	if (pstate == FTable_page_locker::P_INIT ){
	
		GPU_ASSERT(fpage->locker.lock ==1);
	
		GPU_ASSERT(fpage->frame==NULL);
	

	// if we inited, the page is locked and we just keep going

/*** DEBUG
		if (atomicAdd(&countInited[block_id],1)>=1)  {
			GPU_ASSERT(0);
		}
**/

		fpage->allocPage(file_id,block_id<<FS_LOGBLOCKSIZE);
//GPU_ASSERT((fpage->frame->file_offset)>=0);

		if (cpu_fd>=0)
		{
		   int datasize=read_cpu(cpu_fd,fpage->frame);
	           if (datasize < 0) {
        	         // TODO: error handling
                       GPU_ASSERT("Failed to read data from CPU"==NULL);
                   }
	           fpage->frame->content_size=datasize;
		}
		if (type_req==PAGE_WRITE_ACCESS) fpage->markDirty();
	}
	GPU_ASSERT((pstate == FTable_page_locker::P_INIT && fpage->locker.lock) || (( pstate == FTable_page_locker::P_RW  || pstate== FTable_page_locker::P_READY) && fpage->locker.rw_counter>0) );
	// if we do not need to zero out the page (cpu_fd<0) 

	// if the page was initialized, return. Make sure to return with all threads active
	if (pstate == FTable_page_locker::P_INIT && cpu_fd>=0 ) fpage->locker.unlock_init();
	
END_SINGLE_THREAD
	if ((pstate == FTable_page_locker::P_INIT && cpu_fd >= 0 ) || pstate == FTable_page_locker::P_RW || pstate == FTable_page_locker::P_READY )
		return fpage;

	//fill the page with zeros - optimization for the case of write-once exclusive create owned by GPU
	bzero_page((volatile char*)fpage->frame->page);
	__threadfence(); // make sure all threads will see these zeros

BEGIN_SINGLE_THREAD
	GPU_ASSERT(cpu_fd<0);

	GPU_ASSERT(pstate == FTable_page_locker::P_INIT);
        fpage->frame->content_size=0;
	fpage->locker.unlock_init();
		
//GPU_ASSERT(fpage->frame->file_offset>=0);

END_SINGLE_THREAD
	return fpage;	
}

DEBUG_NOINLINE __device__ int gmsync(volatile void *addr, size_t length,int flags)
{

	size_t tmp=((char*)addr)- ((char*)g_ppool->rawStorage);
//	assert(tmp>=0);
	size_t offset=tmp>>FS_LOGBLOCKSIZE;
	GPU_ASSERT(offset<PPOOL_FRAMES);
	
	__threadfence(); // make sure all writes to the page become visible
BEGIN_SINGLE_THREAD

	volatile PFrame* p=&(g_ppool->frames[offset]);
	volatile FTable_page* fp=p->fpage;

	GPU_ASSERT(fp);
	// super ineffisient way to find which file this page belongs to
	int i=0;
	for(  i=0;i<FSTABLE_SIZE;i++){
		if (p->file_id == g_ftable->files[i].pages->file_id){
			// no lock on page is required - last 0
			writeback_page(g_otable->entries[i].cpu_fd,fp,g_otable->entries[i].flags,0);
			break;
		}
	}
	GPU_ASSERT(i!=FSTABLE_SIZE);
	// if this assert fires it means that the file with that id was not
	// found among open files. That's not valid becuase msync works only if the 
	// file is mapped -> it cannot be closed.
	
END_SINGLE_THREAD
	return 0;
}

DEBUG_NOINLINE __device__ int gmunmap(volatile void *addr, size_t length)
{

	size_t tmp=((char*)addr)- ((char*)g_ppool->rawStorage);
//	assert(tmp>=0);
	size_t offset=tmp>>FS_LOGBLOCKSIZE;
	if (offset>=PPOOL_FRAMES) return -1;
	
	__threadfence(); // make sure all writes to the page become visible
BEGIN_SINGLE_THREAD

	volatile PFrame* p=&(g_ppool->frames[offset]);
	volatile FTable_page* fp=p->fpage;

	GPU_ASSERT(fp);
	fp->locker.unlock_rw();

END_SINGLE_THREAD
	return 0;
}

DEBUG_NOINLINE __device__ volatile void* gmmap(void *addr, size_t size,
					int prot, int flags, int fd, off_t offset)
{

	__shared__ volatile PFrame* frame; // the ptr is to global mem but is stored in shmem
	__shared__ size_t block_id;
	__shared__ int block_offset;

	 volatile FTable_page* fpage;
	
	__shared__ int cpu_fd;
	__shared__ volatile FTable_entry* fentry;
BEGIN_SINGLE_THREAD	
	fentry=&g_ftable->files[fd];
	block_id=offset2block(offset,FS_LOGBLOCKSIZE);
	block_offset=offset2blockoffset(offset,FS_BLOCKSIZE);


	GPU_ASSERT(fd>=0 && fd<MAX_NUM_FILES);
	
	cpu_fd=g_otable->entries[fd].cpu_fd;
	GPU_ASSERT( cpu_fd >=0 && g_otable->entries[fd].refCount >0 );


	
	if (block_offset+size > FS_BLOCKSIZE) assert("Reading beyond the  page boundary"==0);
	
	GPU_ASSERT(block_id<MAX_BLOCKS_PER_FILE);
	
	// decide whether to fetch data or not
	if ( g_otable->entries[fd].flags == O_GWRONCE ) cpu_fd=-1;
	
		
END_SINGLE_THREAD
	int purpose= (g_otable->entries[fd].flags == O_GRDONLY) ? PAGE_READ_ACCESS:PAGE_WRITE_ACCESS;
	fpage=getRwLockedPage(fentry,block_id,fd,cpu_fd, purpose);
BEGIN_SINGLE_THREAD

	// page inited, just read, frane us a _shared_ mem variable
	frame=fpage->frame;
	//TODO: handle reading beyond eof
	if  (frame->content_size < block_offset+size && flags==O_GRDONLY) 
	{
		GPU_ASSERT("Failed to map beyond the end of file"!=NULL);
	}
	if (flags!= O_GRDONLY) atomicMax((uint*)&(frame->content_size),block_offset+size);

END_SINGLE_THREAD
GPU_ASSERT(frame!=NULL); 

	return (void*)(((uchar*)(frame->page))+block_offset);
}	

DEBUG_NOINLINE __device__ size_t gwrite(int fd,size_t offset, size_t size, uchar* buffer)
{
	//attempt to write to a specific block
	//if null -> allocate
	// otherwise -> copy to bufcache
	// mark dirty

	// we ignore that we may run out of disk space
	
	GPU_ASSERT(fd>=0 && fd<MAX_NUM_FILES);
	
	GPU_ASSERT( g_otable->entries[fd].refCount >0 );

	__shared__ volatile PFrame* frame; // the ptr is to global mem but is stored in shmem
	__shared__ size_t block_id;
	__shared__ int block_offset;
	__shared__ int cpu_fd;
	__shared__ int written;

	__shared__ volatile FTable_page* fpage;
	__shared__ volatile FTable_entry* fentry;


	BEGIN_SINGLE_THREAD
		block_id=offset2block(offset,FS_LOGBLOCKSIZE);
		block_offset=offset2blockoffset(offset,FS_BLOCKSIZE);
		fentry=&g_ftable->files[fd];
		cpu_fd=g_otable->entries[fd].cpu_fd;
		if (g_otable->entries[fd].flags == O_GWRONCE || ( size == FS_BLOCKSIZE && block_offset==0)) 
		{	
			// we will not read the data from CPU if (1) the file is ONLY_ONCE, or the writes are whole-page writes
			cpu_fd=-1;
		}	
		written=0;
	END_SINGLE_THREAD
	
	while(written<size){

		int single_op=min((int)(size-written),(int)(FS_BLOCKSIZE-block_offset));	
	
		GPU_ASSERT(block_id<MAX_BLOCKS_PER_FILE);
		//TODO: handle reading beyond eof
		// allow multiple threads to get into this function
		// the value returned is correct only in thread 0
		fpage=getRwLockedPage(fentry,block_id,fd,cpu_fd, PAGE_WRITE_ACCESS);
	
	
	BEGIN_SINGLE_THREAD
		frame=fpage->frame;
		atomicMax((uint*)&frame->content_size,block_offset+single_op);
		fpage->markDirty();
		
	END_SINGLE_THREAD
		// go over the page and reset it if necessary
		// cpu_fd==-1 it will reset the page
	copy_block((uchar*)(frame->page)+block_offset,buffer+written,single_op);
	__threadfence(); // we must sync here otherwise swapper will be inconsistent

	BEGIN_SINGLE_THREAD
		written+=single_op;
		fpage->locker.unlock_rw();
		// the page is unlocked for flush only here.
		block_id++;
		block_offset=0;
	END_SINGLE_THREAD;
	}
	return size;
}

// currently pread is expected to be issued by all threads in a thread block
// with the same parameters
// all parameters other than in thread idx ==0 are ignored

DEBUG_NOINLINE __device__ size_t gread(int fd, size_t offset, size_t size, uchar* buffer)
{


	__shared__ volatile PFrame* frame; // the ptr is to global mem but is stored in shmem

	__shared__  volatile FTable_page* fpage;


	__shared__ size_t block_id;
	__shared__ int block_offset;
	
	__shared__ volatile FTable_entry* fentry;
	__shared__ int cpu_fd;
	__shared__ int  data_read;

BEGIN_SINGLE_THREAD

	block_id=offset2block(offset,FS_LOGBLOCKSIZE);
	block_offset=offset2blockoffset(offset,FS_BLOCKSIZE);
	fentry=&g_ftable->files[fd];
	cpu_fd=g_otable->entries[fd].cpu_fd;

	GPU_ASSERT(fd>=0 && fd<MAX_NUM_FILES);
	
	GPU_ASSERT( cpu_fd >=0 && g_otable->entries[fd].refCount >0 );
	
	data_read=0;
END_SINGLE_THREAD


	while(data_read<size){

		int single_op=min((int)(size-data_read),(int)(FS_BLOCKSIZE-block_offset));	
		
		GPU_ASSERT(block_id<MAX_BLOCKS_PER_FILE);
	
	// synchtreads in getRwLockedPage
		fpage=getRwLockedPage(fentry,block_id,fd,cpu_fd,PAGE_READ_ACCESS);
	
	
		// page inited, just read, frane us a _shared_ mem variable
		frame=fpage->frame;
		//TODO: handle reading beyond eof	
	
		GPU_ASSERT(frame!=NULL); 
	
		copyNoCache_block(buffer+data_read,(uchar*)(frame->page)+block_offset,single_op);
		
	BEGIN_SINGLE_THREAD
		block_offset=0;
		data_read+=single_op;
		block_id++;
		fpage->locker.unlock_rw();
	
	END_SINGLE_THREAD
	}
	return size;
}	


DEBUG_NOINLINE __device__ uint gunlink(char* filename)
{
	GPU_ASSERT(NULL);
	// tobe implemented
	return 0;
}
DEBUG_NOINLINE __device__ size_t fstat(int fd)
{
	return g_otable->entries[fd].size;
}

#endif
