#ifndef SWAPPER_CU
#define SWAPPER_CU

#include "swapper.cu.h"
#include "hash_table.cu.h"
#include "fs_structures.cu.h"
#include "radix_tree.cu.h"
#include "fs_constants.h"
#include "fs_globals.cu.h"


DEBUG_NOINLINE __device__ int swapout(int npages){
	//find if there are files in the closed file table

	if (g_closed_ftable.count)
	{
		volatile hash_table_entry* h=NULL;
		do{
			h=g_closed_ftable.get_next(h);
			if (h==NULL) break;
			volatile rtree* toFlush=h->value;

		
			// we do not write since its not dirty => -1
			// this function takes tree lock too
			toFlush->lock_swap();
			npages=toFlush->swapout(-1,npages,-1);
			toFlush->unlock_swap();

			/* now lock the hash table to make sure we're dealing with the same rtree */
			g_closed_ftable.lock_table_node(h);
			if (h->value->count==0){
				// reset the entry
				g_closed_ftable.reset_node(h);
			}
			g_closed_ftable.unlock_table_node(h);

		}while(h && npages>0);
	}
	
	for (int is_dirty=0;is_dirty<2 && npages>0; is_dirty++){
		if (npages>0)
		{
			int i=MAX_NUM_FILES-1;
			volatile rtree* pages;
			int flags;
			int cpu_fd;
			
			for(;i>=0;i--)
			{	
				g_otable->lock();
					pages=g_ftable->files[i].pages;
					cpu_fd=g_otable->entries[i].cpu_fd;
					flags=g_otable->entries[i].flags;
				g_otable->unlock();

				if (pages->count && (is_dirty == pages->dirty_tree))
				{
					pages->lock_swap();
					// the file descriptor can't be wrong because file cannot be closed without
					// 1. writing out its pages
					// 2. changing the metadata only then
					// so if the metadata is changed, no files can be written
					int npages_left=pages->swapout(cpu_fd,npages, flags);
					if( flags == O_GWRONCE && npages_left <npages) 
					{
						 pages->drop_cache=1;
					}
					npages=npages_left;
					pages->unlock_swap();
	
					if (npages<=0) break;
				}
			}
		}
	}
	return npages;
}


/** all the necessary locks are taken **/
DEBUG_NOINLINE __device__ int writeback_page(int cpu_fd, volatile FTable_page* p,int flags, bool tolock)
{

	GPU_ASSERT(p);
	//already flushed?
	volatile PFrame *f=p->frame;
	GPU_ASSERT(cpu_fd>=0);

	int type=(flags==(O_GWRONCE))?RW_IPC_DIFF:RW_IPC_WRITE;
	GPU_ASSERT(p->locker.lock||!tolock);
	GPU_ASSERT(p->locker.rw_counter==0||!tolock);
	GPU_ASSERT(p->locker.page_state== FTable_page_locker::P_FLUSH||!tolock);
	int ret_val=g_cpu_ipcRWQueue->read_write_block(cpu_fd,f->rs_offset<<FS_LOGBLOCKSIZE,f->file_offset,f->content_size,type);
	if (ret_val!=f->content_size) {
		// TODO: add error handling
	//	GPU_ASSERT(NULL);
		return ret_val;
	}
	return 0;
}

/* called for flush only */
DEBUG_NOINLINE __device__ int flush_cpu( volatile FTable_entry* file, volatile OTable_entry* e, int flags)
{
	// go over all dirty pages and write them out.
	// TODO: batching
	// TODO: batch writes
	int ret=0;
	if (file->pages->dirty_tree){
		 file->pages->lock_for_flush();
		 // do not free pages
		 file->pages->traverse_all(e->cpu_fd,file->pages->drop_cache,file->pages->dirty_tree,flags); // flush the data to the host
		 file->pages->unlock_after_flush();
	}
	return ret;
}


#endif
