#include "radix_tree.cu.h"
#include "swapper.cu.h"
#include "util.cu.h"
#include "fs_globals.cu.h"
__device__ int getNewFileId()
{
	return atomicAdd(&g_file_id,1);
}

 DEBUG_NOINLINE __device__ void rt_node::init() volatile
{	
/*	n_leaves=0;
	char* ptr=(char*)&(leaves);
	for(int i=0;i<sizeof(leaves);i++){ ptr[i]=0;}
*/
}

DEBUG_NOINLINE __device__ volatile rt_node* rt_node::alloc( volatile rt_node** toUpdatePtr)
{
	*toUpdatePtr=(rt_node*)g_rtree_mempool.allocNode();
	if (*toUpdatePtr == NULL) return NULL;
	(*toUpdatePtr)->init(); // empty function
	return *toUpdatePtr;
}

DEBUG_NOINLINE __device__  void rt_node::free(volatile rt_node* toFree){
	
	g_rtree_mempool.freeNode(toFree);
}


DEBUG_NOINLINE __device__ void rtree::init_thread() volatile
{
	dirty_tree=0;
	count=0;
	drop_cache=0;
	file_id=-1;
	tree_lock=0;
	swap_lock=0;
	root[0].init();
	root[1].init();
	root[2].init();
	root[3].init();

	root[3].n_leaves=root[2].n_leaves=root[1].n_leaves=1;
	root[0].n_leaves=0;

	root[3].leaves.nodes[0]=&(root[2]);
	root[2].leaves.nodes[0]=&(root[1]);
	root[1].leaves.nodes[0]=&(root[0]);

	LIST_HEAD_INIT(&busy_list);

}
#define GET_OFFSET_LEAF(d_offset,level,res) res[level]=d_offset&MASK; d_offset=d_offset>>LOGNUM_LEAVES; if (d_offset==0) return level; 
DEBUG_NOINLINE __device__ int rtree::getOffsetLevel(size_t d_offset, unsigned char* offset) 
{
	GET_OFFSET_LEAF(d_offset,0,offset);
	GET_OFFSET_LEAF(d_offset,1,offset);
	GET_OFFSET_LEAF(d_offset,2,offset);
	GET_OFFSET_LEAF(d_offset,3,offset);
	GPU_ASSERT(d_offset !=0);	
	return -1;
}



#define MOVE_ONE_LEVEL_DOWN(curr_offset,new_ptr,tmp,ALLOC) \
		(tmp)=&((new_ptr)->leaves.nodes[curr_offset]); \
		if(*(tmp)) (new_ptr)= (volatile rt_node*)(*(tmp)); \
		else \
		if (ALLOC)\
		{ \
			GPU_ASSERT(tree_lock);\
			(new_ptr)->n_leaves++; GPU_ASSERT((new_ptr)->n_leaves<=NUM_LEAVES);\
			(new_ptr)=rt_node::alloc(( rt_node volatile**)tmp);\
		} \
		else return NULL;

//** requires tree lock if invoked with locked=true *//
DEBUG_NOINLINE __device__ volatile FTable_page* rtree::getLeaf(size_t d_offset,FTable_page_locker::page_states_t* pstate, bool locked,int purpose) volatile
{
	unsigned char offset_tmp[MAX_LEVELS];
	unsigned char* offset=offset_tmp;
	int levels=getOffsetLevel(d_offset,offset);
	GPU_ASSERT(levels>=0);

	volatile rt_node* n=&(root[levels]);
	volatile void * volatile* target;
	volatile FTable_page* leaf=NULL;

	switch(levels){
	case 3:
		MOVE_ONE_LEVEL_DOWN(offset[3],n,target,locked);
	case 2:
		MOVE_ONE_LEVEL_DOWN(offset[2],n,target,locked);
	case 1:
		MOVE_ONE_LEVEL_DOWN(offset[1],n,target,locked);
	case 0: 
		leaf=&(n->leaves.pages[offset[0]]);
		if (!locked)
		{
			*pstate=leaf->locker.try_lock_rw();	
			return leaf;
		}
		
		*pstate=leaf->locker.try_lock_init();

		if (*pstate == FTable_page_locker::P_INIT)
		{ 
			GPU_ASSERT(((int)n->n_leaves)>=0);
			if (!n->n_leaves)  
			{
				LIST_ADD(&busy_list,n);
			}
			// if this returns true the node MUST be inited
			// and it's now locked. 
			// otherwise we later need to try to 
			// lock it for rw/init 
			
			// can change only with tree_lock taken
			GPU_ASSERT(tree_lock);
			n->n_leaves++; 
			count++; 
			GPU_ASSERT(n->n_leaves<=NUM_LEAVES);
			if (purpose == PAGE_WRITE_ACCESS){
				dirty_tree=1;
			}
		}

		break;
	default:
		GPU_ASSERT(NULL);
	}
	return leaf;
}

/* this function deletes only the last level node (assuming all its leaves have been deleted
   assumes tree_lock to be taken
   */
#define FREE_LAST_LEVEL(parent,offset) (parent)->n_leaves--; GPU_ASSERT(tree_lock);
#define FREE_LEAF(parent,offset) FREE_LAST_LEVEL(parent,offset); (parent)->leaves.nodes[offset]=NULL;
#define FREE_NODE(node) if ((node)->n_leaves != 0) break; rt_node::free(node);
#define FREE_LAST_LEVEL_NODE(node) if ((node)->n_leaves != 0) break; LIST_DEL(node);rt_node::free(node);
DEBUG_NOINLINE __device__ void rtree::delLastLevelNode( size_t d_offset) volatile
{
	unsigned char offset[MAX_LEVELS];
	volatile rt_node* route_node0; 
	volatile rt_node* route_node1; 
	volatile rt_node* route_node2; 
	volatile rt_node* route_node3; 

	int levels=getOffsetLevel(d_offset,offset);
	GPU_ASSERT(levels>=0);
	volatile rt_node* n=&root[levels];
	
	switch(levels){
	case 0: // nothing to delete
		GPU_ASSERT(((int)n->n_leaves)>=0);
		if ((n)->n_leaves==0) LIST_DEL(n);
		break;
	case 3:
		route_node3=n;
		route_node2=(volatile rt_node*)(route_node3)->leaves.nodes[offset[3]];
		route_node1=(volatile rt_node*)(route_node2)->leaves.nodes[offset[2]];
		route_node0=(volatile rt_node*)(route_node1)->leaves.nodes[offset[1]];

		FREE_LAST_LEVEL_NODE(route_node0);
		FREE_LEAF(route_node1,offset[1]);
		FREE_NODE(route_node1);
		FREE_LEAF(route_node2,offset[2]);
		FREE_NODE(route_node2);
		FREE_LEAF(route_node3,offset[3]);
		break;

	case 2:

		route_node2=n;
		route_node1=(volatile rt_node*)(route_node2)->leaves.nodes[offset[2]];
		route_node0=(volatile rt_node*)(route_node1)->leaves.nodes[offset[1]];
		FREE_LAST_LEVEL_NODE(route_node0);
		FREE_LEAF(route_node1,offset[1]);
		FREE_NODE(route_node1);
		FREE_LEAF(route_node2,offset[2]);
		break;
	case 1: 
		route_node1=n;
		route_node0=(volatile rt_node*)(route_node1)->leaves.nodes[offset[1]];
		
		FREE_LAST_LEVEL_NODE(route_node0);
		FREE_LEAF(route_node1,offset[1]);
		break;
	default:
		GPU_ASSERT(NULL);
	}
		
}

// assumes that the tree had all  its leaves deleted already
// this operation assumes swap lock and tree lock to be taken
DEBUG_NOINLINE __device__ void rtree::delete_tree() volatile
{
	GPU_ASSERT(count==0);
	GPU_ASSERT(tree_lock);
	volatile rt_node* route_nodes[]={0,0,0,&root[3]};
	uchar3 idx={0,0,0};
	uchar3 loc_count;
	for(idx.x=0,loc_count.x=route_nodes[3]->n_leaves; loc_count.x>0;idx.x++){
		if (!route_nodes[3]->leaves.nodes[idx.x]) continue;
		loc_count.x--;	
		route_nodes[2]=(volatile rt_node*)route_nodes[3]->leaves.nodes[idx.x];

		for(idx.y=0,loc_count.y=route_nodes[2]->n_leaves>0; loc_count.y>0;idx.y++){
			if (!route_nodes[2]->leaves.nodes[idx.y]) continue;
			loc_count.y--;	
			route_nodes[1]=(volatile rt_node*)route_nodes[2]->leaves.nodes[idx.y];
			
			for(idx.z=0,loc_count.z=route_nodes[1]->n_leaves; loc_count.z>0;idx.z++){

				if (!route_nodes[1]->leaves.nodes[idx.z]) continue;
				loc_count.z--;	
				
				if ( route_nodes[1]->leaves.nodes[idx.z] ==&root[0]) continue;
				rt_node::free((volatile rt_node*)route_nodes[1]->leaves.nodes[idx.z]); // this zeros out rt_node structure
			}
			if ( route_nodes[1]==&root[1]) continue;
			rt_node::free(route_nodes[1]);
		}
		if (route_nodes[2]==&root[2]) continue;
		rt_node::free(route_nodes[2]);
	}
	init_thread();
/*
	count=0;
	drop_cache=0;
	dirty_tree=0;
	LIST_HEAD_INIT(&busy_list);
*/
}

/** 
  swapout starts from the last_flushed and moves on over the busy_list until it swaps out min_flushed pages
  + it swaps out FULL rt_node 
  + it locks swap to prevent concurrent flushing
  + it locks per page every time it swaps it out 
  + it locks a whole tree to delete rt_node from it
  
  + since the tree lock is not taken until the tree structure is changed, the # of leaves in rt_node 
  + is changed later
  
  + it holds lock_swap and lock_tree at the same time
  + addLeaf holds lock_tree()
  + swapout holds both lock_swap and lock_tree

  + concurrent reads/writes are allowed when some part of the data is being swapped
  + 
  **/

DEBUG_NOINLINE __device__ void rtree::traverse_all(int fd, bool toFree, bool dirty, int flags ) volatile
{
	int all_flushed=1;
	if ( !dirty && !toFree) return;
	// we DO NOT update the number of leaves because traverse_all is either
	// invoked without removal
	// OR it is always the final stage before killing the whole tree
	for(volatile rt_node* n=LIST_PREV(&busy_list);  n!=&busy_list;n=LIST_PREV(n))
	{
		
		// EXPECT NULL HERE - the node could have been deleted 
		if (n==NULL){ n=LIST_PREV(&busy_list); continue; }
	
		int leaf_count=0;
		GPU_ASSERT(n);
		if (n->n_leaves) KILL_BUFFER_CACHE;

		for(int i=0;i<NUM_LEAVES && leaf_count<n->n_leaves;){
			volatile FTable_page *f=(volatile FTable_page*)&(n->leaves.pages[i]);

			i++;

			// lock the page - lock flush until managed to lock
			if (!f->frame) continue;
			
			MUTEX_LOCK(f->locker.lock); // hard lock
			if (f->locker.rw_counter>0 || !f->frame ) 
			{
				if (f->locker.rw_counter>0) all_flushed=0;
				MUTEX_UNLOCK(f->locker.lock);
				continue;
			}
			// check that it's the page that corresponds to the right file_id
			if (f->frame->file_id!=file_id) {
				// if its not the right one, we are on the wrong rt_node
				MUTEX_UNLOCK(f->locker.lock);
				WRONG_FILE_ID;
				break;
			}
		
			f->locker.page_state=FTable_page_locker::P_FLUSH;
			leaf_count++;
			GPU_ASSERT(!(fd<0 && dirty));
			if (dirty && f->frame->dirty) 
			{
				writeback_page(fd,f,flags,1);
				f->frame->dirty=false;
			}
			if (toFree){
				f->freePage(FREE_LOCKED);
			}else{
				f->locker.page_state=FTable_page_locker::P_READY;
				f->locker.unlock_flush();
			}
		}
	}
	if (toFree && !all_flushed)
	{	
		GPU_ASSERT("trying to free a radix tree while not all nodes were possible to flush\n"==NULL);
	}
	if (all_flushed) { dirty_tree=0; count=0;}
	if (toFree && !(LIST_EMPTY(&busy_list))) delete_tree();

}



/*** this function is internally synchronized ***/

DEBUG_NOINLINE __device__ int rtree::swapout(int fd, int num_flushed, int flags)  volatile
{
	// MUST TAKE SWAP LOCK HERE
	GPU_ASSERT(swap_lock);

//	GPU_ASSERT(fd>=0);
	// run through the busy list - 
	volatile rt_node* n;


	n=LIST_PREV(&busy_list); // we can't get NULL here c's busy list is always initialized to NOT null
	
	volatile FTable_page* to_free[NUM_LEAVES]; // all those to be freed
	while( num_flushed>0 && n!=&busy_list )
	{
		// here we MUST expect NULL because the node might be in the process of deletion
		// then restart
		if (n==NULL){ n=LIST_PREV(&busy_list); continue; }


		int flushed=0;
		size_t file_offset=0;
		GPU_ASSERT(n);
		for(int i=0;i<NUM_LEAVES && n->n_leaves;i++){
			volatile FTable_page *f=(volatile FTable_page*)&(n->leaves.pages[i]);
			
			if (!f->frame) continue;

			// lock the page
			if (f->locker.try_lock_flush())
			{
				GPU_ASSERT(f->frame);
				// check that it's the page that corresponds to the right file_id
				if (f->frame->file_id!=file_id) {
					// if its not the right one, we are on the wrong rt_node
					f->locker.unlock_flush();
					WRONG_FILE_ID;
					break;
				}
				// we are going to flush here
				f->locker.page_state=FTable_page_locker::P_FLUSH;
		        	
//	GPU_ASSERT((f->frame->file_offset)>=0);
//debug	
//{
//uchar o[4];
//int levels=getOffsetLevel(f->frame->file_offset>>FS_LOGBLOCKSIZE,o);
//GPU_ASSERT(levels>=0);
//}

				file_offset=f->frame->file_offset;
	
				GPU_ASSERT(fd>=0 || !f->frame->dirty);

				if (f->frame->dirty && f->frame->content_size) 
				{ 
					writeback_page(fd,f,flags,1);
					f->frame->dirty=false;
					FLUSHED_WRITE

				}else{
					FLUSHED_READ
				}
				to_free[flushed]=f;
				flushed++; 

			}else{
				TRY_LOCK_FAILED
			}
		}
		// nothing to remove
		if (flushed==0) {
			n=LIST_PREV(n);
			continue; 
		}


		num_flushed-=flushed;
		// consider removing rt_node - lock the tree 
		//no structural change to the tree is possible
		lock_tree();
		GPU_ASSERT(tree_lock);
		for(int fp=0;fp<flushed;fp++){
			to_free[fp]->freePage(FREE_UNLOCKED);
			__threadfence();
		}
		// update count of valid nodes in the tree
//		n->n_leaves-=flushed;
		GPU_ASSERT(((int)n->n_leaves)>=0);
		count-=flushed;
		n->n_leaves-=flushed;
		if ( n->n_leaves ) {
			
			n=LIST_PREV(n);
			unlock_tree();

			continue; // we do not delete the node
		}
		n=LIST_PREV(n);
		
		delLastLevelNode(file_offset>>FS_LOGBLOCKSIZE);
		GPU_ASSERT(count>=0);

		unlock_tree();
		
	}
	return num_flushed;
}

