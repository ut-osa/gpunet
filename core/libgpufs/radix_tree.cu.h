#ifndef radix_tree_h_
#define radix_tree_h_
#include "fs_constants.h"
#include "fs_structures.cu.h"
#include "util.cu.h"
struct rt_node;

struct list_head
{
	volatile rt_node* next;
	volatile rt_node* prev;
};

#define LIST_PREV(n) ((n)->list.prev)
#define LIST_NEXT(n) ((n)->list.next)

#define LIST_HEAD_INIT(h) LIST_NEXT(h)=(h);LIST_PREV(h)=(h);

#define LIST_DEL(n)  \
		 GPU_ASSERT((n)!=&busy_list);\
		 LIST_PREV(LIST_NEXT(n))=LIST_PREV(n); __threadfence();\
		LIST_NEXT(LIST_PREV(n))=LIST_NEXT(n); __threadfence(); \
		LIST_PREV(n)=NULL; LIST_NEXT(n)=NULL; __threadfence();

#define LIST_ADD(head,n) \
		GPU_ASSERT(LIST_NEXT(head)!=n);\
		LIST_NEXT(n)=LIST_NEXT(head); LIST_PREV(n)=(head); __threadfence(); \
		GPU_ASSERT(LIST_NEXT(n)!=n && (LIST_PREV(n)!=n));\
		LIST_PREV(LIST_NEXT(n))=(n);  LIST_NEXT(LIST_PREV(n))=(n); __threadfence(); \
		GPU_ASSERT(LIST_NEXT(n)!=n && (LIST_PREV(n)!=n));

#define LIST_EMPTY(head) LIST_NEXT((head))==head
struct rt_node
{
	volatile unsigned char n_leaves;
	union leaves_t{
		volatile FTable_page pages[NUM_LEAVES];
		void volatile * nodes[NUM_LEAVES];
	};
	volatile leaves_t 	leaves;
	volatile list_head	list;
	
	
	__device__ static volatile rt_node* alloc(rt_node volatile* * toUpdatePtr);
	__device__ static void free(volatile rt_node* toFree);
	__device__ void init() volatile;
};

struct rtree
{	
	volatile rt_node root[MAX_LEVELS];
	
	int tree_lock;
	int swap_lock;
	int count;
	int drop_cache;
	int dirty_tree;
	// uniq ID indentifying all pages that will belong to that tree
	int file_id;
	
	rt_node busy_list;

	__device__ void init_thread() volatile;
	
	__device__ static int getOffsetLevel(size_t d_offset,unsigned char* offset);

	__device__ volatile FTable_page* getLeaf(size_t d_offset,FTable_page_locker::page_states_t* pstate, bool locked,int purpose) volatile;

	__device__ void delLastLevelNode(size_t d_offset) volatile;
	__device__ void delete_tree() volatile;
	/* returns the remaining of min_flushed*/
	/* this function is internally synchronized with both locks */
	__device__ int swapout(int fd, int min_flushed, int flags)  volatile;


	__device__ void traverse_all(int fd, bool toFree, bool dirty, int flags) volatile;

	__device__ void lock_tree() volatile{
		MUTEX_LOCK(tree_lock);
	}
	__device__ void unlock_tree() volatile{
		__threadfence();
		MUTEX_UNLOCK(tree_lock);
	}

	__device__ void lock_swap() volatile{
		MUTEX_LOCK(swap_lock);
	}
	__device__ void unlock_swap() volatile{
		__threadfence();
		MUTEX_UNLOCK(swap_lock);
	}
	__device__ __forceinline__ void lock_for_flush() volatile
	{
		MUTEX_LOCK(swap_lock);
		MUTEX_LOCK(tree_lock);
	}

	__device__ __forceinline__ void unlock_after_flush() volatile 
	{
		__threadfence();
		MUTEX_UNLOCK(tree_lock);
		MUTEX_UNLOCK(swap_lock);
	}	
};

#endif
