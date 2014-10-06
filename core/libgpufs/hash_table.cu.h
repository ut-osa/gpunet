#ifndef __HASH_TABLE_H__
#define __HASH_TABLE_H__

#include "fs_constants.h"
#include "util.cu.h"

#define HT_MODULO MAX_NUM_CLOSED_FILES
#define HT_MODULO_MASK (HT_MODULO-1)

#define BITMAP_SET(b,idx) (b)[(idx)>>5]&=(0x1<<((idx)&31));
#define BITMAP_CLEAR(b,idx) (b)[(idx)>>5]&=(~(0x1<<((idx)&31)));


struct rtree;
struct hash_table_entry
{
	volatile rtree* value;
	volatile int key;
	int lock;
};
struct hash_table
{
	int lock;
	volatile int count;
	//volatile unsigned int bitmap[HT_MODULO/(sizeof(int))];

	volatile hash_table_entry  t[HT_MODULO];

	__device__	void init_thread(volatile rtree* _t) volatile;

	// returns old value
	__device__ volatile rtree* exchange(unsigned int key, volatile rtree* value, unsigned int* out_oldkey) volatile;
		
	__device__ volatile rtree* get(int key) volatile;

	__device__ volatile hash_table_entry* get_next(volatile hash_table_entry* it) volatile;

	__device__ void reset(unsigned int key, volatile rtree* value) volatile;
	__device__ void reset_node(volatile hash_table_entry* node) volatile{
		node->key=-1;
		atomicAdd(((int*)&count),-1);
	}
	
	 __device__ void lock_table(unsigned int idx)volatile{
		
		lock_table_node(&t[idx&HT_MODULO_MASK]);
	}
	 __device__ void unlock_table(unsigned int idx)volatile{
		unlock_table_node(&t[idx&HT_MODULO_MASK]);
	}
	 __device__ void lock_table_node(volatile hash_table_entry* e) volatile{
		MUTEX_LOCK(e->lock);
	}
	
	 __device__ void unlock_table_node(volatile hash_table_entry* e) volatile{
		__threadfence();
		MUTEX_UNLOCK(e->lock);
	}

};

#endif
