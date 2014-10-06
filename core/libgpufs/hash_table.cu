#ifndef __HASH_TABLE_CU__
#define __HASH_TABLE_CU__

#include "hash_table.cu.h"
#include "radix_tree.cu.h"
#include "fs_debug.cu.h"


DEBUG_NOINLINE __device__	void hash_table::init_thread(volatile rtree* _t) volatile
	{
		for(int i=0;i<HT_MODULO;i++)
		{
			t[i].value=&_t[i];
			t[i].key=i;
			t[i].lock=0;
		}
		count=0;

	}
	// returns old value

DEBUG_NOINLINE __device__	void hash_table::reset(unsigned int key, volatile rtree* value) volatile
	{
		unsigned int n_key=key&HT_MODULO_MASK;
		t[n_key].value=value;
		reset_node(&t[n_key]);
	}
DEBUG_NOINLINE __device__	volatile rtree* hash_table::exchange(unsigned int key, volatile rtree* value, 
								     unsigned int * old_key  ) volatile
	{
		*old_key=0;
		unsigned int n_key=key&HT_MODULO_MASK;
		
		if (key!=t[n_key].key)
		{
			*old_key=t[n_key].key;
			t[n_key].value->lock_for_flush();
			t[n_key].value->traverse_all(-1,true,false,-1); // delete the old data, but no need to swap out
			t[n_key].value->unlock_after_flush();
		}
		volatile rtree* old=t[n_key].value;
		t[n_key].value=value;
		t[n_key].key=key;
		atomicAdd(((int*)&count),1);
		return old;
	}
		
DEBUG_NOINLINE __device__	volatile rtree* hash_table::get(int key) volatile
	{
		int n_key=key&HT_MODULO_MASK;
		if (t[n_key].key == key )
		{
			HT_HIT;
			return t[n_key].value;
		}else{
			HT_MISS;
			return NULL;
		}
	}

DEBUG_NOINLINE __device__ volatile hash_table_entry* hash_table::get_next(volatile hash_table_entry* it) volatile
	{
		
		if (!count) return NULL;
		if (!it) it=t;

		for( ; it<t+HT_MODULO;++it)
		{
			if (it->value->count)
			{
				return it;				
			}
		}
		return NULL;
	}


#endif
