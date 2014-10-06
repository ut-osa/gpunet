#ifndef PRECLOSE_NODE_CU
#define PRECLOSE_NODE_CU
#include "fs_constants.h"
#include "preclose_table.cu.h"
#include "util.cu.h"
#include "fs_globals.cu.h"


	__device__ void preclose_table::init_thread() volatile
	{
		_lock=0;
		size=0;
		bzero_thread(entries,sizeof(preclose_node)*MAX_NUM_PRECLOSE_FILES);
	}

	__device__ int preclose_table::findEntry(volatile char*filename, volatile FTable_entry* _new_f, volatile OTable_entry* _new_o) volatile
	{	int found=0;
		for(int i=0;i<MAX_NUM_PRECLOSE_FILES &&found<size ;i++){
			if(entries[i].occupied)
			{
				found ++;
				if (strcmp_thread(filename,entries[i].o.filename, FILENAME_SIZE) == 0){
					_new_f->deepcopy(&entries[i].f);
					_new_o->copy_old_data(&entries[i].o);
					entries[i].occupied=0;
					size--;
					PRECLOSE_FETCH;
					return 0;
				}
			}
		}
		return 1;
	}
	
	__device__ int preclose_table::add(volatile FTable_entry* _old_f, volatile OTable_entry* _old_o) volatile
	{
		for(int i=0;i<MAX_NUM_PRECLOSE_FILES;i++){
			if(entries[i].occupied==0){
				size++;
				entries[i].occupied=1;
				entries[i].f.deepcopy(_old_f);
				entries[i].o.copy_old_data(_old_o);
				PRECLOSE_PUSH;
				return 0;
			}
		}
		return 1;
	}
	
	
#endif
