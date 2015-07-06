#include <assert.h>
#include "net_constants.h"
#include "ipc_shared.h"

void cpu_ipc_entry_init(cpu_ipc_entry* e)
{
	e->status=CPU_IPC_READY;
	e->cpu_sock=-1;
}

void cpu_ipc_queue_init(cpu_ipc_queue* q)
{
	for(int i=0;i<TASK_ARRAY_SIZE;i++)
	{
		cpu_ipc_entry_init(&q->entries[i]);
	}
}

