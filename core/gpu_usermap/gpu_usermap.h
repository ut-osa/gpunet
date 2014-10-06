#ifndef __IBSVR_H__
#define __IBSVR_H__

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/cdev.h>
#include <linux/types.h>

#include "nv-p2p.h"

/* system-wide settings for gpu_usermap */
struct gpu_usermap_drv {
    struct cdev cdev;
    struct module *owner;
    const char *name;
    
    int major;
    int minor;
};

extern struct gpu_usermap_drv* gpu_usermap_drv;

/* process-wide settings for gpu_usermap */
struct gpu_usermap_obj {
    struct kref refcount;
    struct mutex mutex;
	void *gpu_addr;
	void *gpu_addr_begin;
	void *gpu_addr_end;
	size_t len;
	struct nvidia_p2p_page_table *page_table;
	struct list_head vmalist;
	atomic_t vma_count;

	uint64_t p2p_token;
	uint32_t va_space_token;
};


struct gpu_usermap_vma_entry {
	struct list_head head;
	struct vm_area_struct *vma;
	pid_t pid;
};

#include "gpu_usermap_abi.h"

#endif
