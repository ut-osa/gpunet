#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/device.h>
#include <linux/cdev.h>
#include <linux/types.h>
#include <linux/poll.h>
#include <linux/wait.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/sched.h>

#include <asm/uaccess.h>/* for put_user */

#include "gpu_usermap.h"

MODULE_AUTHOR("OSA");
MODULE_DESCRIPTION("Infiniband and CUDA GPUDirect");
MODULE_LICENSE("GPL");

#define perror(FMT, ARGS...) printk(KERN_ERR "gpu_usermap %s:%d " FMT, __FUNCTION__, __LINE__, ## ARGS)

static struct class *gpu_usermap_class;
struct gpu_usermap_drv* gpu_usermap_drv;

#define GPU_USERMAP_DEV_COUNT   32

#define GPU_PAGE_SHIFT   16
#define GPU_PAGE_SIZE    ((u64)1 << GPU_PAGE_SHIFT)
#define GPU_PAGE_OFFSET  (GPU_PAGE_SIZE-1)
#define GPU_PAGE_MASK    (~GPU_PAGE_OFFSET)

/* 
 * declarations of internal functions
 */
int gpu_usermap_init(void);
void gpu_usermap_cleanup(void);

static int gpu_usermap_open(struct inode *, struct file *);
static int gpu_usermap_release(struct inode *, struct file *);
static ssize_t gpu_usermap_read(struct file *, char *, size_t, loff_t *);
static ssize_t gpu_usermap_write(struct file *, const char *, size_t, loff_t *);
static int gpu_usermap_mmap(struct file *filp, struct vm_area_struct *vma);
/* 
 * gpu_usermap file operations
 */
static struct file_operations gpu_usermap_fops = {
    .read  = gpu_usermap_read,
    .write = gpu_usermap_write,
    .open  = gpu_usermap_open,
	.mmap = gpu_usermap_mmap,
    .release = gpu_usermap_release
};

static void destroy_gpu_usermap_obj(struct kref *kref) {
    struct gpu_usermap_obj *obj = container_of(kref, struct gpu_usermap_obj, refcount);
    kfree(obj);
}

static int gpu_usermap_open(struct inode *inode, struct file *filp) {
    int ret = 0;
    struct gpu_usermap_obj *obj = kzalloc(sizeof(*obj), GFP_KERNEL);
    if (!obj) {
        return -ENOMEM;
    }

    /* initialize obj */
    mutex_init(&obj->mutex);
    kref_init(&obj->refcount);
	INIT_LIST_HEAD(&obj->vmalist);

    filp->private_data = obj;

    return ret;
}

static int gpu_usermap_release(struct inode *inode, struct file *filp) {
    int ret = 0;
    struct gpu_usermap_obj *obj = (struct gpu_usermap_obj*)filp->private_data;

	ret = nvidia_p2p_put_pages(0, 0, (uint64_t)obj->gpu_addr_begin,
							   obj->page_table);
    
    kref_put(&obj->refcount, destroy_gpu_usermap_obj);
    
    return ret;
}

static 
ssize_t gpu_usermap_read(struct file *filp, char *buf, size_t len, loff_t * offset) {
    return len;
}

static void gpu_usermap_dummy_callback(void *data)
{
	struct gpu_usermap_obj *obj = (struct gpu_usermap_obj *)data;
	int ret = 0;

	__module_get(THIS_MODULE);

	ret = nvidia_p2p_free_page_table(obj->page_table);
	if (ret) {
		perror("gpu_usermap_dummy_callback nvidia_p2p_free_page_table -- ret %d\n", ret);
	}

	module_put(THIS_MODULE);

	return;
}

static 
ssize_t gpu_usermap_write(struct file *filp, const char *buf, size_t len, loff_t * off) {
	struct gpu_usermap_req req;
	struct gpu_usermap_obj *priv = filp->private_data;
	int ret;
	
	if (len != sizeof(req))
		return -EINVAL;

	if (copy_from_user(&req, buf, len))
		return -EFAULT;

	if (req.magic != GUSERMAP_MAGIC) {
		printk(KERN_ERR "Magic value mismatch: %x (expected %x)\n",
			   req.magic, GUSERMAP_MAGIC);
		return -EINVAL;
	}
	
	// gpu_addr_begin/end are always aligned at GPU_PAGE_SIZE

	priv->p2p_token = req.p2p_token;
	priv->va_space_token = req.va_space_token;
	
	priv->gpu_addr = (void*)req.gpu_addr;
	priv->gpu_addr_begin = (void*)(req.gpu_addr & (~(GPU_PAGE_SIZE - 1)));
	priv->gpu_addr_end = (void*)((req.gpu_addr + req.len + GPU_PAGE_SIZE - 1) & (~(GPU_PAGE_SIZE - 1)));
	priv->len = ((char*)priv->gpu_addr_end - (char*)priv->gpu_addr_begin);

	ret = nvidia_p2p_get_pages(priv->p2p_token,
							   priv->va_space_token,
							   (uint64_t)priv->gpu_addr_begin, (uint64_t)priv->len,
							   &priv->page_table, gpu_usermap_dummy_callback, priv);
	if (ret < 0) {
		perror("gpu_usermap_write nvidia_p2p_get_pages ret: %d %p %lx\n", ret, priv->gpu_addr_begin, priv->len);
		return -EFAULT;
	} 

	/*printk(KERN_INFO "page_size: 0x%x, entries: %d\n",
		   (unsigned int)priv->page_table->page_size,
		   (int)priv->page_table->entries);*/
	
    return len;
}


static int gpu_usermap_fault(struct vm_area_struct *vma, struct vm_fault *vmf);
static void gpu_usermap_vm_open(struct vm_area_struct *vma);
static void gpu_usermap_vm_close(struct vm_area_struct *vma);

// I am not sure if pgfault handling is necessary, but nothing to lose with it.
static const struct vm_operations_struct gpu_usermap_vm_ops = {
	.fault = gpu_usermap_fault,
	.open = gpu_usermap_vm_open,
	.close = gpu_usermap_vm_close,
};

static int gpu_usermap_fault(struct vm_area_struct *vma, struct vm_fault *vmf) {
	struct gpu_usermap_obj *priv = vma->vm_file->private_data;

	resource_size_t offset = ((unsigned long)vmf->virtual_address - vma->vm_start) >> PAGE_SHIFT;
	struct page *page;

	page = pfn_to_page(priv->page_table->pages[offset]->physical_address >> PAGE_SHIFT);
	get_page(page);
	vmf->page = page;
	
	return 0;
}

static void gpu_usermap_vm_open(struct vm_area_struct *vma)
{
	struct gpu_usermap_obj *priv = vma->vm_file->private_data;
	
	struct gpu_usermap_vma_entry *vma_entry = kmalloc(sizeof(*vma_entry), GFP_KERNEL);
	
	atomic_inc(&priv->vma_count);

	if (vma_entry) {
		vma_entry->vma = vma;
		vma_entry->pid = current->pid;
		list_add(&vma_entry->head, &priv->vmalist);
	}
}

static void gpu_usermap_vm_close(struct vm_area_struct *vma)
{
	struct gpu_usermap_obj *priv = vma->vm_file->private_data;
	
	struct gpu_usermap_vma_entry *pt, *temp;

	atomic_dec(&priv->vma_count);
	list_for_each_entry_safe(pt, temp, &priv->vmalist, head) {
		if (pt->vma == vma) {
			list_del(&pt->head);
			kfree(pt);
			break;
		}
	}
}

static int gpu_usermap_mmap(struct file *filp, struct vm_area_struct *vma) {
	struct gpu_usermap_obj *priv = filp->private_data;
	unsigned long size = vma->vm_end - vma->vm_start;
	// gpu_offset is offset in a GPU page, and cpu_offset is offset in a vma
	unsigned long gpu_offset, cpu_offset = 0;
	int i = 0;
	int ret;

	vma->vm_flags |= (VM_IO | VM_LOCKED | VM_RESERVED);

	// non-cached. 
	vma->vm_page_prot = pgprot_noncached(vma->vm_page_prot);

	// the mapping may start in the middle of GPU page
	gpu_offset = ((((uintptr_t)priv->gpu_addr) % GPU_PAGE_SIZE) & (~(PAGE_SIZE - 1)));

	while (size > 0) {
		unsigned long len = GPU_PAGE_SIZE - (gpu_offset % GPU_PAGE_SIZE);
		len = min(len, size);

		//printk(KERN_INFO "io_remap_pfn_range idx: %d, gpu offset: %lx cpu offset:%lx vma_addr: %lx, physaddr: %lx, size: %lx, end: 0x%lx", i, gpu_offset, cpu_offset, vma->vm_start, (unsigned long)priv->page_table->pages[i]->physical_address, len, (unsigned long int)(vma->vm_start + cpu_offset + len));
		
		if ((ret = io_remap_pfn_range(vma, vma->vm_start + cpu_offset,
									  (priv->page_table->pages[i]->physical_address + gpu_offset) >> PAGE_SHIFT,
									  len,
									  vma->vm_page_prot))) {
			printk(KERN_INFO "gpu_usermap_mmap io_remap_pfn_range i: %d returning %d", i, ret);
			return -EAGAIN;
		}
		
		gpu_offset = (gpu_offset + len) % GPU_PAGE_SIZE;
		cpu_offset += len;
		size -= len;
		i++;
	}
	vma->vm_ops = &gpu_usermap_vm_ops;

	return 0;
}

int gpu_usermap_init(void) {
    int ret = 0;
    dev_t dev;
    struct gpu_usermap_drv *drv;
    struct device* gpu_usermap_dev;

    /* creating class for /dev/ node creation */
    gpu_usermap_class = class_create(THIS_MODULE, "chardev");
    if (IS_ERR(gpu_usermap_class)) {
        printk(KERN_ERR "class creation failed\n" );
        return PTR_ERR(gpu_usermap_class);
    }

    /* system-wide gpu_usermap_drv initialization */
    drv = kzalloc(sizeof(*drv), GFP_KERNEL);
    if (!drv) {
        printk(KERN_ERR "kzalloc failed");
        ret = -ENOMEM;
        goto err_class;
    }

    drv->owner = THIS_MODULE;
    drv->name = "gpu_usermap";

    /* reserve major/minor numbers */
    ret = alloc_chrdev_region(&dev, 0, GPU_USERMAP_DEV_COUNT, drv->name);
    if (ret) {
        printk(KERN_ERR "register_chrdev failed with %d\n", ret);
        goto err_alloc;
    }

    drv->major = MAJOR(dev);
    drv->minor = MINOR(dev);

    /* creating a device in chardev class for the dev */
    gpu_usermap_dev = device_create(gpu_usermap_class, NULL, dev, NULL, drv->name);
    if(IS_ERR(gpu_usermap_dev)) {
        printk(KERN_ERR "device creation failed\n");
        ret = PTR_ERR(gpu_usermap_dev);
        goto err_alloc_chrdev;
    }
    
    /* initializing cdev obj */
    cdev_init(&drv->cdev, &gpu_usermap_fops);
    drv->cdev.owner = drv->owner;
    
    ret = cdev_add(&drv->cdev, dev, 1);
    if (ret) {
        printk(KERN_ERR "cdev_add failed with %d\n", ret);
        goto err_alloc_device;
    }

    gpu_usermap_drv = drv;
    printk(KERN_INFO "gpu_usermap_init done\n");
    
    return ret;

err_alloc_device:
    device_destroy(gpu_usermap_class, dev);
err_alloc_chrdev:
    unregister_chrdev_region(dev, GPU_USERMAP_DEV_COUNT);
err_alloc:
    kfree(drv);
err_class:
    class_destroy(gpu_usermap_class);

    return ret;
}

void gpu_usermap_cleanup(void) {
    dev_t dev;

    printk(KERN_INFO "gpu_usermap_cleanup entering");
    
    dev = MKDEV(gpu_usermap_drv->major, gpu_usermap_drv->minor);
    
    cdev_del(&gpu_usermap_drv->cdev);
    device_destroy(gpu_usermap_class, dev);
    unregister_chrdev_region(dev, GPU_USERMAP_DEV_COUNT);
    
    kfree(gpu_usermap_drv);
    gpu_usermap_drv = NULL;
    
    class_destroy(gpu_usermap_class);
    
    printk(KERN_INFO "gpu_usermap_cleanup done");
}

module_init(gpu_usermap_init);
module_exit(gpu_usermap_cleanup);
