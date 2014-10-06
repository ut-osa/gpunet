#ifndef GPUFS_USER_H_
#define GPUFS_USER_H_

#include <asm-generic/ioctl.h>

// open flag for GPU
#define O_GPU 020000000
#define GPUFS_DEV_NAME "/dev/gpufs"

#define GPUFS_DEV_IOC_MAGIC 'g'
#define GPUFS_DEV_IOCTL_SET_OWNER _IOWR(GPUFS_DEV_IOC_MAGIC, 1, int*)
#define GPUFS_DEV_IOCTL_DROP_OWNER _IOWR(GPUFS_DEV_IOC_MAGIC, 2, int)


struct gpufs_dev_ioctl_struct
{
	/* input */
	char 		input_is_owner; // ownership requested ( only if writing ).
	int  		input_gpuid; // which id
	unsigned long 	input_inode; // file inode
	/* output */
	int  		output_ownerid; // previous owner
};

#endif
