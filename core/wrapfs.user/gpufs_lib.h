#ifndef GPUFS_LIB_H_
#define GPUFS_LIB_H_
#include <sys/types.h>

#define SAFE_CALL(func,ret) if (((ret)=(func))) fprintf(stderr,"%s:%d:%s failed with ret value %d\n",__FILE__,__LINE__,__func__,ret);

int gpufs_open(const char* gpufs_dev);
int gpufs_close(int gpufs_dev_fd);

int gpufs_file_open(int gpufs_dev_fd, int gpuid, /* */
		const char *pathname, int flags, mode_t mode, char* o_flush);


int gpufs_drop_residence(int gpufs_dev_fd, int gpuid, unsigned long g_inode);

int gpufs_file_close(int gpufs_dev_fd, int gpuid, int file_fd, char drop_residence);

#define GPUFS_DEV_NAME "/dev/gpufs"

#endif
