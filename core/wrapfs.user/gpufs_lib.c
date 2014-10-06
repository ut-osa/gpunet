#include "gpufs_user.h"
#include "gpufs_lib.h"


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <errno.h>
#include <string.h>


int gpufs_open(const char* gpufs_dev)
{
	int gpufs_fd=open(gpufs_dev,O_RDWR);
	return gpufs_fd;
}
int gpufs_close(int gpufs_dev_fd)
{
	return close(gpufs_dev_fd);
}

int gpufs_file_open(int gpufs_dev_fd, int gpuid, /* */
		const char *pathname, int flags, mode_t mode, char* o_flush)
{
	int ret=0;
	assert(pathname&&o_flush);
	struct stat inode_stat;
	int file_not_exist=0;
	if (stat(pathname,&inode_stat)) {
		if (errno!=ENOENT) return -1;
		file_not_exist=1;
	}
	
	int fd=open(pathname,flags|O_GPU,mode);

	if (fd<0) return -1;
	
	SAFE_CALL(fstat(fd,&inode_stat),ret);
	if (ret) return -1;

			
	struct gpufs_dev_ioctl_struct s={
		.input_is_owner= ((flags&O_WRONLY)|| (flags&O_RDWR)) >0,
		.input_gpuid=gpuid,
		.input_inode=inode_stat.st_ino,
		.output_ownerid=gpuid
	};

	SAFE_CALL(ioctl(gpufs_dev_fd,GPUFS_DEV_IOCTL_SET_OWNER, &s),ret) 
	if (ret){
		close(fd);
		if (file_not_exist) unlink(pathname);
		return ret;
	}

	*o_flush=(s.output_ownerid != s.input_gpuid);
	fprintf(stderr,"file %s opened for gpu %d as %s of inode %lu, prev state is %s \n",
			pathname, s.input_gpuid, (s.input_is_owner?"owner":"NOT owner"),
			s.input_inode, *o_flush?"flushed":"NOT flushed");
	return fd;
}

int gpufs_drop_residence(int gpufs_dev_fd, int gpuid, unsigned long g_inode)
{
	struct gpufs_dev_ioctl_struct s={
		.input_is_owner=0,
		.input_gpuid=gpuid,
		.input_inode=g_inode,
		.output_ownerid=-5
	};
	int ret=0;
	SAFE_CALL(ioctl(gpufs_dev_fd,GPUFS_DEV_IOCTL_DROP_OWNER,&s),ret);
	return ret;
	

}

int gpufs_file_close(int gpufs_dev_fd, int gpuid, int file_fd, char drop_residence)
{
	int ret=0;
	if (drop_residence) {
		struct stat s;
		SAFE_CALL(fstat(file_fd,&s),ret);
		if (ret) goto fail;
		fprintf(stderr,"dropping residence\n");	
		SAFE_CALL( gpufs_drop_residence(gpufs_dev_fd, gpuid, s.st_ino),ret);
		if (ret) goto fail;
	}
fail:
	if (ret) close(file_fd);
	else ret=close(file_fd);
	return ret;
}


