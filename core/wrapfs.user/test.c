#include "gpufs_user.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include "gpufs_lib.h"


#define gpufs_dir "/tmp/wrapfs/"
int main()
{
	int gpufs_dev_fd=0;
	int ret=0;
	gpufs_dev_fd=gpufs_open(GPUFS_DEV_NAME);

	if (gpufs_dev_fd<0)
	{
		perror("gpufs_open failed");
		return -1;
	}
	char filename[1024];
	sprintf(filename,"%s/test",gpufs_dir);
	
	int gpuid=4;
	char to_flash=0;
	int fd;
	
	/*
	gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_CREAT|O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}

	if (to_flash) fprintf(stderr,"opening results in flush\n");
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,1),ret);
	if (ret) return ret;

	// reopen the file again - 
	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,0),ret);
	if (ret) return ret;
	
	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	fprintf(stderr,"No flush was expected\n");

	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,0),ret);
	if (ret) return ret;

	// now open file on CPU
	fprintf(stderr,"Opening on CPU\n");
	int fd_cpu=open(filename,O_RDONLY);
	if (fd_cpu<0){ perror("CPU failed to open file on GPUFS");return -1;}
	close(fd_cpu);

	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	fprintf(stderr,"No flush was expected\n");
	
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,0),ret);
	if (ret) return ret;


	// now open file on CPU for write
	fprintf(stderr,"Opening on CPU for write\n");
	fd_cpu=open(filename,O_RDWR);
	if (fd_cpu<0){ perror("CPU failed to open file on GPUFS");return -1;}
	close(fd_cpu);

	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	fprintf(stderr,"*** Flush expected\n");
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,0),ret);

*/	
	// now open for read on two GPUs 
	gpuid=4;
	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_WRONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,0),ret);
	
	gpuid=1;
	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_RDONLY,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	fprintf(stderr,"*** Flush expected\n");
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,1),ret);

	fprintf(stderr,"Opening on CPU \n");
	int fd_cpu=open(filename,O_RDONLY);
	if (fd_cpu<0){ perror("CPU failed to open file on GPUFS");return -1;}
	close(fd_cpu);


	gpuid=4;
	fd=gpufs_file_open(gpufs_dev_fd,gpuid,filename,O_RDWR,0444,&to_flash);
	if (fd<0){
		perror("gpufs_file_open failed");
		return -1;
	}
	fprintf(stderr,"*** No flush expected \n");
	SAFE_CALL(gpufs_file_close(gpufs_dev_fd,gpuid,fd,1),ret);

	if (ret) return ret;


	SAFE_CALL(gpufs_close(gpufs_dev_fd),ret);
	return ret;
}
