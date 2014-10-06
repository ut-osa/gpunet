#ifndef HOST_LOOP_CPP
#define HOST_LOOP_CPP

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "gpufs_lib.h"

void fd2name(const int fd, char* name, int namelen) {

  char slink[100];
  pid_t me = getpid();
  name[0] = 0;
  sprintf(slink, "/proc/%d/fd/0", me);

  int s = readlink(slink, name, namelen - 1);
  if (s >= 0)
    name[s] = '\0';
}

double transfer_time = 0;
void open_loop(volatile GPUGlobals* globals, int gpuid) {
  char* use_gpufs_lib = getenv("USE_GPUFS_DEVICE");

  for (int i = 0; i < FSTABLE_SIZE; i++) {
    char filename[FILENAME_SIZE];
    volatile CPU_IPC_OPEN_Entry* e = &globals->cpu_ipcOpenQueue->entries[i];
    // we are doing open
    if (e->status == CPU_IPC_PENDING && e->cpu_fd < 0) {
      memcpy(filename, (char*) e->filename, FILENAME_SIZE);
      // OPEN
      if (e->flags & O_GWRONCE) {
        e->flags = O_RDWR | O_CREAT;
      }
      char pageflush = 0;
      int cpu_fd = 0;

      if (use_gpufs_lib) {  //cpu_fd=gpufs_file_open(globals->gpufs_fd,gpuid,filename,e->flags,S_IRUSR|S_IWUSR,&pageflush);
      } else {
        cpu_fd = open(filename, e->flags, S_IRUSR | S_IWUSR);
      }

      if (cpu_fd < 0) {
        fprintf(stderr, "Problem with opening file %s on CPU: %s \n ", filename,
                strerror(errno));
      }

      struct stat s;
      if (fstat(cpu_fd, &s)) {
        fprintf(stderr, "Problem with fstat the file %s on CPU: %s \n ",
                filename, strerror(errno));
      }

      //fprintf(stderr, "FD %d,  inode %ld, size %ld, Found file %s\n",i, s.st_ino, s.st_size, filename);

      e->cpu_fd = cpu_fd;
      e->flush_cache = pageflush;
      e->cpu_inode = s.st_ino;
      e->size = s.st_size;
      __sync_synchronize();
      e->status = CPU_IPC_READY;
      __sync_synchronize();
    }
    if (e->status == CPU_IPC_PENDING && e->cpu_fd >= 0) {
      // do close
      // fprintf(stderr, "FD %d, closing file %s\n",i, e->filename);
      if (use_gpufs_lib) {
        //e->cpu_fd=gpufs_file_close(globals->gpufs_fd,gpuid,e->cpu_fd);
        //gpufs_drop_residence(globals->gpufs_fd, gpuid, e->drop_residence_inode);
      } else {
        e->cpu_fd = close(e->cpu_fd);
      }
      __sync_synchronize();
      e->status = CPU_IPC_READY;
      __sync_synchronize();
    }

  }
}

int max_req = 0;
int report = 0;
void rw_loop(volatile GPUGlobals* globals) {
  char* no_pci = getenv("GPU_NOPCI");
  if (no_pci && !report)
    fprintf(stderr,
            "Warning: no data will be transferred in and out of the GPU\n");

  char* no_files = getenv("GPU_NOFILE");
  if (no_files && !report)
    fprintf(stderr, "Warning: no file reads/writes will be performed\n");
  report = 1;
  int cur_req = 0;

  for (int i = 0; i < RW_IPC_SIZE; i++) {
    volatile CPU_IPC_RW_Entry* e = &globals->cpu_ipcRWQueue->entries[i];
    if (e->status == CPU_IPC_PENDING) {
/*
      cur_req++;
      		fprintf(stderr, "FD %d, cpu_fd %d, buf_offset %d, size "
       "%d, type %s, ret_val %d\n",i,
       e->cpu_fd,
       e->buffer_offset,
       e->size,
       e->type==RW_IPC_READ?"read":"write",
       e->return_value
       );
*/
      int req_cpu_fd = e->cpu_fd;
      size_t req_buffer_offset = e->buffer_offset;
      size_t req_file_offset = e->file_offset;
      size_t req_size = e->size;
      int req_type = e->type;
      assert(
          req_type == RW_IPC_READ || req_type == RW_IPC_WRITE || req_type == RW_IPC_DIFF || req_type == RW_IPC_TRUNC);
      if (req_type != RW_IPC_TRUNC) {
        assert(req_cpu_fd>=0 && req_size>0);
      }

      if (globals->streamMgr->task_array[i] != -1) {
        // we only need to check the stream
        cudaError_t cuda_status = cudaStreamQuery(
            globals->streamMgr->memStream[i]);

        if (cudaErrorNotReady == cuda_status) {
          // rush to the next request, this one is not ready
          //
          //fprintf(stderr,".");
          continue;
        }
        if (cuda_status != cudaSuccess) {
          fprintf(stderr, "Error in the host loop.\n ");
          cudaError_t error = cudaDeviceSynchronize();
          fprintf(stderr, "%d Device failed, CUDA error message is: %s\n\n",__LINE__,
                  cudaGetErrorString(error));
          exit(-1);
        }

        // we are here only if success
      }

      switch (req_type) {
        case RW_IPC_READ: {
          // read
          int cpu_read_size = 0;
          if (globals->streamMgr->task_array[i] == -1)
          // the request only started to be served
              {
            if (!no_files) {
              transfer_time -= _timestamp();
              cpu_read_size = pread(req_cpu_fd, globals->streamMgr->scratch[i],
                                    req_size, req_file_offset);
              transfer_time += _timestamp();
            } else {
              cpu_read_size = req_size;
            }
            char fname[256];
            if (cpu_read_size < 0) {
              fd2name(req_cpu_fd, fname, 256);
              fprintf(stderr, "Problem with reading file %s on CPU: %s \n ",
                      fname, strerror(errno));
            }
            if (cpu_read_size != req_size) {
              //fprintf(stderr, "Read %d required %d from offset %d on CPU\n ",
              //        cpu_read_size, req_size, req_file_offset);
            }
            if (cpu_read_size == 0) {
              //fprintf(stderr, "Nothing has been read\n");
            }

            e->return_value = cpu_read_size;

            if (cpu_read_size > 0) {
              globals->streamMgr->task_array[i] = req_type;
              if (!no_pci) {
                CUDA_SAFE_CALL(
                    cudaMemcpyAsync(
                        ((char*) globals->rawStorage) + req_buffer_offset,
                        globals->streamMgr->scratch[i], cpu_read_size,
                        cudaMemcpyHostToDevice,
                        globals->streamMgr->memStream[i]));
              }
            }
          }
          // if read failed or we did not update cpu_read_size since we didn't take the previous if
          if (cpu_read_size <= 0) {
            // complete the request
            globals->streamMgr->task_array[i] = -1;
            __sync_synchronize();
            e->status = CPU_IPC_READY;
            __sync_synchronize();
          }

        }
          break;

        case RW_IPC_TRUNC:
          e->return_value = ftruncate(req_cpu_fd, 0);
          __sync_synchronize();
          e->status = CPU_IPC_READY;
          __sync_synchronize();
          break;
        case RW_IPC_DIFF: {
          if (globals->streamMgr->task_array[i] == -1) {
            globals->streamMgr->task_array[i] = req_type;  // enqueue
            if (!no_pci) {
              //						fprintf(stderr,"RW_IPC_DIFF buf_offset %llu, size %llu\n", req_buffer_offset, req_size);
              CUDA_SAFE_CALL(
                  cudaMemcpyAsync(
                      globals->streamMgr->scratch[i],
                      ((char*) globals->rawStorage) + req_buffer_offset,
                      req_size, cudaMemcpyDeviceToHost,
                      globals->streamMgr->memStream[i]));
            }

            struct stat s;
            if (fstat(req_cpu_fd, &s)) {
              char fname[256];
              fd2name(req_cpu_fd, fname, 256);
              fprintf(stderr, "Problem with fstat the file %s on CPU: %s \n ",
                      fname, strerror(errno));
            }
            // stat to get size. if smaller -> can't use mmap

            if (s.st_size < req_file_offset + req_size) {
              if (ftruncate(req_cpu_fd, req_file_offset + req_size)) {
                char fname[256];
                fd2name(req_cpu_fd, fname, 256);
                fprintf(stderr,
                        "Problem with ftruncate for file %s on CPU: %s \n ",
                        fname, strerror(errno));
              }
            }
          } else {
            globals->streamMgr->task_array[i] = -1;
            // request completion
            if (!no_files) {
              uchar* out = (uchar*) mmap(NULL, req_size, PROT_WRITE, MAP_SHARED,
                                         req_cpu_fd, req_file_offset);
              if (out == MAP_FAILED ) {
                perror("mmap failed");
                req_size = (size_t) -1;
              }

#if 0
              if (fstat(req_cpu_fd,&s)) assert("cant stat"==NULL);
              float* __d=(float*)out;
              fprintf(stderr,"empty regions: \n");
              int __start=0;
              for (int z=0;z<s.st_size/4;z++) {
                if (__d[z] ==0 && __start==0 ) {__start=z;};
                if (__d[z] != 0 && __start!=0 ) {fprintf(stderr,"EEE %d- %d\n",__start,z); __start=0;};
              }
              if (__start!=0) fprintf(stderr,"EEE %d-%d\n",__start,s.st_size/4);

              __d=(float*)scratch;
              fprintf(stderr,"writing into regions: \n");
              __start=0;
              for (int z=0;z<req_size/4;z++) {
                if (__d[z] !=0 && __start==0 ) {__start=req_file_offset/4+z;};
                if (__d[z] == 0 && __start!=0 ) {fprintf(stderr,"WWW %d- %d\n",__start,req_file_offset/4+z); __start=0;};
              }
              if (__start!=0) fprintf(stderr,"WWW %d-%d\n",__start,req_size/4);

#endif
//// END DEBUG
              for (int zzz = 0; zzz < req_size; zzz++) {
                uchar* tmp = globals->streamMgr->scratch[i];
                if (tmp[zzz]) {
                  out[zzz] = tmp[zzz];
                }
              }

              if (munmap(out, req_size)) {
                perror("munmap failed");
                req_size = (size_t) -1;
              }
            }  // end of no_files
            e->return_value = req_size;
            __sync_synchronize();
            e->status = CPU_IPC_READY;
            __sync_synchronize();
          }
        }
          break;
        case RW_IPC_WRITE: {
          if (globals->streamMgr->task_array[i] == -1) {
            if (!no_pci) {

              CUDA_SAFE_CALL(
                  cudaMemcpyAsync(
                      globals->streamMgr->scratch[i],
                      ((char*) globals->rawStorage) + req_buffer_offset,
                      req_size, cudaMemcpyDeviceToHost,
                      globals->streamMgr->memStream[i]));
            }
            globals->streamMgr->task_array[i] = req_type;  // enqueue
          } else {
            globals->streamMgr->task_array[i] = -1;  // compelte
            int cpu_write_size = req_size;
            if (!no_files) {
              cpu_write_size = pwrite(req_cpu_fd,
                                      globals->streamMgr->scratch[i], req_size,
                                      req_file_offset);
            }

            if (cpu_write_size < 0) {

              char fname[256];
              fd2name(req_cpu_fd, fname, 256);
              fprintf(stderr, "Problem with writing  file %s on CPU: %s \n ",
                      fname, strerror(errno));
            }
            if (cpu_write_size != req_size) {
              char fname[256];
              fd2name(req_cpu_fd, fname, 256);
              fprintf(stderr, "Wrote less than expected on CPU for file %s\n ",
                      fname);
            }
            e->return_value = cpu_write_size;
            __sync_synchronize();
            e->status = CPU_IPC_READY;
            __sync_synchronize();
          }
        }
          break;
        default:
          assert(NULL);
      }
    }
  }
  if (max_req < cur_req)
    max_req = cur_req;

}
#endif
