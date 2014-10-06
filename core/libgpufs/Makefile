ifndef UTILS_DIR
  include $(shell pwd | sed 's/gpunet[/].*/gpunet/')/make.conf
endif

GPUFSLIB=../wrapfs.user
LIBS=
#CUDA_SDK_ROOT=/
CUDA_LIBS=-L$(CUDA_ROOT)/lib64 -lcudart $(GPUFSLIB)/gpufs_lib.o /usr/local/librdmacm_osa.a
CUDA_INCLUDE=-I$(CUDA_ROOT)/include -I$(CUDA_SDK_ROOT)/C/common/inc -I$(GPUFSLIB)

BLOCK_SIZES=-DREDEFINE_BLOCKSIZE -DFS_BLOCKSIZE="(16*1024*1024)" -DFS_LOGBLOCKSIZE=24


#NVCC=nvcc --maxrregcount 24 --gpu-architecture=sm_21 $(CUDA_INCLUDE)
#NVCC=nvcc  --gpu-architecture=sm_21 $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  -maxrregcount 32 -I../../gpu/ -O2

GENCODE_FLAGS   := $(shell $(UTILS_DIR)/nvcc_option_gen.sh)

ifneq ($(dbg),1)
	MACROS=-DDEBUG_NOINLINE="" -DRELEASE=1 -O2 -DMALLOC_STATS
#	NVCC=$(CUDA_ROOT)/bin/nvcc  --gpu-architecture=sm_21 $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  -maxrregcount 32 
	NVCC=$(CUDA_ROOT)/bin/nvcc   $(GENCODE_FLAGS) $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  -maxrregcount 32
else
	MACROS=-DDEBUG_NOINLINE="__noinline__"   -DMALLOC_STATS --debug -O0 -g 
	NVCC=$(CUDA_ROOT)/bin/nvcc  $(GENCODE_FLAGS) $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  --device-debug
endif

CC=g++ $(CUDA_INCLUDE) -Wall -Wno-write-strings $(MACROS) 

#PAYLOAD:=
#bigmatrix_mmap.cu 


SOURCES=fs.o 

GPUFS_SOURCES=cpu_ipc.o\
fs_calls.o\
 fs_initializer.o\
 fs_structures.o\
 hash_table.o\
 mallocfree.o\
 preclose_table.o\
 radix_tree.o\
 swapper.o\
timer.o\
fs_debug.o

all:	libgpufs.a
	

timer.o: timer.cpp timer.h
	$(CC) timer.cpp -c

ifneq ($(dbg),1)
fs: $(SOURCES) 
	$(NVCC) -link -o fs $(SOURCES)  -L/home/marks/home/gpufs/workloads/libgpufs -lgpufs  $(CUDA_LIBS)
else
fs: $(SOURCES)
	$(NVCC) -link -o fs $(SOURCES)  -L/home/marks/home/gpufs/workloads/libgpufs -lgpufsd  $(CUDA_LIBS)
	cp fs fs.debug
endif


ifneq ($(dbg),1)
libgpufs.a : $(GPUFS_SOURCES)
	ar ru libgpufs.a $(GPUFS_SOURCES)
else
libgpufsd.a: $(GPUFS_SOURCES)
	ar ru libgpufsd.a $(GPUFS_SOURCES)
endif

fs.o: fs.cu  $(PAYLOAD)
	$(NVCC) -dc fs.cu -I/home/marks/home/gpufs/workloads/libgpufs

cpu_ipc.o: cpu_ipc.cu fs_constants.h \
    util.cu.h  fs_debug.cu.h fs_structures.cu.h radix_tree.cu.h cpu_ipc.cu.h
	$(NVCC) -dc cpu_ipc.cu

fs_calls.o: fs_calls.cu fs_debug.cu.h fs_constants.h  util.cu.h  cpu_ipc.cu.h \
    fs_structures.cu.h  mallocfree.cu.h  timer.h   hash_table.cu.h
	$(NVCC) -dc fs_calls.cu
fs_initializer.o: fs_initializer.cu fs_debug.cu.h fs_constants.h  util.cu.h  cpu_ipc.cu.h \
    fs_structures.cu.h  mallocfree.cu.h  timer.h   hash_table.cu.h \
	radix_tree.cu.h preclose_table.cu.h fs_initializer.cu.h
	$(NVCC) -dc fs_initializer.cu

hash_table.o: hash_table.cu hash_table.cu.h fs_constants.h util.cu.h fs_debug.cu.h radix_tree.cu.h fs_structures.cu.h
	$(NVCC) -dc hash_table.cu

mallocfree.o: mallocfree.cu fs_constants.h util.cu.h mallocfree.cu.h fs_structures.cu.h swapper.cu.h
	$(NVCC) -dc mallocfree.cu

preclose_table.o: preclose_table.cu.h preclose_table.cu util.cu.h fs_debug.cu.h fs_constants.h
	$(NVCC) -dc preclose_table.cu

radix_tree.o: radix_tree.cu radix_tree.cu.h fs_constants.h swapper.cu.h util.cu.h fs_debug.cu.h fs_structures.cu.h
	$(NVCC) -dc radix_tree.cu

swapper.o: swapper.cu.h swapper.cu fs_structures.cu.h fs_constants.h hash_table.cu.h util.cu.h fs_debug.cu.h radix_tree.cu.h
	$(NVCC)	-dc swapper.cu

fs_structures.o: swapper.cu.h swapper.cu fs_structures.cu.h fs_constants.h hash_table.cu.h util.cu.h fs_debug.cu.h radix_tree.cu.h
	$(NVCC)	-dc fs_structures.cu

fs_debug.o: fs_debug.cu fs_debug.cu.h
	$(NVCC) -dc fs_debug.cu
#fs_nofs: fs_nofs.cu 
#	$(NVCC) fs_nofs.cu  timer.cpp -o fs_nofs
#fs_nofs.debug: fs_nofs.cu
#	$(NVCC) fs_nofs.cu -o fs_nofs.debug --device-debug=3  --debug -O0 


#fs: $(SOURCES)
#	$(NVCC) fs.cu -o fs.$(BLOCKSIZE) -DDEBUG_NOINLINE="" -DRELEASE=1 -O2  -DREDEFINE_BLOCKSIZE -DFS_BLOCKSIZE=$(BLOCKSIZE) -DFS_LOGBLOCKSIZE=$(LOGBLOCKSIZE) -DMALLOC_STATS
#	$(NVCC) fs.cu -o fs -DDEBUG_NOINLINE="" -DRELEASE=1 -O2 -DMALLOC_STATS $(MAXREGCOUNT)

PHONY:  clean

debug:
#	$(NVCC) fs.cu -o fs.debug --device-debug=3  --debug -O0 -g -DDEBUG_NOINLINE="__noinline__" -DMALLOC_STATS  -DREDEFINE_BLOCKSIZE -DFS_BLOCKSIZE=$(BLOCKSIZE) -DFS_LOGBLOCKSIZE=$(LOGBLOCKSIZE)
#fs.debug: $(SOURCES)
	$(NVCC) fs.cu -o fs.debug --device-debug=3  --debug -O0 -g -DDEBUG_NOINLINE="__noinline__" -DMALLOC_STATS

clean:
	rm -f fs fs.debug *.o *.a

