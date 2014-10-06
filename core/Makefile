ifndef UTILS_DIR
  include $(shell pwd | sed 's/gpunet[/].*/gpunet/')/make.conf
endif

LIBGPUNETDIR?=.

LIBS=
CUDA_LIBS=-L$(CUDA_ROOT)/lib64 -lcudart
INFINIBAND_ROOT:=../../infiniband
CUDA_INCLUDE=-I$(CUDA_ROOT)/include -I$(CUDA_SDK_ROOT)/C/common/inc -I/usr/local/include

GENCODE_FLAGS := $(shell $(UTILS_DIR)/nvcc_option_gen.sh)

ifneq ($(dbg),1)
	MACROS=-DDEBUG_NOINLINE=""  -DMALLOC_STATS
	NVCC=$(CUDA_ROOT)/bin/nvcc $(GENCODE_FLAGS) $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  -maxrregcount 32 
else
	MACROS=-DDEBUG_NOINLINE="__noinline__"   -DMALLOC_STATS --debug -O0 -g 
	NVCC=$(CUDA_ROOT)/bin/nvcc $(GENCODE_FLAGS)  $(CUDA_INCLUDE) -Xptxas -v  $(MACROS)  --device-debug
endif

CC=g++ -std=c++0x $(CUDA_INCLUDE) -Wall -Wno-write-strings $(MACROS)  -g

SOURCES=net.o 

GPUNET_SOURCES=\
	      gpu_ipc.o\
	      gpunet.o\
	      ipc_shared.o\
	      net_globals.o\
	      net_structures.o\
	      net_private.o\
	      timer.o hostloop.o ipc_shared.o util.o

GPUUNIX_SOURCES=\
	      gpu_ipc.o\
	      gpuunix.o\
	      ipc_shared.o\
	      unixnet_globals.o\
	      unix_structures.o\
	      net_private.o\
	      timer.o hostloop.o ipc_shared.o util.o

GPUNET_PSRC=gpu_ipc.o\
	    gpunet.o\
	    ipc_shared.o\
	    net_globals.o\
	    net_structures.o\
	    net_private.o\
	    timer.o ipc_shared.o\
	    hostloopp.o\
	    stopwatch.o\
	    profiler.o util.o

GPUUNIX_PSRC=\
	      gpu_ipc.o\
	      gpuunix.o\
	      ipc_shared.o\
	      unixnet_globals.o\
	      unix_structures.o\
	      net_private.o\
	      timer.o\
	      ipc_shared.o\
	      hostloopp.o\
	      stopwatch.o\
	      profiler.o util.o

all:	libgpunet.a libgpuunix.a libgpunetP.a libgpuunixP.a libgpufs/libgpufs.a wrapfs.user/gpufs_lib.o sancheck


ifneq ($(dbg),1)
net: $(SOURCES) 
	$(NVCC) -link -o net net.o  -L$(LIBGPUNETDIR)  $(CUDA_LIBS) $(CUDA_INCLUDE) -lgpunet 
else
net.debug: $(SOURCES)
	$(NVCC) -link -o net.debug net.o -L$(LIBGPUNETDIR)   $(CUDA_LIBS) $(CUDA_INCLUDE) -lgpunetd 
endif


ifneq ($(dbg),1)
libgpunet.a : $(GPUNET_SOURCES)
	ar ru libgpunet.a $(GPUNET_SOURCES)

libgpunetP.a : $(GPUNET_PSRC)
	ar ru libgpunetP.a $(GPUNET_PSRC)

libgpuunix.a : $(GPUUNIX_SOURCES)
	ar ru libgpuunix.a $(GPUUNIX_SOURCES)

libgpuunixP.a : $(GPUUNIX_PSRC)
	ar ru libgpuunixP.a $(GPUUNIX_PSRC)

libgpufs/libgpufs.a:
	make -C libgpufs libgpufs.a

wrapfs.user/gpufs_lib.o:
	make -C wrapfs.user

else
libgpunetd.a: $(GPUNET_SOURCES)
	ar ru libgpunetd.a $(GPUNET_SOURCES)
endif

gpu_ipc.o: gpu_ipc.cu net_constants.h util.cu.h net_debug.h ipc_shared.h \
 gpu_ipc.cu.h net_structures.cu.h net_globals_add.cu.h
	$(NVCC) -dc gpu_ipc.cu

gpunet.o: gpunet.cu net_globals_add.cu.h net_constants.h ipc_shared.h \
 gpu_ipc.cu.h util.cu.h net_debug.h gpunet.cu.h net_structures.cu.h
	$(NVCC) -dc gpunet.cu

gpuunix.o: gpunet.cu net_globals_add.cu.h net_constants.h ipc_shared.h \
 gpu_ipc.cu.h util.cu.h net_debug.h gpunet.cu.h net_structures.cu.h
	$(NVCC) -o gpuunix.o -DDEFAULT_TO_UNIX_SOCKET -dc gpunet.cu

sancheck: util.cu util.cu.h sancheck.cu
	$(NVCC) -o sancheck sancheck.cu 

net_globals.o: net_globals.cu net_globals.h net_constants.h \
 net_structures.cu.h hostloop.h gpu_ipc.cu.h ipc_shared.h util.cu.h \
 net_debug.h net_private.h
	$(NVCC) -dc net_globals.cu

unixnet_globals.o: net_globals.cu net_globals.h net_constants.h \
 net_structures.cu.h hostloop.h gpu_ipc.cu.h ipc_shared.h util.cu.h \
 net_debug.h net_private.h
	$(NVCC) -DDEFAULT_TO_UNIX_SOCKET -DUSE_G_BUFFER_SPACE -o unixnet_globals.o -dc net_globals.cu

net_private.o: net_private.cc net_private.h net_constants.h
	$(CC) net_private.cc -c


net_structures.o: net_structures.cu net_structures.cu.h net_constants.h \
 net_globals_add.cu.h util.cu.h net_debug.h
	$(NVCC) -dc net_structures.cu

unix_structures.o: net_structures.cu net_structures.cu.h net_constants.h \
 net_globals_add.cu.h util.cu.h net_debug.h
	$(NVCC) -DDEFAULT_TO_UNIX_SOCKET -o unix_structures.o -dc net_structures.cu

net.o: net.cu $(PAYLOAD)
	$(NVCC) -dc net.cu

util.o: util.cu util.cu.h
	$(NVCC) -dc util.cu

timer.o: timer.cpp timer.h
	$(CC) timer.cpp -c

hostloop.o: hostloop.cpp ipc_shared.h net_constants.h net_globals.h \
 net_debug.h net_private.h
	$(CC) hostloop.cpp -c

hostloopp.o: hostloop.cpp ipc_shared.h net_constants.h net_globals.h \
 net_debug.h net_private.h profiler.h clock_cat.h
	$(CC) -DENABLE_PROFILER hostloop.cpp -c -o hostloopp.o

profiler.o: profiler.cc profiler.h _stopwatch.h clock_cat.h
	$(CC) -DENABLE_PROFILER profiler.cc -c

stopwatch.o: stopwatch.cc _stopwatch.h clock_cat.h
	$(CC) -DENABLE_PROFILER stopwatch.cc -c

ipc_shared.o: ipc_shared.cpp net_constants.h ipc_shared.h
	$(CC) ipc_shared.cpp -c

PHONY:  clean


clean:
	rm -f net net.debug *.o *.a

