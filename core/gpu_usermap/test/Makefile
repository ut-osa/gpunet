ifndef UTILS_DIR
  include $(shell pwd | sed 's/gpunet[/].*/gpunet/')/make.conf
endif

CU_BINS = test_gpu_usermap test_gpu_usermap_perf test_gpu_usermap_ringbuf test_gpu_usermap_ringbuf2

CUDA_INC_PATH   ?= $(CUDA_ROOT)/include
CUDA_BIN_PATH   ?= $(CUDA_ROOT)/bin

LIBGPUNET_DIR=$(GPUNET_DIR)/gpuapi-branches/sangmank

CU_LDFLAGS   += -L/$(CUDA_ROOT)/lib64 /usr/local/lib/librdmacm_osa.a -lcuda
LDFLAGS=-L$(CUDA_ROOT)/lib64 -lcudart

CU_CFLAGS += -I$(CUDA_INC_PATH) -I$(LIBGPUNET_DIR) -I/usr/local/include

LINK_DEPENDENCIES += 
CU_LINK_DEPENDENCIES += 

include $(UTILS_DIR)/app/make.frag
