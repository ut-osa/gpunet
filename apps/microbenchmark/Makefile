ifndef UTILS_DIR
  include $(shell pwd | sed 's/gpunet[/].*/gpunet/')/make.conf
endif

CU_BINS = gpu_socket_bw_server gpu_socket_bw_client
BINS = cpu_socket_bw_client cpu_socket_bw_server 

CUDA_INC_PATH   ?= $(CUDA_ROOT)/include
CUDA_BIN_PATH   ?= $(CUDA_ROOT)/bin

LIBGPUNET_DIR=$(GPUNET_DIR)/core
LIBGPUFS_DIR=$(LIBGPUNET_DIR)/libgpufs
WRAPFS_DIR=$(LIBGPUNET_DIR)/wrapfs.user
LIBRDMACM_DIR=$(GPUNET_DIR)/core/rsocket

LIBRDMACM_AR=${LIBRDMACM_DIR}/src/.libs/librdmacm_osa.a

CU_LDFLAGS   += -L$(LIBGPUNET_DIR) -L/$(CUDA_ROOT)/lib64 -L$(LIBGPUNET_DIR) -lgpunet -L $(LIBGPUFS_DIR) -lgpufs $(WRAPFS_DIR)/gpufs_lib.o -L$(CUDA_ROOT)/lib64 ${LIBRDMACM_AR} -libverbs -lcuda
LDFLAGS=${LIBRDMACM_AR} -libverbs -L$(CUDA_ROOT)/lib64 -lcudart -lcuda

LIBGPUNET_SRCS=$(wildcard $(LIBGPUNET_DIR)/*.cpp) $(wildcard $(LIBGPUNET_DIR)/*.cu) $(wildcard $(LIBGPUNET_DIR)/*.h)
LIBGPUFS_SRCS=$(wildcard $(LIBGPUFS_DIR)/*.cpp) $(wildcard $(LIBGPUFS_DIR)/*.cu) $(wildcard $(LIBGPUFS_DIR)/*.h)
LIBRDMACM_SRCS=$(wildcard $(LIBRDMACM_DIR)/src/*.c) $(wildcard $(LIBRDMACM_DIR)/include/rdma/*.h) $(wildcard $(LIBRDMACM_DIR)/include/infiniband/*.h)
WRAPFS_SRCS=$(wildcard $(WRAPFS_DIR)/*.c)

CU_CFLAGS += -I$(CUDA_INC_PATH) -I$(LIBGPUNET_DIR) -I$(LIBGPUFS_DIR) -I$(WRAPFS_DIR)


LINK_DEPENDENCIES += $(LIBGPUNET_DIR)/libgpunet.a $(LIBGPUFS_DIR)/libgpufs.a ${LIBRDMACM_AR}
CU_LINK_DEPENDENCIES += $(LIBGPUNET_DIR)/libgpunet.a $(LIBGPUFS_DIR)/libgpufs.a ${LIBRDMACM_AR}

COMPILE_DEPENDENCIES += common.h microbench_util_cpu.h
CU_COMPILE_DEPENDENCIES += common.h microbench_util.h

cpu_multisocket_server.o_DEP += common_async.h
cpu_multisocket_client.o_DEP += common_async.h
cpu_socket_bw_server_duplex.o_DEP += common_async.h
cpu_socket_bw_client_duplex.o_DEP += common_async.h

gpu_socket_bw_server_timing += common_timing.h

include ../../utils/app/make.frag

$(LIBGPUNET_DIR)/libgpunet.a: $(LIBGPUNET_SRCS) ${LIBRDMACM_AR}
	cd $(LIBGPUNET_DIR); make

$(LIBGPUFS_DIR)/libgpufs.a: $(LIBGPUFS_SRCS)
	cd $(LIBGPUFS_DIR); make

$(WRAPFS_DIR)/gpufs_lib.o: $(WRAPFS_SRCS)
	cd $(WRAPFS_DIR); make

${LIBRDMACM_AR}: $(LIBRDMACM_SRCS)
	cd $(LIBRDMACM_DIR); make
