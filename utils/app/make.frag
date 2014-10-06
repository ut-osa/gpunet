ifndef UTILS_DIR
  include $(shell pwd | sed 's/gpu-net[/].*/gpu-net/')/make.conf
endif

NVCC = nvcc

SRCS += $(sort $(wildcard *.c)) 
CC_SRCS += $(sort $(wildcard *.cc))

CU_SRCS += $(sort $(wildcard *.cu))
OBJS += $(patsubst %.c,%.o,$(SRCS)) $(patsubst %.cc,%.o,$(CC_SRCS))
CU_OBJS += $(patsubst %.cu,%.cu.obj,$(CU_SRCS))

CU_CFLAGS += -m64 -maxrregcount 32 -dc -g -DDEBUG_NOINLINE="" -DRELEASE=1 -DMALLOC_STATS

CU_LDFLAGS   += -lcudart -lrt -lpthread 


# GENCODE_SM10    := -gencode arch=compute_10,code=sm_10
# GENCODE_SM20    := -gencode arch=compute_20,code=sm_21
# GENCODE_SM30    := -gencode arch=compute_30,code=sm_30
# GENCODE_SM35    := -gencode arch=compute_35,code=sm_35
GENCODE_FLAGS   := $(shell $(UTILS_DIR)/nvcc_option_gen.sh)

ifeq ($(GENCODE_FLAGS),-gencode arch=compute_35,code=sm_35)
	CU_LDFLAGS += -lcudadevrt
endif

#BINS += $(patsubst %.cc,%,$(SRCS))

all: $(BINS) $(CU_BINS)
	@echo "DONE"

define BIN_template 
  $(1)_OBJS += $(1).o
  
  ifneq (,$(wildcard $(1).c))
    $(1).o_SRC += $(1).c
    $(1)_CC = gcc
  endif

  ifneq (,$(wildcard $(1).cc))
    $(1).o_SRC += $(1).cc
    $(1)_CC = g++
  endif

  $(1): $$($(1)_OBJS) $$($(1)_DEP) $(LINK_DEPENDENCIES)
	@echo "Linking $(1)" from  $$($(1)_OBJS) 
	@$$($(1)_CC) $(CFLAGS) $$($(1)_CFLAGS) -o $$(@) $$($(1)_OBJS) $$($(1)_LIBS) $(LDFLAGS) $$($(1)_LDFLAGS) $(LIBS)
endef

define CU_BIN_template 
  $(1)_OBJS += $(1).cu.obj
  $(1).cu.obj_SRC += $(1).cu

  $(1): $$($(1)_OBJS) $$($(1)_DEP) $(CU_LINK_DEPENDENCIES)
	@echo "Linking $(1)" from $$($(1)_OBJS) $$($(1)_LIBS) 
	@$(NVCC) -Xnvlink -v $(GENCODE_FLAGS) -link -o $$(@) $$($(1)_OBJS) $(CU_LDFLAGS) $$($(1)_LDFLAGS) $$($(1)_LIBS) $(LIBS) 
endef

define OBJ_template 
  $(1): $$($(1)_SRC) $$($(1)_DEP) $(COMPILE_DEPENDENCIES)
	@echo "Compiling $(1) from " $$($(1)_SRC)
	@$(CC) -c $(CFLAGS) $$($(1)_CFLAGS) -o $$(@) $$($(1)_SRC) $(LIBS)
endef

define CU_OBJ_template 
  $(1): $$($(1)_SRC) $$($(1)_DEP) $(CU_COMPILE_DEPENDENCIES)
	@echo "Compiling $(1)"
	@$(NVCC) $(CU_CFLAGS) $(GENCODE_FLAGS) $$($(1)_CFLAGS) -o $$(@) $$($(1)_SRC)
endef

$(foreach bin,$(BINS),$(eval $(call BIN_template,$(bin))))
$(foreach obj,$(OBJS),$(eval $(call OBJ_template,$(obj))))
$(foreach cu_bin,$(CU_BINS),$(eval $(call CU_BIN_template,$(cu_bin))))
$(foreach cu_obj,$(CU_OBJS),$(eval $(call CU_OBJ_template,$(cu_obj))))

clean:
	rm -rf $(OBJS) $(BINS) $(CU_OBJS) $(CU_BINS)


#$(BINS): $$($$@_OBJS) $$($$@

.PHONY: all clean


# Local Variables:
# mode: Makefile
# End: