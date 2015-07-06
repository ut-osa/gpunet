#ifndef __GPU_USERMAP_ABI__
#define __GPU_USERMAP_ABI__

#define GUSERMAP_MAGIC 0xdeadbaaa

enum {
	GUSERMAP_REQ_MAP,
	GUSERMAP_REQ_UNMAP
};

struct gpu_usermap_req {
	uint32_t magic;
	uintptr_t gpu_addr; // for MAP
	uint64_t len;   // for MAP
	
	uint64_t p2p_token;
	uint32_t va_space_token;
};

#endif
