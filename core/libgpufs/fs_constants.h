#ifndef FS_CONSTANTS
#define FS_CONSTANTS

/** radix tree constants (radix_tree.cu)*/
#define MAX_LEVELS 4
#define MAX_RT_NODES 5000

// home many ages to swapout at once
#define MIN_PAGES_SWAPOUT (16)

// number of leaves per node - either 128 or 64
#define RT_16_LEAVES

#ifdef RT_64_LEAVES
#define NUM_LEAVES 64
#define LOGNUM_LEAVES 6
#define MASK 0x3F
#endif

#ifdef RT_128_LEAVES
#define NUM_LEAVES 128
#define LOGNUM_LEAVES 7
#define MASK 0x7F
#endif

#ifdef RT_16_LEAVES
#define NUM_LEAVES 16
#define LOGNUM_LEAVES 4
#define MASK 0xF
#endif

// memory pool constants (fs_structures.cu)

#ifndef REDEFINE_BLOCKSIZE
//#define FS_BLOCKSIZE (1024*1024*2)
//#define FS_LOGBLOCKSIZE (21)

#define FS_LOGBLOCKSIZE (20)
#define FS_BLOCKSIZE (1<<FS_LOGBLOCKSIZE)

#endif



//#define PPOOL_SIZE (1024*1024*1024*2L)
#define PPOOL_SIZE (512*1024*1024)
#define PPOOL_FRAMES (PPOOL_SIZE>>FS_LOGBLOCKSIZE)

	

// FS constants 
#define MAX_BLOCKS_PER_FILE (1<<(LOGNUM_LEAVES*MAX_LEVELS))
#define MAX_NUM_FILES (64)
// must be power of 2
#define MAX_NUM_CLOSED_FILES (1024)

#define MAX_NUM_PRECLOSE_FILES (32)

//** ERROR CODES **//
#define E_FSTABLE_FULL -1
#define E_IPC_OPEN_ERROR -2

//** OPEN CLOSE
#define FSTABLE_SIZE MAX_NUM_FILES
#define FILENAME_SIZE 64 // longest filename string 

#define FSENTRY_EMPTY 	0
#define FSENTRY_PENDING	1
#define FSENTRY_OPEN	2
#define FSENTRY_CLOSING	3

//** CPU IPC 
#define CPU_IPC_EMPTY 0
#define CPU_IPC_PENDING 1
#define CPU_IPC_READY 2

//** CPU IPC R/W TABLE
#define RW_IPC_SIZE 64

#define RW_IPC_READ 0
#define RW_IPC_WRITE 1
#define RW_IPC_DIFF 2
#define RW_IPC_TRUNC 3

#define IPC_MGR_EMPTY 0
#define IPC_MGR_BUSY 1
#define IPC_TYPE_READ 0
#define IPC_TYPE_WRITE 1

#include<fcntl.h>

#define O_GRDONLY (O_RDONLY)
#define O_GWRONLY (O_WRONLY)
#define O_GCREAT (O_CREAT)
#define O_GRDWR (O_RDWR)
#define O_GWRONCE (O_GCREAT|O_GWRONLY)

#define GMAP_FAILED MAP_FAILED


#define PAGE_READ_ACCESS 0
#define PAGE_WRITE_ACCESS 1
typedef char Page[FS_BLOCKSIZE];

typedef unsigned char uchar;

#endif
