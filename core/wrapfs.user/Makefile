OBJ=gpufs_lib.o test.o
CPPFLAGS=-I../libgpufs/

CC=gcc

all: gpufs_lib.o

gpufs_lib.o: gpufs_lib.c gpufs_lib.h

test.o: test.cpp gpufs_lib.h

test: gpufs_lib.o test.o 
	$(CXX) -o test gpufs_lib.o test.o

clean:
	rm -f *.o
