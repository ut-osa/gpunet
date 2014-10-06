everything: crctable crc sse4crc.a

sse4crc.a: sse4crc.o
	ar r sse4crc.a sse4crc.o 

crc: crctable.out crc.cu crc.hpp sse4crc.h sse4crc.o
	nvcc -O2 -o crc --generate-code code=sm_35,arch=compute_35 crc.cu sse4crc.o

sse4crc.o: sse4crc.c sse4crc.h
	gcc -std=c99 -mtune=native -msse4.2 -O2 -c sse4crc.c

crctable.out: crctable
	./crctable

crctable: crctable.c crcmodel.h crcmodel.o
	gcc -o crctable -O3 crctable.c crcmodel.o

crcmodel.o: crcmodel.c crcmodel.h
	gcc -c crcmodel.c
