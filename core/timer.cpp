#ifndef TIMER
#define TIMER
#include <sys/time.h>

double _timestamp(){
	struct timeval tv;
	gettimeofday(&tv,0);
	return 1e6*tv.tv_sec+tv.tv_usec;
}
#endif

