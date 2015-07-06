#ifndef PROFILER_H
#define PROFILER_H

#include <stdio.h>
#include "clock_cat.h"

#ifdef __cplusplus
extern "C" {
#endif
#ifdef ENABLE_PROFILER
void profiler_output(FILE*);
int ev_start(clock_category cat, int socket);
int ev_stop(clock_category cat, int socket);

#define EV_START(x, y)	ev_start(x,y)
#define EV_STOP(x, y)	ev_stop(x,y)
#define PROFILER_OUTPUT(x) profiler_output(x)
#define EV_FLIPPER(start_ev, stop_ev, label) flipper ____this_flipper(start_ev, stop_ev, label);

#else

#define EV_START(x,y)
#define EV_STOP(x,y)
#define PROFILER_OUTPUT(x)	do { fprintf(x, "Profiler disabled\n"); } while (0)
#define EV_FLIPPER(start_ev, stop_ev, label)

#endif

#ifdef __cplusplus
};
#endif

class flipper {
public:
	flipper(clock_category start, clock_category stop, int soc)
		:start_(start), stop_(stop), socket_(soc)
	{
		EV_STOP(stop_, socket_);
		EV_START(start_, socket_);
	}
	~flipper()
	{
		EV_START(stop_, socket_);
		EV_STOP(start_, socket_);
	}
private:
	clock_category start_;
	clock_category stop_;
	int socket_;
};


#endif
