#include "profiler.h"
#include "_stopwatch.h"
#include <map>

void profiler_output(FILE* f)
{
	fprintf(f, "Trying to output profiling data\n");
	clockcenter::center().printclocks(f);
	clockcenter::center().printperiods(f);
}

typedef clockcenter::journal_ref journal_ref;

static std::map<int, journal_ref> clockmaps[CLOCK_CATEGORY_LAST];

static journal_ref getclock(clock_category cat, int socket, bool create_if_not_exist = true)
{
	std::map<int, journal_ref>::iterator iter = clockmaps[cat].find(socket);
	if (iter == clockmaps[cat].end()) {
		if (!create_if_not_exist)
			return journal_ref();
		clockmaps[cat][socket] = GET_JOURNAL_WITH_ID(cat, socket);
	}
	return clockmaps[cat][socket];
}

int ev_start(clock_category cat, int socket)
{
#ifdef ENABLE_PROFILER
	journal_ref ref = getclock(cat, socket);
	ref->start();
#endif
	return 0;
}

int ev_stop(clock_category cat, int socket)
{
#ifdef ENABLE_PROFILER
	journal_ref ref = getclock(cat, socket, false);
	if (ref)
		ref->pause();
#endif
	return 0;
}
