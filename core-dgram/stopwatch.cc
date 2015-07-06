#include <sched.h>
#include <limits.h>
#include <map>
#include "_stopwatch.h"

double nowf(void);

/*
 * Stopwatch
 */
#define NANO 1000000000L
static timespec zerotime = {0, 0};
static timespec maxtime = {LONG_MAX, NANO};

static inline timespec now()
{
	struct timespec tp;
	clock_gettime(CLOCK_MONOTONIC, &tp);
	return tp;
}

static bool operator==(const timespec& lhs, const timespec& rhs)
{
	return lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec == rhs.tv_nsec;
}

static bool operator<(const timespec& lhs, const timespec& rhs)
{
	if (lhs.tv_sec < rhs.tv_sec)
		return true;
	if (lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec < rhs.tv_nsec)
		return true;
	return false;
}

static timespec operator+=(timespec& lhs, const timespec& rhs)
{
	lhs.tv_sec += rhs.tv_sec;
	lhs.tv_nsec += rhs.tv_nsec;
	if (lhs.tv_nsec >= NANO) {
		lhs.tv_nsec -= NANO;
		lhs.tv_sec++;
	}
	return lhs;
}

static timespec operator-(const timespec& lhs, const timespec& rhs)
{
	timespec ret = lhs;
	ret.tv_sec -= rhs.tv_sec;
	ret.tv_nsec -= rhs.tv_nsec;
	if (ret.tv_nsec < 0) {
		ret.tv_nsec += NANO;
		ret.tv_sec--;
	}
	return ret;
}

static double timespec2ms(const timespec& tp)
{
	return (double)tp.tv_sec*1000.0 + ((double)tp.tv_nsec/1e6);
}

struct starting_time {
public:
	starting_time()
	{
		t = now();
	}
	timespec t;
};

static starting_time st;

double nowf(void)
{
	return timespec2ms(now()-st.t);
}

stopwatch::stopwatch(clock_category category_number, const string& name)
	: cat_(category_number), name_(name), sum_(zerotime), last_(zerotime)
{
}

stopwatch::~stopwatch()
{
}

void stopwatch::pause()
{
	sum_ += now() - last_;
}

void stopwatch::start()
{
	last_ = now();
}

void stopwatch::reset()
{
	sum_ = zerotime;
}

double stopwatch::ms_sum() const
{
	return timespec2ms(sum_);
}

timespec stopwatch::isum() const
{
	return sum_;
}

journal::journal(clock_category category_number, const void* task_id)
	:stopwatch(category_number, std::string()), task_id_(task_id)
{
}

journal::~journal()
{
}

void journal::pause()
{
	/* stopwatch::pause(), but we need the value from now() */
	timespec n = now();
	sum_ += n  - last_;

	if (!log_.empty()) {
		period& p = log_.back();
		p.finish = n;
	}
}

void journal::start()
{
	stopwatch::start();
	log_.emplace_back();
	period& p = log_.back();
	p.processor_id = sched_getcpu();
	p.start = last_;
}

timespec journal::itime(int idx) const
{
	if ((size_t)idx >= log_.size()) 
		return zerotime;
	if (log_[idx].start == zerotime || log_[idx].finish == zerotime)
		return zerotime;
	return log_[idx].start;
}

const char* cat2str(clock_category cat);

const char* journal_fmtstr =
	"PROCESSOR %d FROM %f TO %f WOKING ON CAT %d REQUEST FROM CLIENT %p, COMMENT: %s\n";

void journal::output(FILE* f, int idx, const timespec& initial)
{
	double offset = timespec2ms(initial);
	fprintf(f, journal_fmtstr,
			log_[idx].processor_id,
			timespec2ms(log_[idx].start) - offset,
			timespec2ms(log_[idx].finish) - offset,
			(int)cat_,
			task_id_,
			cat2str(cat_));
}

/*
 * Clock center stuff
 */

static const char* catname[] = {
	"Receive",
	"Inqueue(recv)",
	"Send",
	"Inqueue(send)",
	"Upload",
	"Download",
	"Compute"
};

static_assert(sizeof(catname) >= CLOCK_CATEGORY_LAST*sizeof(char*),
		"THERE IS UNDEFINED CATEGORY NAME");

#define CATSTR(x) case x:		\
	return catname[x];		\


const char* cat2str(clock_category cat)
{
	if (cat < CLOCK_CATEGORY_LAST)
		return catname[cat];
	switch (cat) {
	default:
		return "UNKNOW CATEGORY";
	};
}

clockcenter::watch_ref
clockcenter::get_stop_watch(clock_category category_number, const std::string& name)
{
	watch_ref ref(new stopwatch(category_number, name));
	center().list_.push_back(ref);
	return ref;
}

clockcenter::journal_ref
clockcenter::get_perclient_journal(clock_category cat, const void* task)
{
	journal_ref ref(new journal(cat, task));
	center().jlist_.push_back(ref);
	center().list_.push_back(ref);
	return ref;
}

struct clockstat {
	int count;
	timespec sum;

	clockstat()
		:count(0), sum(zerotime)
	{
	}
};

void clockcenter::printclocks(FILE* f)
{
	std::map<clock_category, clockstat> statmap;
	for(auto iter = list_.begin();
		 iter != list_.end();
	 	 iter++) {
		watch_ref ref = *iter;
		clockstat& stat = statmap[ref->category()];
		stat.count++;
		stat.sum += ref->isum();
#if 0
		if (ref->category() == RECEIVE_INQUEUE_TIME)
			fprintf(f, "%s sample %f\n", cat2str(ref->category()), ref->ms_sum());
#endif
	}
	for(auto iter = statmap.begin();
		iter != statmap.end();
		iter++) {
		clock_category cat = iter->first;
		const clockstat& stat = iter->second;
		fprintf(f, "Category %d: %s, Average time %f\n",
				cat,
				cat2str(cat),
				timespec2ms(stat.sum)/(double)stat.count);
	}

	fflush(f);
}

void clockcenter::printperiods(FILE* f)
{
	timespec initial = zerotime;
	std::vector<int> positions;
	positions.resize(jlist_.size());
	do {
		timespec earliest = maxtime;
		int idx = -1;
		for(size_t i = 0; i < jlist_.size(); i++) {
			timespec it = jlist_[i]->itime(positions[i]);
			if (it == zerotime)
				continue;
			if (it < earliest) {
				earliest = it;
				idx = i;
			}
		}
		if (initial == zerotime)
			initial = earliest;
		if (idx < 0)
			break;
		jlist_[idx]->output(f, positions[idx]++, initial);
	} while (true);
	fflush(f);
}

clockcenter::clockcenter()
{
}

clockcenter::~clockcenter()
{
}

clockcenter& clockcenter::center()
{
	static clockcenter c;
	return c;
}
