#ifndef STOPWATCH_H
#define STOPWATCH_H

#include "clock_cat.h"
#include <string>
#include <memory>
#include <vector>
#include <stdio.h>
#include <time.h>

using std::string;

class stopwatch {
private:
	stopwatch();
public:
	stopwatch(clock_category category_number, const string& name);
	~stopwatch();

	void pause();
	void start();
	void reset();

	std::string name() const { return name_; }
	clock_category category() const { return cat_; }

	double ms_sum() const;
	timespec isum() const;
protected:
	clock_category cat_;
	string name_;
	struct timespec sum_;
	struct timespec last_;
};

class journal : public stopwatch {
public:
	journal(clock_category category_number, const void* task_id);
	~journal();

	void pause();
	void start();

	void output(FILE*, int idx, const timespec& initial);
	timespec itime(int idx) const;
private:
	const void *task_id_;
	struct period {
		int processor_id;
		timespec start;
		timespec finish;
	};
	std::vector<period> log_;
};

class clockcenter {
public:
	typedef std::shared_ptr<stopwatch> watch_ref;
	typedef std::shared_ptr<journal>   journal_ref;

	static watch_ref get_stop_watch(clock_category cat, const std::string& name = std::string());
	static journal_ref get_perclient_journal(clock_category, const void* task);

	void printclocks(FILE*);
	void printperiods(FILE*);

	clockcenter();
	~clockcenter();

	static clockcenter& center();
private:
	std::vector<watch_ref> list_;
	std::vector<journal_ref> jlist_;
};

#define GET_JOURNAL(cat)	(clockcenter::get_perclient_journal(cat, this))
#define GET_JOURNAL_WITH_ID(cat, id)	(clockcenter::get_perclient_journal(cat, (void*)id))

extern const char* journal_fmtstr;

#endif
