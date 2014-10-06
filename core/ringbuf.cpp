#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

struct ringbuf{
	volatile int *vals;
	volatile int _head;
	volatile int _tail;
	volatile int _size;

	explicit ringbuf(int size){
		vals=new int[size];
		_size=size;
		_head=_tail=0;
	}
	~ringbuf(){delete []vals;}

	bool empty(){
		return (_tail==_head);
	}
	bool full(){
		return ((_head+1)%_size)==_tail;
	}

	int pop(bool &e){
		if (empty()) {e=0; return 0;}
		e=1;
		int newval=vals[_tail];
		__sync_synchronize();
		_tail=(_tail+1)%_size;
		__sync_synchronize();
		return newval;
	}
	bool push(int val){
		if (full()) return false;
		vals[_head]=val;
		__sync_synchronize();
		_head=(_head+1)%_size;
		__sync_synchronize();
		return true;
	}
};
#include <assert.h>

ringbuf r(1<<26);
/*
  int main(){
  ringbuf r(4);
  for (int z=0;z<100;z++){

  bool stat;
  r.pop(stat);
  assert(!stat);

  assert(r.push(1));
  printf("%d\n",r.pop(stat));
  assert(stat);

  assert(r.push(2));
  assert(r.push(3));

  printf("%d\n",r.pop(stat));
  assert(stat);

  assert(r.push(4));
  assert(r.push(5));

  assert(!r.push(6));

  printf("%d\n",r.pop(stat));
  assert(stat);
  printf("%d\n",r.pop(stat));
  assert(stat);
  printf("%d\n",r.pop(stat));
  assert(stat);
  r.pop(stat);
  assert(!stat);
  printf("============%d ===========\n", r._head);
  }
  return 0;
  }
*/
#include <time.h>
#define TOTAL_VALS (1<<30)

void* cons(void*)
{
	double empty_count=0;
	bool stat;
	int res;
	int prev=-1;
	int total=(TOTAL_VALS);
	struct timespec t;
	t.tv_sec=0;
	t.tv_nsec=1000;
	while(1){
		if (total==0) break;
		res=r.pop(stat);
		if (!stat) {nanosleep(&t,NULL); empty_count++;continue;}
		assert(res==prev+1);
		total--;
		prev=res;
	}
	printf("empty %.0f\n",empty_count);
	return NULL;
}


void* prod(void*)
{
	struct timespec t;
	t.tv_sec=0;
	t.tv_nsec=1000;
	bool stat;
	int total=(TOTAL_VALS);
	int prev=0;
	while(1){
		if (total==0) break;
		stat=r.push(prev);
		if (!stat) {nanosleep(&t,NULL);continue;}
		total--;
		prev++;
	}
	return NULL;
}

int main(){
	pthread_t t1,t2;
	assert(!pthread_create(&t1,NULL, prod,NULL));
	assert(!pthread_create(&t2,NULL, cons,NULL));
	pthread_join(t1,NULL);
	pthread_join(t2,NULL);
	return 0;
}
