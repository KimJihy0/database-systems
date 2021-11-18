#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <unordered_map>

#define verbose 1

typedef struct lock_t lock_t;

int trx_begin();
int trx_commit(int trx_id);
int detect_deadlock(int trx_id);

/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(int64_t table_id, int64_t key, int trx_id);
int lock_release(lock_t* lock_obj);

void print_waits_for_graph();

#endif /* __LOCK_TABLE_H__ */
