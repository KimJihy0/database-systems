#ifndef DB_TRX_H_
#define DB_TRX_H_

#include "buffer.h"
#include <unordered_map>
#include <stack>

#define SHARED      0
#define EXCLUSIVE   1

#define ACTIVE      0
#define COMMITTED   1
#define ABORTED     2

#define INIT_BIT(n)     (1UL << (n))
#define GET_BIT(num,n)  (((num) >> (n)) & 1U)
#define SET_BIT(num,n)  ({ (num) |= (1UL << (n)); })

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct lock_entry_t* sentinel;
    pthread_cond_t cond_var;
    struct lock_t* trx_next_lock;
    int lock_mode;
    int owner_trx_id;
    uint64_t bitmap;
};

struct lock_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
};

struct trx_entry_t {
    struct lock_t* head;
    int waits_for_trx_id;
    int trx_state;
};

extern std::unordered_map<int, trx_entry_t*> trx_table;

int init_lock_table();
int shutdown_lock_table();

int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);

int lock_acquire(int64_t table_id, pagenum_t page_num,
                 int idx, int trx_id, int lock_mode);
lock_t* lock_alloc(int64_t table_id, pagenum_t page_num,
                   int idx, int trx_id, int lock_mode);
int detect_deadlock(int trx_id);
int lock_release(lock_t* lock_obj);

#endif