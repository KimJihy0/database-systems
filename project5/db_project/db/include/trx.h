#ifndef DB_TRX_H_
#define DB_TRX_H_

#include "buffer.h"
#include <pthread.h>
#include <unordered_map>
#include <stack>

#define verbose     0

#define SHARED      0
#define EXCLUSIVE   1

#define ACTIVE      0
#define COMMITTED   1
#define ABORTED     2

// #define GET_BIT(num,n) ((num >> n) & 1U)
// #define SET_BIT(num,n) ({ num |= 1UL << n; })

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct lock_entry_t* sentinel;
    pthread_cond_t cond_var;
    int lock_mode;
    int record_id;
    struct lock_t* trx_next_lock;
    int owner_trx_id;
    // uint64_t lock_bitmap;
    // uint64_t wait_bitmap;
};

struct log_t {
    log_t(int64_t table_id, pagenum_t page_num, uint16_t offset, uint16_t size) :
        table_id(table_id), page_num(page_num), offset(offset), size(size) {}
    int64_t table_id;
    pagenum_t page_num;
    uint16_t offset;
    uint16_t size;
    char old_value[108];
    char new_value[108];
};

struct lock_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
};

struct trx_entry_t {
    struct lock_t* head;
    int waits_for_trx_id;
    int trx_state;
    std::stack<log_t> logs;
};

extern std::unordered_map<int, trx_entry_t*> trx_table;
extern pthread_mutex_t trx_latch;

int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);
int detect_deadlock(int trx_id);

int init_lock_table();
int shutdown_lock_table();
int lock_acquire(int64_t table_id, pagenum_t page_num, int64_t key,
                 int idx, int trx_id, int lock_mode);
int lock_attach(int64_t table_id, pagenum_t page_num, int64_t key,
                int idx, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

void print_waits_for_graph();
void* print_locks(void* args);

#endif