#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <unordered_map>

#include "buffer.h"
#include "log.h"

#define verbose 0

#define SHARED      0
#define EXCLUSIVE   1

#define INIT_BIT(n)     (1UL << (n))
#define GET_BIT(m, n)   (((m) >> (n)) & 1U)
#define SET_BIT(m, n)   ({ (m) |= (1UL << (n)); })

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
    uint64_t last_LSN;
};

struct pair_hash {
    std::size_t operator()(const std::pair<int64_t, pagenum_t>& pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<pagenum_t>()(pair.second);
    }
};

int init_lock_table();
int shutdown_lock_table();

int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);
void trx_rollback(int trx_id);

int trx_get_trx_id();
void trx_set_trx_id(int trx_id);
int trx_is_active(int trx_id);
uint64_t trx_get_last_LSN(int trx_id);
void trx_set_last_LSN(int trx_id, uint64_t last_LSN);
void trx_resurrect_entry(int trx_id);
void trx_remove_entry(int trx_id);

int lock_acquire(int64_t table_id, pagenum_t page_num, int idx, int trx_id, int lock_mode, page_t** p);
lock_t* lock_alloc(int64_t table_id, pagenum_t page_num, int idx, int trx_id, int lock_mode);
int detect_deadlock(int trx_id);
int lock_release(lock_t* lock_obj);

void print_waits_for_graph(int num = 8);
void* print_locks(void* args);

#endif
