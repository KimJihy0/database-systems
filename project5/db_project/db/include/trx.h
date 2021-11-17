#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>
#include <pthread.h>
#include <unordered_map>

#define SHARED 0
#define EXCLUSIVE 1

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct lock_entry_t* sentinel;
    pthread_cond_t cond_var;
    int lock_mode;
    int64_t key;
    struct lock_t* trx_next_lock;
    int64_t owner_trx_id;
};

struct lock_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
};

struct trx_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
}

int trx_begin();
int trx_commit(int trx_id);

// signature 변경가능
int init_lock_table();
lock_t* lock_aquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif