#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>
#include <pthread.h>
#include <unordered_map>
#include <stack>

#define SHARED 0
#define EXCLUSIVE 1
// #define UPGRADABLE 2

typedef uint64_t pagenum_t;

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct lock_entry_t* sentinel;
    pthread_cond_t cond_var;
    int lock_mode;
    int record_id;
    struct lock_t* trx_next_lock;
    int owner_trx_id;
};

struct log_t {
    int64_t table_id;
    pagenum_t page_num;
    int64_t key;
    char old_value[112];
    char new_value[112];
};

struct lock_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
};

struct trx_entry_t {
    struct lock_t* head;
    int waits_for_trx_id;
    std::stack<log_t> logs;
};

extern std::unordered_map<int, trx_entry_t> trx_table;

int trx_begin();
int trx_commit(int trx_id);
int detect_deadlock(int trx_id);

int init_lock_table();
int shutdown_lock_table();
int lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif