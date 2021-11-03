#include "lock_table.h"

struct lock_t {
    lock_t* prev_lock;
    lock_t* next_lock;
    // Sentinel pointer
    pthread_cond_t cond_var;
};

typedef struct lock_t lock_t;
pthread_mutex_t lock_table_latch;
std::unordered_map<std::tuple<int, int>, struct {lock_t* head, lock_t* tail}> lock_table;

int init_lock_table() {
    return 0;
}

lock_t* lock_acquire(int table_id, int64_t key) {
    pthread_mutex_lock(&lock_table_latch);

    pthread_mutex_unlock(&lock_table_latch);
    return nullptr;
};

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}
