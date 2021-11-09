#include "lock_table.h"

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct entry_t* sentinel;
    pthread_cond_t cond_var;
};

struct entry_t {
    struct lock_t* tail;
    struct lock_t* head;
};

pthread_mutex_t lock_table_latch;
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, int64_t> &pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<int64_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, int64_t>, entry_t, pair_hash> lock_table;

int init_lock_table() {
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key) {
    pthread_mutex_lock(&lock_table_latch);

    entry_t* entry = &(lock_table[{table_id, key}]);

    lock_t* lock_obj = (lock_t*)malloc(sizeof(lock_t));
    if (lock_obj == NULL) return NULL;
    lock_obj->sentinel = entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;

    if (entry->tail != NULL) {
        entry->tail->next_lock = lock_obj;
        lock_obj->prev_lock = entry->tail;
        entry->tail = lock_obj;
        lock_obj->next_lock = NULL;
        pthread_cond_wait(&(lock_obj->cond_var), &lock_table_latch);
    }
    else {
        entry->head = lock_obj;
        lock_obj->prev_lock = NULL;
        entry->tail = lock_obj;
        lock_obj->next_lock = NULL;
    }

    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
}

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    entry_t* entry = lock_obj->sentinel;

    if (lock_obj->next_lock != NULL) {
        entry->head = lock_obj->next_lock;
        lock_obj->next_lock->prev_lock = NULL;
        pthread_cond_signal(&(entry->head->cond_var));
    }
    else {
        entry->head = NULL;
        entry->tail = NULL;
    }

    free(lock_obj);

    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}

/* ---To do---
 * entry_t 중복
 * NULL vs. nullptr
 */