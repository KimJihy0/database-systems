#include "trx.h"

pthread_mutex_t lock_table_latch;
pthread_mutex_t trx_table_latch; 
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, pagenum_t> &pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<pagenum_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, pagenum_t>, lock_entry_t, pair_hash> lock_table;
std::unordered_map<int64_t, trx_entry_t> trx_table;
int64_t trx_id;

int trx_begin() {
    pthread_mutex_lock(&trx_table_latch);
    trx_id++;
    trx_entry_t* trx_entry = &(trx_table[trx_id]);
    trx_entry->head = (lock_t*)malloc(sizeof(lock_t));
    if (trx_entry->head == NULL) return 0;
    trx_entry->head->trx_next_lock = NULL;
    trx_entry->tail = trx_entry->head;
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}

int trx_commit(int trx_id) {
    // pthread_mutex_lock(&trx_table_latch);
    trx_entry_t* trx_entry = &(trx_table[trx_id]);
    lock_t* lock_obj = trx_entry->head->trx_next_lock;
    while (lock_obj != NULL) {
        if (lock_release(lock_obj) != 0) return 0;
        lock_obj = lock_obj->trx_next_lock;
    }
    free(trx_entry->head);
    trx_table.erase(trx_id);
    // pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}

int init_lock_table() {
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    trx_table_latch = PTHREAD_MUTEX_INITIALIZER;
    trx_id = 0;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
    pthread_mutex_lock(&lock_table_latch);

    lock_entry_t* lock_entry = &(lock_table[{table_id, page_id}]);
    trx_entry_t* trx_entry = &(trx_table[trx_id]);

    lock_t* lock_obj = (lock_t*)malloc(sizeof(lock_t));
    if (lock_obj == NULL) return NULL;
    lock_obj->prev_lock = lock_entry->tail;
    lock_obj->next_lock = NULL;
    lock_obj->sentinel = lock_entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;
    lock_obj->lock_mode = lock_mode;
    lock_obj->key = key;
    lock_obj->trx_next_lock = NULL;
    lock_obj->owner_trx_id = trx_id;

    if (lock_entry->head == NULL) {
        lock_entry->head = lock_obj;
        lock_entry->tail = lock_obj;
    }
    else {
        lock_entry->tail->next_lock = lock_obj;
        lock_entry->tail = lock_obj;
    }

    trx_entry->tail->trx_next_lock = lock_obj;
    trx_entry->tail = lock_obj;

    // sleep

    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
}

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    lock_entry_t* lock_entry = lock_obj->sentinel;
    trx_entry_t* trx_entry = &(trx_table[lock_obj->owner_trx_id]);

    // wake up someone

    if (lock_obj->prev_lock != NULL) {
        lock_obj->prev_lock->next_lock = lock_obj->next_lock;
    }
    else {
        lock_entry->head = lock_obj->next_lock;
    }
    if (lock_obj->next_lock != NULL) {
        lock_obj->next_lock->prev_lock = lock_obj->prev_lock;
    }
    else {
        lock_entry->tail = lock_obj->prev_lock;
    }

    free(lock_obj);

    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}



/* ---To do.---
 * 해시 - djb2?
 * record_id == key?
 * trx_begin()에서 return trx_id가 critical area 밖에 있음
 * trx_entry_t 없애고 단순한 linked-list로 수정
 * head, tail -> dummy node 가능?
 * 
 * ---Done.---
 * 
 */ 