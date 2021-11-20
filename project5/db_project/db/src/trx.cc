#include "trx.h"

pthread_mutex_t lock_mng_latch;
pthread_mutex_t trx_mng_latch; 
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, pagenum_t> &pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<pagenum_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, pagenum_t>, lock_entry_t, pair_hash> lock_table;
std::unordered_map<int, trx_entry_t> trx_table;
int trx_id;

int trx_begin() {
    pthread_mutex_lock(&trx_mng_latch);
    int ret_val = ++trx_id;
    pthread_mutex_unlock(&trx_mng_latch);
    return ret_val;
}

int trx_commit(int trx_id) {
    lock_t* lock_obj = trx_table[trx_id].head;
    lock_t* del_obj;
    while (lock_obj != NULL) {
        if (lock_release(lock_obj) != 0) return 0;
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        free(del_obj);
    }
    trx_table.erase(trx_id);
    return trx_id;
}

int detect_deadlock(int trx_id) {
    pthread_mutex_lock(&trx_mng_latch);
    int cur_trx_id = trx_table[trx_id].waits_for_trx_id;
    while (cur_trx_id != 0 && cur_trx_id != trx_id) {
        cur_trx_id = trx_table[cur_trx_id].waits_for_trx_id;
    }
    pthread_mutex_unlock(&trx_mng_latch);
    return cur_trx_id;
}

int init_lock_table() {
    pthread_mutex_init(&lock_mng_latch, 0);
    pthread_mutex_init(&trx_mng_latch, 0);
    return 0;
}

int shutdown_lock_table() {
    pthread_mutex_destroy(&lock_mng_latch);
    pthread_mutex_destroy(&trx_mng_latch);
    return 0;
}

int lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
    pthread_mutex_lock(&lock_mng_latch);

    lock_entry_t* lock_entry = &(lock_table[{table_id, page_id}]);

    lock_t* lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (lock_obj->owner_trx_id == trx_id) {
            pthread_mutex_unlock(&lock_mng_latch);
            if (lock_obj->lock_mode == SHARED && lock_mode == EXCLUSIVE) return -1;
            return 0;
        }
        lock_obj = lock_obj->next_lock;
    }

    lock_obj = new lock_t;
    lock_obj->prev_lock = lock_entry->tail;
    lock_obj->next_lock = NULL;
    lock_obj->sentinel = lock_entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;
    lock_obj->lock_mode = lock_mode;
    lock_obj->record_id = key;
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

    trx_entry_t* trx_entry = &(trx_table[trx_id]);

    lock_obj->trx_next_lock = trx_entry->head;
    trx_entry->head = lock_obj;

    lock_t* cur_obj = lock_entry->head;
    while (cur_obj != lock_obj) {
        cur_obj = lock_entry->head;
        while (cur_obj != lock_obj) {
            if (cur_obj->record_id == key &&
                (cur_obj->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE)) {
                trx_entry->waits_for_trx_id = cur_obj->owner_trx_id;
                if (detect_deadlock(trx_id) != 0) {
                    trx_entry->waits_for_trx_id = 0;
                    pthread_mutex_unlock(&lock_mng_latch);
                    return -1;
                }
                pthread_cond_wait(&(cur_obj->cond_var), &lock_mng_latch);
                break;
            }
            cur_obj = cur_obj->next_lock;
        }
    }
    trx_entry->waits_for_trx_id = 0;
    pthread_mutex_unlock(&lock_mng_latch);
    return 0;
}

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_mng_latch);

    lock_entry_t* lock_entry = lock_obj->sentinel;

    if (lock_obj->prev_lock != NULL) {
        lock_obj->prev_lock->next_lock = lock_obj->next_lock;
    }
    else lock_entry->head = lock_obj->next_lock;
    if (lock_obj->next_lock != NULL) {
        lock_obj->next_lock->prev_lock = lock_obj->prev_lock;
    }
    else lock_entry->tail = lock_obj->prev_lock;

    pthread_cond_broadcast(&(lock_obj->cond_var));

    pthread_mutex_unlock(&lock_mng_latch);
    return 0;
}



/* ---To do---
 * pathname?????
 * upgradable lock(U_lock)
 * lock_acquire()에서 deadlock detection해도 되는지. NULL return 가능한지.
 * detection 주기, 위치
 * logging 필요? 필요하다면 꼭 파일에 해야 하는지
 * waits_for_trx_id : 1개인지.
 * max_trx_id
 * LRU -> head, tail로 구현 가능한지 확인.
 * trx_abort()가 index manager에 있어서 찝찝한 상태.
 * rollback시 value 확인
 * trx_abort()에서 relock 가능성 다분
 * 
 * ---Done---
 * mattr 전역변수로
 * NUM_BUFS 1일 때 에러나야 함.
 * if (p_buffer_idx != -1) 일단 지워놨음.
 * relock시 latch.__data.__lock value 확인.
 * 그냥 최근거를 abort
 * trx_begin()에서 return trx_id가 critical area 밖에 있음
 * hash - djb2?
 * db_find(), db_update()에서 abort.
 * policy 똑바로
 * wake up 위치 안 내려도 되는지.
 * record_id == key?
 * trx_entry_t 없애고 simple linked-list로 수정
 * head, tail -> dummy node 가능?
 * 
 */ 
