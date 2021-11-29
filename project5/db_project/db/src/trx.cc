#include "trx.h"

pthread_mutex_t lock_latch;
pthread_mutex_t trx_latch;
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, pagenum_t>& pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<pagenum_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, pagenum_t>, lock_entry_t, pair_hash> lock_table;
std::unordered_map<int, trx_entry_t*> trx_table;
static int trx_id;

int init_lock_table() {
    if (pthread_mutex_init(&lock_latch, 0) != 0) return -1;
    if (pthread_mutex_init(&trx_latch, 0) != 0) return -1;
    return 0;
}

int shutdown_lock_table() {
    if (pthread_mutex_destroy(&lock_latch) != 0) return -1;
    if (pthread_mutex_destroy(&trx_latch) != 0) return -1;
    return 0;
}

int trx_begin() {
    pthread_mutex_lock(&trx_latch);
    int local_trx_id = ++trx_id;
    trx_table[trx_id] = new trx_entry_t;
    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = ACTIVE;
    pthread_mutex_unlock(&trx_latch);
    return local_trx_id;
}

int trx_commit(int trx_id) {
    if (trx_table[trx_id]->trx_state == ABORTED) return 0;
    pthread_mutex_lock(&lock_latch);

    lock_t* del_obj;
    lock_t* lock_obj = trx_table[trx_id]->head;
    while (lock_obj != NULL) {
        lock_release(lock_obj);
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }

    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = COMMITTED;

    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int trx_abort(int trx_id) {
    if (trx_table[trx_id]->trx_state == ABORTED) return 0;
    pthread_mutex_lock(&lock_latch);

    lock_t* del_obj;
    lock_t* lock_obj = trx_table[trx_id]->head;
    while (lock_obj != NULL) {
        lock_release(lock_obj);
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }

    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = ABORTED;

    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int lock_acquire(int64_t table_id, pagenum_t page_num,
                 int idx, int trx_id, int lock_mode) {
    pthread_mutex_lock(&lock_latch);

    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);
    lock_t* lock_obj;

    // duplicated lock
    lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (GET_BIT(lock_obj->bitmap, idx) != 0 &&
                lock_obj->owner_trx_id == trx_id &&
                lock_obj->lock_mode >= lock_mode) {
            pthread_mutex_unlock(&lock_latch);
            return 0;
        }
        lock_obj = lock_obj->next_lock;
    }

    // implicit locking
    lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (GET_BIT(lock_obj->bitmap, idx) != 0) break;
        lock_obj = lock_obj->next_lock;
    }
    if (lock_obj == NULL) {
        page_t* p;
        int p_buffer_idx = buffer_read_page(table_id, page_num, &p);
        pthread_mutex_lock(&trx_latch);
        int impl_trx_id = p->slots[idx].trx_id;
        if (trx_table[impl_trx_id] != NULL &&
                trx_table[impl_trx_id]->trx_state == ACTIVE) {
            if (impl_trx_id == trx_id) {
                pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
                pthread_mutex_unlock(&trx_latch);
                pthread_mutex_unlock(&lock_latch);
                return 0;
            }
            lock_alloc(table_id, page_num, idx, impl_trx_id, EXCLUSIVE);
        }
        else if (lock_mode == EXCLUSIVE) {
            p->slots[idx].trx_id = trx_id;
            buffer_write_page(table_id, page_num, &p);
            pthread_mutex_unlock(&trx_latch);
            pthread_mutex_unlock(&lock_latch);
            return 0;
        }
        pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
        pthread_mutex_unlock(&trx_latch);
    }

    // lock compression
    if (lock_mode == SHARED) {
        lock_obj = lock_entry->head;
        while (lock_obj != NULL) {
            if (lock_obj->lock_mode == SHARED && lock_obj->owner_trx_id == trx_id) break;
            lock_obj = lock_obj->next_lock;
        }
        if (lock_obj != NULL) {
            lock_t* cur_obj = lock_entry->head;
            while (cur_obj != NULL) {
                if (GET_BIT(cur_obj->bitmap, idx) != 0 &&
                        cur_obj->owner_trx_id != trx_id &&
                        cur_obj->lock_mode == EXCLUSIVE) {
                    break;
                }
                cur_obj = cur_obj->next_lock;
            }
            if (cur_obj == NULL) {
                SET_BIT(lock_obj->bitmap, idx);
                pthread_mutex_unlock(&lock_latch);
                return 0;
            }
        }
    }

    // lock allocation
    lock_obj = lock_alloc(table_id, page_num, idx, trx_id, lock_mode);

    // deadlock detection
    lock_t* cur_obj = lock_entry->head;
    while (cur_obj != lock_obj) {
        if (GET_BIT(cur_obj->bitmap, idx) != 0 &&
                cur_obj->owner_trx_id != trx_id &&
                (cur_obj->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE)) {
            trx_table[trx_id]->waits_for_trx_id = cur_obj->owner_trx_id;
            if (detect_deadlock(trx_id) == trx_id) {
                pthread_mutex_unlock(&lock_latch);
                return -1;
            }
            pthread_cond_wait(&(cur_obj->cond_var), &lock_latch);
            trx_table[trx_id]->waits_for_trx_id = 0;
            cur_obj = lock_entry->head;
            continue;
        }
        cur_obj = cur_obj->next_lock;
    }

    pthread_mutex_unlock(&lock_latch);
    return 0;
}

lock_t* lock_alloc(int64_t table_id, pagenum_t page_num,
                   int idx, int trx_id, int lock_mode) {
    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);

    lock_t* lock_obj = new lock_t;
    lock_obj->prev_lock = lock_entry->tail;
    lock_obj->next_lock = NULL;
    lock_obj->sentinel = lock_entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;
    lock_obj->trx_next_lock = trx_table[trx_id]->head;
    lock_obj->lock_mode = lock_mode;
    lock_obj->owner_trx_id = trx_id;
    lock_obj->bitmap = INIT_BIT(idx);

    if (lock_entry->head == NULL)
        lock_entry->head = lock_obj;
    else
        lock_entry->tail->next_lock = lock_obj;
    lock_entry->tail = lock_obj;
    trx_table[trx_id]->head = lock_obj;

    return lock_obj;
}

int detect_deadlock(int trx_id) {
    std::unordered_map<int, int> visit;
    do {
        visit[trx_id] = 1;
        trx_id = trx_table[trx_id]->waits_for_trx_id;
    } while (trx_id != 0 && !visit[trx_id]);
    return trx_id;
}

int lock_release(lock_t* lock_obj) {
    lock_entry_t* lock_entry = lock_obj->sentinel;

    if (lock_obj->prev_lock != NULL)
        lock_obj->prev_lock->next_lock = lock_obj->next_lock;
    else lock_entry->head = lock_obj->next_lock;
    if (lock_obj->next_lock != NULL)
        lock_obj->next_lock->prev_lock = lock_obj->prev_lock;
    else lock_entry->tail = lock_obj->prev_lock;

    pthread_cond_broadcast(&(lock_obj->cond_var));

    return 0;
}

/* ---To do---
 * trx_table에서 delete (abort, commit) (nullify, delete, and erase(clear?))
 * max_trx_id
 * bpt에서 trx_id 초기화 지우기
 * 
 * project4 구조 원상복귀
 * rollback시 value 확인?
 * cmake gdb
 * pathname?????
 * 
 * ---Done---
 * insert, delete 시 slot_index 바뀌는데 무시하는건지.
 * detection 주기, 위치
 * memory leak in trx_table
 * lock_entry 구조체 메모리 정렬
 * lock_acquire() 1,2번째 while문 확인. (lock_bitmap? wait_bitmap?)
 * while() { pthread_cond_wait } ?
 * log latch
 * lock_acquire()에서 deadlock detection해도 되는지. NULL return 가능한지.
 * ACTIVE, COMMITTED, ABORTED : 0,1,2 or 1,2,3
 * log->key(X) log->offset(O)
 * trx 관련 trx_latch
 * trx_abort()가 index manager에 있어서 찝찝한 상태.
 * upgradable lock(U_lock)
 * undo할 때 lock?
 * logging 필요? 필요하다면 꼭 파일에 해야 하는지
 * waits_for_trx_id : 1개인지.
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
 */
