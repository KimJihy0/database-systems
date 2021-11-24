#include "trx.h"

pthread_mutex_t lock_latch;
pthread_mutex_t trx_latch; 
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, pagenum_t> &pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<pagenum_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, pagenum_t>, lock_entry_t, pair_hash> lock_table;
std::unordered_map<int, trx_entry_t*> trx_table;
int trx_id;

int trx_begin() {
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_begin");
    #endif
    pthread_mutex_lock(&trx_latch);
    int local_trx_id = ++trx_id;
                            #if verbose
                            printf("(%d)\ntrx_begin(%d) start\n", trx_id, trx_id);
                            #endif
    trx_table[trx_id] = new trx_entry_t;
    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = ACTIVE;
    pthread_mutex_unlock(&trx_latch);
                            #if verbose
                            printf("trx_begin(%d) end\n", trx_id);
                            #endif
    return local_trx_id;
}

int trx_commit(int trx_id) {
    if (trx_table[trx_id]->trx_state == ABORTED) return 0;
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_commit(%d)\n", trx_id);
    #endif

    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("trx_commit(%d) start\n", trx_id);
                            #endif

    lock_t* lock_obj = trx_table[trx_id]->head;
    lock_t* del_obj;
    while (lock_obj != NULL) {
        lock_release(lock_obj);
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }
    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = COMMITTED;

                            #if verbose
                            printf("trx_commit(%d) end\n", trx_id);
                            #endif
    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int trx_abort(int trx_id) {
    if (trx_table[trx_id]->trx_state == ABORTED) return 0;
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_abort(%d)\n", trx_id);
    #endif
    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("trx_abort(%d) start\n", trx_id);
                            #endif

    page_t * p;
    int i;
    log_t* log;
    while (!(trx_table[trx_id]->logs.empty())) {
        log = &(trx_table[trx_id]->logs.top());
        buffer_read_page(log->table_id, log->page_num, &p);
        for (i = 0; i < p->num_keys; i++) if (p->slots[i].key == log->key) break;
        memcpy(p->values + p->slots[i].offset - 128, log->old_value, p->slots[i].size);
        buffer_write_page(log->table_id, log->page_num, &p);
        trx_table[trx_id]->logs.pop();
    }
   
    lock_t* lock_obj = trx_table[trx_id]->head;
    lock_t* del_obj;
    while (lock_obj != NULL) {
        lock_release(lock_obj);
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }
    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = ABORTED;

                            #if verbose
                            printf("trx_abort(%d) end\n", trx_id);
                            #endif
    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int detect_deadlock(int trx_id) {
    std::unordered_map<int, int> visit;
    pthread_mutex_lock(&trx_latch);
    do {
        visit[trx_id] = 1;
        trx_id = trx_table[trx_id]->waits_for_trx_id;
    } while (trx_id != 0 && !visit[trx_id]);
    pthread_mutex_unlock(&trx_latch);
    return trx_id;
}

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

int lock_acquire(int64_t table_id, pagenum_t page_num, int64_t key, int idx, int trx_id, int lock_mode) {
                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d)\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif
    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d) lock\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif

    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);
    lock_t* lock_obj;

    lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (lock_obj->record_id == key && lock_obj->owner_trx_id == trx_id) {
            pthread_mutex_unlock(&lock_latch);
                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d) exist\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif
            // if (lock_obj->lock_mode == SHARED && lock_mode == EXCLUSIVE) {
            //                 #if verbose
            //                 printf("but conflict\n");
            //                 #endif
            //     // return -1;
            // }
            return 0;
        }
        lock_obj = lock_obj->next_lock;
    }

    // implement implicit locking

    if (lock_attach(table_id, page_num, key, idx, trx_id, lock_mode) != 0) {
                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d) deadlock\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif
        pthread_mutex_unlock(&lock_latch);
        return -1;
    }

                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d) success\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif

    pthread_mutex_unlock(&lock_latch);
    return 0;
}

int lock_attach(int64_t table_id, pagenum_t page_num, int64_t key, int idx, int trx_id, int lock_mode) {
    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);
    trx_entry_t* trx_entry = trx_table[trx_id];
    lock_t* lock_obj = lock_entry->head;

                            #if verbose
                            printf("lock_acquire(%ld, %ld, %c, %d) alloc~~\n", page_num, key, lock_mode ? 'X' : 'S', trx_id);
                            #endif
    lock_obj = new lock_t;
    lock_obj->prev_lock = lock_entry->tail;
    lock_obj->next_lock = NULL;
    lock_obj->sentinel = lock_entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;
    lock_obj->lock_mode = lock_mode;
    lock_obj->record_id = key;
    lock_obj->trx_next_lock = NULL;
    lock_obj->owner_trx_id = trx_id;
    // lock_obj->lock_bitmap = 0UL;
    // lock_obj->wait_bitmap = 0UL;

    if (lock_entry->head == NULL) {
        lock_entry->head = lock_obj;
        lock_entry->tail = lock_obj;
    }
    else {
        lock_entry->tail->next_lock = lock_obj;
        lock_entry->tail = lock_obj;
    }

    lock_obj->trx_next_lock = trx_entry->head;
    trx_entry->head = lock_obj;

    // SET_BIT(lock_obj->wait_bitmap, idx);

    lock_t* cur_obj = lock_entry->head;
    while (cur_obj != lock_obj) {
        cur_obj = lock_entry->head;
        while (cur_obj != lock_obj) {
            if (cur_obj->record_id == key &&
                    (cur_obj->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE)) {
                trx_entry->waits_for_trx_id = cur_obj->owner_trx_id;
                                #if verbose
                                if (lock_mode) printf("trx %d waits for trx %d to write (%ld, %ld)\n", trx_id, trx_entry->waits_for_trx_id, page_num, key);
                                else printf("trx %d waits for trx %d to read (%ld, %ld)\n", trx_id, trx_entry->waits_for_trx_id, table_id, key);
                                // print_waits_for_graph();
                                // print_locks();
                                #endif
                if (detect_deadlock(trx_id) == trx_id) return -1;
                pthread_cond_wait(&(cur_obj->cond_var), &lock_latch);
                                #if verbose
                                if (lock_mode) printf("trx %d wakes up trx %d to write (%ld, %ld)\n", trx_entry->waits_for_trx_id, trx_id, page_num, key);
                                else printf("trx %d wakes up trx %d to read (%ld, %ld)\n", trx_entry->waits_for_trx_id, trx_id, table_id, key);
                                // print_waits_for_graph();
                                // print_locks();
                                #endif
                trx_entry->waits_for_trx_id = 0;
                break;
            }
            cur_obj = cur_obj->next_lock;
        }
    }
    // SET_BIT(lock_obj->lock_bitmap, idx);
    return 0;
}

int lock_release(lock_t* lock_obj) {
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
    return 0;
}

/* ---To do---
 * 테스트코드 만들어야 하나?
 * log->key(X) log->offset(O)
 * project4 구조 원상복귀
 * broadcast -> signal
 * lock_acquire() 1,2번째 while문 확인.
 * trx 관련 trx_latch
 * 
 * max_trx_id
 * LRU -> head, tail로 구현 가능한지 확인.
 * rollback시 value 확인
 * layer : file->buffer->trx->bpt
 * while() { pthread_cond_wait } ?
 * cmake gdb
 * log latch
 * lock_acquire()에서 deadlock detection해도 되는지. NULL return 가능한지.
 * pathname?????
 * detection 주기, 위치
 * insert, delete 시 slot_index 바뀌는데 무시하는건지.
 * lock_entry 구조체 메모리 정렬
 * piazza @241 (아마 안할듯)
 * memory leak in trx_table
 * ACTIVE, COMMITTED, ABORTED : 0,1,2 or 1,2,3
 * 
 * ---Done---
 * trx_abort()가 index manager에 있어서 찝찝한 상태.
 * upgradable lock(U_lock)
 * undo할 때 lock?
 * logging 필요? 필요하다면 꼭 파일에 해야 하는지
 * waits_for_trx_id : 1개인지.
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
 * ---Recent modification---
 * small deadlock 제거
 * 
 * 
 */ 
