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
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_begin()\n");
    #endif
    pthread_mutex_lock(&trx_latch);
    int local_trx_id = ++trx_id;
                            #if verbose
                            printf("\t\t\t\t\ttrx_begin(%d) start\n", trx_id);
                            #endif
    trx_table[trx_id] = new trx_entry_t;
    trx_table[trx_id]->head = NULL;
    trx_table[trx_id]->waits_for_trx_id = 0;
    trx_table[trx_id]->trx_state = ACTIVE;
                            #if verbose
                            printf("\t\t\t\t\ttrx_begin(%d) end\n", trx_id);
                            #endif
    pthread_mutex_unlock(&trx_latch);
    return local_trx_id;
}

int trx_commit(int trx_id) {
    if (trx_table[trx_id]->trx_state == ABORTED) return 0;
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_commit(%d)\n", trx_id);
    #endif

    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("\t\t\t\t\ttrx_commit(%d) start\n", trx_id);
                            #endif
    trx_entry_t* trx_entry = trx_table[trx_id];
    lock_t* lock_obj = trx_entry->head;

    lock_t* del_obj;
    while (lock_obj != NULL) {
        if (lock_release(lock_obj) != 0) return 0;
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }

    trx_entry->head = NULL;
    trx_entry->waits_for_trx_id = 0;
    trx_entry->trx_state = COMMITTED;

                            #if verbose
                            printf("\t\t\t\t\ttrx_commit(%d) end\n", trx_id);
                            #endif
    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int trx_abort(int trx_id) {
    #if verbose
    printf("----------------------------------------------------------------------------------------trx_abort(%d)\n", trx_id);
    #endif
    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("\t\t\t\t\ttrx_abort(%d) start\n", trx_id);
                            #endif

    trx_entry_t* trx_entry = trx_table[trx_id];
    page_t * p;
    log_t* log;
    while (!(trx_entry->logs.empty())) {
        log = &(trx_entry->logs.top());
        buffer_read_page(log->table_id, log->page_num, &p);
        memcpy(p->values + log->offset, log->old_value, log->size);
        buffer_write_page(log->table_id, log->page_num, &p);
        trx_entry->logs.pop();
    }
    lock_t* lock_obj = trx_entry->head;
   
    lock_t* del_obj;
    while (lock_obj != NULL) {
        if (lock_release(lock_obj) != 0) return 0;
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        delete del_obj;
    }

    trx_entry->head = NULL;
    trx_entry->waits_for_trx_id = 0;
    trx_entry->trx_state = ABORTED;

                            #if verbose
                            printf("\t\t\t\t\ttrx_abort(%d) end\n", trx_id);
                            #endif
    pthread_mutex_unlock(&lock_latch);
    return trx_id;
}

int lock_acquire(int64_t table_id, pagenum_t page_num, int64_t key, int idx, int trx_id, int lock_mode) {
                            #if verbose
                            printf("\t\t\t\t\tlock_acquire(%ld, %d, %c, %d)\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
    pthread_mutex_lock(&lock_latch);
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) lock\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif

    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);
    lock_t* lock_obj;

    lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (GET_BIT(lock_obj->wait_bitmap, idx) != 0 && ///////////////////////// : 뭘로 해도 상관없을듯
            lock_obj->owner_trx_id == trx_id &&
            lock_obj->lock_mode >= lock_mode) {
            pthread_mutex_unlock(&lock_latch);
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) exist\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
            return 0;
        }
        lock_obj = lock_obj->next_lock;
    }

    lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (GET_BIT(lock_obj->wait_bitmap, idx) != 0) break;
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
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) impl exist\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
                pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
                pthread_mutex_unlock(&trx_latch);
                pthread_mutex_unlock(&lock_latch);
                return 0;
            }
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) attaches impl\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            printf("lock_acquire(%ld, %d, %c, %d) attached\n", page_num, idx, 'X', impl_trx_id);
                            #endif
            if (lock_attach(table_id, page_num, key, idx, impl_trx_id, EXCLUSIVE) != 0) {
                            #if verbose
                            printf("?????????????????????????????????????????????????????????????????????????????????????");
                            #endif
            }
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) success\n", page_num, idx, 'X', impl_trx_id);
                            #endif
        }
        else if (lock_mode == EXCLUSIVE) {
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) impl~~\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            printf("lock_acquire(%ld, %d, %c, %d) success\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
            p->slots[idx].trx_id = trx_id;
            buffer_write_page(table_id, page_num, &p);
            pthread_mutex_unlock(&trx_latch);
            pthread_mutex_unlock(&lock_latch);
            return 0;
        }
        pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
        pthread_mutex_unlock(&trx_latch);
    }

    if (lock_attach(table_id, page_num, key, idx, trx_id, lock_mode) != 0) {
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) deadlock\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
        pthread_mutex_unlock(&lock_latch);
        return -1;
    }

                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) success\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif

    pthread_mutex_unlock(&lock_latch);
    return 0;
}

int lock_attach(int64_t table_id, pagenum_t page_num, int64_t key, int idx, int trx_id, int lock_mode) {
    lock_entry_t* lock_entry = &(lock_table[{table_id, page_num}]);
    trx_entry_t* trx_entry = trx_table[trx_id];
    lock_t* lock_obj = NULL;

    if (lock_mode == SHARED) {
        lock_obj = lock_entry->head;
        while (lock_obj != NULL) {
            if (lock_obj->lock_mode == lock_mode && lock_obj->owner_trx_id == trx_id) break;
            lock_obj = lock_obj->next_lock;
        }
    }

    if (lock_obj == NULL) {
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) alloc~~\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
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
        lock_obj->lock_bitmap = 0UL;
        lock_obj->wait_bitmap = 0UL;
    }
    else {
        if (lock_obj->prev_lock != NULL) {
        lock_obj->prev_lock->next_lock = lock_obj->next_lock;
        }
        else lock_entry->head = lock_obj->next_lock;
        if (lock_obj->next_lock != NULL) {
            lock_obj->next_lock->prev_lock = lock_obj->prev_lock;
        }
        else lock_entry->tail = lock_obj->prev_lock;
        lock_obj->prev_lock = lock_entry->tail;
        lock_obj->next_lock = NULL;
                            #if verbose
                            printf("lock_acquire(%ld, %d, %c, %d) toggle~~\n", page_num, idx, lock_mode ? 'X' : 'S', trx_id);
                            #endif
    }
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
    SET_BIT(lock_obj->wait_bitmap, idx);

    lock_t* cur_obj = lock_entry->head;
    while (cur_obj != lock_obj) {
        if (GET_BIT(cur_obj->wait_bitmap, idx) != 0 &&
            cur_obj->owner_trx_id != trx_id &&
            (cur_obj->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE)) {
            trx_entry->waits_for_trx_id = cur_obj->owner_trx_id;
                            #if verbose
                            if (lock_mode) printf("trx %d waits for trx %d to write (%ld, %ld)\n", trx_id, trx_entry->waits_for_trx_id, page_num, idx);
                            else printf("trx %d waits for trx %d to read (%ld, %ld)\n", trx_id, trx_entry->waits_for_trx_id, page_num, idx);
                            print_waits_for_graph();
                            print_locks(NULL);
                            #endif
            if (detect_deadlock(trx_id) == trx_id) return -1;
            pthread_cond_wait(&(cur_obj->cond_var), &lock_latch);
                                #if verbose
                                if (lock_mode) printf("trx %d wakes up trx %d to write (%ld, %ld)\n", trx_entry->waits_for_trx_id, trx_id, page_num, idx);
                                else printf("trx %d wakes up trx %d to read (%ld, %ld)\n", trx_entry->waits_for_trx_id, trx_id, page_num, idx);
                                print_waits_for_graph();
                                print_locks(NULL);
                                #endif
            cur_obj = lock_entry->head;
            trx_entry->waits_for_trx_id = 0;
            continue;
        }
        cur_obj = cur_obj->next_lock;
    }
    SET_BIT(lock_obj->lock_bitmap, idx);
    return 0;
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

#if verbose
void print_waits_for_graph() {
    char c;
    for (int i = 1; i <= 8; i++) {
        printf("\t%d->%d ", i, trx_table[i] ? trx_table[i]->waits_for_trx_id : -1);
    }
    printf("\n");
    for (int i = 1; i <= 8; i++) {
        c = 'N';
        if (trx_table[i])
            switch (trx_table[i]->trx_state) {
                case ACTIVE : c = 'A'; break;
                case COMMITTED : c = 'C'; break;
                case ABORTED : c = 'X'; break;
            }
        printf("\t%d: %c ", i, c);
    }
    printf("\n");
}

void* print_locks(void* args) {
    for (int i = 2559; i > 2557; i--) {
        printf("\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tpage%d : ", i);
        lock_t* lock_obj = lock_table[{568, i}].head;
        for (; lock_obj; lock_obj = lock_obj->next_lock) {
            printf("[T%d ", lock_obj->owner_trx_id);
            for (int j = 1; j >= 0; j--) printf("%ld", GET_BIT(lock_obj->lock_bitmap, j) % 2);
            printf("%c", lock_obj->lock_mode ? 'X' : 'S');
            printf("]->");
        }
        printf("[NULL]\n");
    }
    // for (int i = 0; i < TABLE_NUMBER; i++) {
    //     for (int j = 0; j < RECORD_NUMBER; j++) {
    //         printf("\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t(%d,%d) : T%d / ", i, j, implicit_trx_id[i][j]);
    //     }
    // }
    printf("\n");
    return NULL;
}
#endif


/* ---To do---
 * project4 구조 원상복귀
 * lock_acquire() 1,2번째 while문 확인. (lock_bitmap? wait_bitmap?)
 * 
 * max_trx_id
 * LRU -> head, tail로 구현 가능한지 확인.
 * rollback시 value 확인
 * layer : file->buffer->trx->bpt
 * cmake gdb
 * pathname?????
 * detection 주기, 위치
 * insert, delete 시 slot_index 바뀌는데 무시하는건지.
 * lock_entry 구조체 메모리 정렬
 * memory leak in trx_table
 * 
 * ---Done---
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
 * 
 * ---Recent modification---
 * small deadlock 제거
 * 
 * 
 */ 
