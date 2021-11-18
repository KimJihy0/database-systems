#include "lock_table.h"

struct lock_t {
    struct lock_t* prev_lock;
    struct lock_t* next_lock;
    struct lock_entry_t* sentinel;
    pthread_cond_t cond_var;
    struct lock_t* trx_next_lock;
    int64_t owner_trx_id;
};

struct lock_entry_t {
    struct lock_t* head;
    struct lock_t* tail;
};

struct trx_entry_t {
    struct lock_t* head;
    int waits_for_trx_id;
};

pthread_mutex_t lock_table_latch;
pthread_mutex_t trx_table_latch;
struct pair_hash {
    std::size_t operator() (const std::pair<int64_t, int64_t> &pair) const {
        return std::hash<int64_t>()(pair.first) ^ std::hash<int64_t>()(pair.second);
    }
};
std::unordered_map<std::pair<int64_t, int64_t>, lock_entry_t, pair_hash> lock_table;
std::unordered_map<int64_t, trx_entry_t> trx_table;
int trx_id;

int trx_begin() {
    pthread_mutex_lock(&trx_table_latch);
    int ret_val = ++trx_id;
    #if verbose
    printf("trx_begin(%d)\n", ret_val);
    #endif
    pthread_mutex_unlock(&trx_table_latch);
    return ret_val;
}

int trx_commit(int trx_id) {
    pthread_mutex_lock(&trx_table_latch);
    #if verbose
    printf("trx_commit(%d)\n", trx_id);
    #endif
    lock_t* lock_obj = trx_table[trx_id].head;
    lock_t* del_obj;
    while (lock_obj != NULL) {
        if (lock_release(lock_obj) != 0) return 0;
        del_obj = lock_obj;
        lock_obj = lock_obj->trx_next_lock;
        free(del_obj);
    }
    trx_table.erase(trx_id);
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id; 
}

int detect_deadlock(int trx_id) {
    trx_entry_t* cur_entry = &(trx_table[trx_id]);
    int cur_trx_id = cur_entry->waits_for_trx_id;
    while (cur_trx_id != 0) {
        cur_entry = &(trx_table[cur_trx_id]);
        if (cur_entry->waits_for_trx_id == trx_id) break;
        cur_trx_id = cur_entry->waits_for_trx_id;
    }
    return cur_trx_id;
}

int init_lock_table() {
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    trx_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key, int trx_id) {
    pthread_mutex_lock(&lock_table_latch);
    #if verbose
    printf("lock_acquire(%ld, %ld, %d)\n", table_id, key, trx_id);
    #endif

    lock_entry_t* lock_entry = &(lock_table[{table_id, key}]);

    lock_t* lock_obj = lock_entry->head;
    while (lock_obj != NULL) {
        if (lock_obj->owner_trx_id == trx_id) {
            pthread_mutex_unlock(&lock_table_latch);
            return lock_obj;
        }
        lock_obj = lock_obj->next_lock;
    }

    lock_obj = (lock_t*)malloc(sizeof(lock_t));
    if (lock_obj == NULL) return NULL;
    lock_obj->prev_lock = lock_entry->tail;
    lock_obj->next_lock = NULL;
    lock_obj->sentinel = lock_entry;
    lock_obj->cond_var = PTHREAD_COND_INITIALIZER;
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

    lock_t* cur_obj = NULL;
    while (cur_obj != lock_obj) {
        cur_obj = lock_entry->head;
        while (cur_obj != lock_obj) {
            trx_entry->waits_for_trx_id = cur_obj->owner_trx_id;
            #if verbose
            printf("trx %d waits for trx %d to write (%ld, %ld)\n", trx_id, trx_entry->waits_for_trx_id, table_id, key);
            print_waits_for_graph();
            #endif
            if (detect_deadlock(trx_id) != 0) {
                trx_entry->waits_for_trx_id = 0;
                pthread_mutex_unlock(&lock_table_latch);   
                return NULL;
            }
            pthread_cond_wait(&(cur_obj->cond_var), &lock_table_latch);
            #if verbose
            printf("trx %d wakes up trx %d to write (%ld, %ld)\n", trx_entry->waits_for_trx_id, trx_id, table_id, key);
            #endif
            break;
        }
        // cur_obj = cur_obj->next_lock;
    }
    trx_entry->waits_for_trx_id = 0;

    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
}

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    lock_entry_t* lock_entry = lock_obj->sentinel;

    pthread_cond_broadcast(&(lock_obj->cond_var));

    if (lock_obj->prev_lock != NULL) {
        lock_obj->prev_lock->next_lock = lock_obj->next_lock;
    }
    else lock_entry->head = lock_obj->next_lock;
    if (lock_obj->next_lock != NULL) {
        lock_obj->next_lock->prev_lock = lock_obj->prev_lock;
    }
    else lock_entry->tail = lock_obj->prev_lock;

    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}

void print_waits_for_graph() {
    for (int i = 1; i <= 8; i++) {
        printf("\t%d->%d, ", i, trx_table[i].waits_for_trx_id);
    }
    printf("\n");
}