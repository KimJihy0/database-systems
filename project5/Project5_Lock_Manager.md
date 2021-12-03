# Milestone01. Lock Manager
Transaction provides 4 properties, ACID : Atomicity, Consistency, Isolation, Durability.  
Lock Manager provides consistency, and isolation for concurrency control.  

* Consistency  
Transaction can only bring the database from one valid state to another, maintaining database invariants.
* Isolation  
Concurrent execution of transactions leaves the database in the same state that would have been obtained if the transactions were executed sequentially.

# 1. Conflict-serializable schedule  
Two operations **conflict** if they:  
* are by different transactions,
* are on the same object,
* at least one of them is a write.

Two schedules are **conflict equivalent** iff:
* They involve the same actions of the same transactions, and
* Every pair of conflicting actions is ordered the same way.

Schedule S is **conflict-serializable schedule** if:
* S is conflict equivalent to some serial schedule.

Conflict-serializable schedule provides isolation in a safe and fast way. 
# Implementation
I implemented conflict-serializable schedule as follows.
1. allocates new lock (has shared or exclusive lock mode)
2. checks every lock in the same lock entry to see if there is a conflict.
3. if there is a conflicting lock, new lock waits for that lock to release.  
otherwise, go to 5.
4. when wakes up, go to 2.
5. succeeds to acquire new lock. (if there is no deadlock)

```cpp
// lock_aquire()
// 1.
...
lock_t* lock_obj = lock_alloc(record_id, lock_mode)
lock_t* cur_obj = lock_entry->head;
while (cur_obj != lock_obj) {
    // 2.
    if (cur_obj->record_id == record_id &&                              // are on the same object
        cur_obj->owner_trx_id != trx_id &&                              // are by different transactions
        (cur_obj->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE)) {  // at least one of them is a write.
        ...
        // 3.
        pthread_cond_wait(&cur_obj->cond_bar);
        ...
        // 4.
        continue;
    }
    cur_obj = cur_obj->next_lock;
}
return 0;
// 5.
```

# 2. Strict two phase locking (2PL)
**Strict 2PL** protocol:
* Transaction must obtain a **S lock** before reading, and an **X lock** before writing.
* All locks released together when transaction completes.

Strict 2PL guarantees conflict serializability without cascading aborts.

# Implementation
I implemented strict 2PL using mutex.  
Between pthread_mutex_lock and pthread_mutex_unlock, All locks released together.

```cpp
// trx_commit(), trx_abort()
...
pthread_mutex_lock(&lock_latch);
lock_t* lock_obj = trx_table[trx_id]->head;
while (lock_obj != NULL) {
    lock_release(lock_obj);
    lock_obj = lock_obj->trx_next_lock;
    ...
}
...
pthread_mutex_unlock(&lock_latch);
return trx_id;
```

# 3. Deadlock detection
**Deadlock**:  
* Cycle of transactions waiting for locks to be released by each other.  

Deadlock can be detected by waits-for graph.  
Cyclic graph means that deadlock exists.  
When deadlock is detected, abort should be performed.

# Implementation
I implemented deadlock detection as follows.  
1. When conflict is detected, sets `waits_for_trx_id` to the `trx_id` of the conflicting lock.
2. Before waiting for the conflicting lock, detects deadlock.  
3. If deadlock is detected(cyclic graph), return and API calls `trx_abort()`.  
(If `trx_id` is not included in the cycle, just waits.)  
Otherwise, waits for the conflicting lock.  
After waking up, sets `waits_for_trx_id` to `0`(means waiting for no one).

```cpp
// lock_acquire()
...
while (...) {
    if (...) {
        // 1.
        trx_table[trx_id]->waits_for_trx_id = cur_obj->owner_trx_id;
        // 2.
        if (detect_deadlock(trx_id) == trx_id) {
            // 3-1.
            return -1;
        }
        // 3-2.
        pthread_cond_wait(&(cur_obj->cond_var), &lock_latch);
        trx_table[trx_id]->waits_for_trx_id = 0;
        ...
    }
    ...
}
return 0;

// detect_deadlock()
int detect_deadlock(int trx_id) {
    std::unordered_map<int, int> visit;
    do {
        visit[trx_id] = 1;
        trx_id = trx_table[trx_id]->waits_for_trx_id;
    } while (is_active(trx_id) && !visit[trx_id]);
    return trx_id;
}

// db_find(), db_update()
...
if (lock_acquire(table_id, p_pgnum, i, trx_id, SHARED) != 0) {
    // 3-1.
    trx_abort(trx_id);
    return trx_id;
}
...
```

# 4. Transaction abort
Abort should be executed in the following order.
1. Undo all modified records by the transaction.
2. Release all acquired lock objects.
3. Remove the transaction table entry.

# Implementation
I implemented transaction abort as follows.
1. When modifying records, logs them.
2. Undo using the logs.
3. Releases all acquired lock objects.
4. Remove the transaction table entry.

```cpp
// db_update()
...
buffer_read_page(table_id, p_pgnum, &p);
// 0.
log_t log(table_id, p_pgnum, offset, size);
memcpy(p->values + offset, value, new_val_size);
trx_table[trx_id]->logs.push(log);
...
buffer_write_page(table_id, p_pgnum, &p);
...

// trx_abort()
// 1.
page_t* p;
while (!(trx_table[trx_id]->logs.empty())) {
    log_t* log = &(trx_table[trx_id]->logs.top());
    buffer_read_page(log->table_id, log->page_num);
    memcpy(p->values + log->offset, log->old_value, log->size);
    buffer_write_page(log->table_id, log->page_num);
    trx_table[trx_id]->logs.pop();
}
// 2.
lock_t* lock_obj = trx_table[trx_id]->head;
while (lock_obj != NULL) {
    lock_release(lock_obj);
    lock_obj = lock_obj->trx_next_lock;
    ...
}
// 3.
delete trx_table[trx_id];
...

```

# Milestone02. Two Optimization Techniques for the Lock Manager
Lock compression is an optimization technique for reducing space overhead for shared locks.  
Implicit locking is an optimization technique for reducing space overhead for exclusive locks.  
They also complement the high overhead of fine granularity.
# 1. Lock compression
For convenience, I turn on the bitmap corresponding to the record even in the X_lock.

# Implementation
I implemented lock compression as follows.
1. Checks whether the shared lock can be acquired immediately in the record.
2. If it is possible to acquire immediately, turn on the bitmap corresponding to the record.

```cpp
// lock_acquire()
...
if (lock_mode == SHARED) {
    ...
    // 1.
    if (lock_obj != NULL) {
        lock_t* cur_obj = lock_entry->head;
        while (cur_obj != NULL) {
            if (...) break;
            cur_obj = cur_obj->next_lock;
        }
        if (cur_obj == NULL) {
            // 2.
            SET_BIT(lock_obj->bitmap, idx);
            return 0;
        }
    }
}
...
```

# 2. Implicit locking

# Implementation
I implemented implicit locking as follows.
1. Checks the lock table to see if there is a lock object for the record to lock.
2. If there's a lock object, attaches the lock object to the lock table and waits.  
Otherwise, checks whether the transaction with the trx id written in the tuple is active.
3. If that transaction is active, in addition to own lock object, the lock object of the transaction that acquired the exclusive lock first must also be attached.  
Otherwise, writes the trx id in the tuple to acquire the implicit lock.


```cpp
...
// lock_acquire()
lock_t* lock_obj = lock_entry->head;
// 1.
while (lock_obj != NULL) {
    if (GET_BIT(lock_obj->bitmap, idx) != 0) break;
    lock_obj = lock_obj->next_lock;
}
if (lock_obj == NULL) {
    // 2-2.
    page_t* p;
    buffer_read_page(table_id, page_num, &p);   // acquire page latch
    int impl_trx_id = p->slots[idx].trx_id;
    if (is_active(impl_trx_id)) {
        ...
        // 3-1.
        lock_alloc(table_id, page_num, idx, impl_trx_id, EXCLUSIVE);
    } else if (lock_mode == EXCLUSIVE) {
        // 3-2.
        p->slots[idx].trx_id = trx_id;
        buffer_write_page(table_id, page_num);  // release page latch 
        return 0;
    }
    buffer_unpin_page(table_id, page_num);      // release page latch
}
// 2-1, 3-1.
...
lock_obj = lock_alloc(table_id, page_num, idx, trx_id, lock_mode);
...
```
