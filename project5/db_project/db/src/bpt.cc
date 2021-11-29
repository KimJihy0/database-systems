#include "bpt.h"

// DBMS

int init_db(int num_buf) {
    if (init_buffer(num_buf) != 0) return -1;
    if (init_lock_table() != 0) return -1;
    return 0;
}

int shutdown_db() {
    if (shutdown_lock_table() != 0) return -1;
    if (shutdown_buffer() != 0) return -1;
    file_close_table_file();
    return 0;
}

int64_t open_table(char * pathname) {
    return file_open_table_file(pathname);
}

// SEARCH & UPDATE

int db_find(int64_t table_id, int64_t key,
            char * ret_val, uint16_t * val_size, int trx_id) {
    pagenum_t p_pgnum;
    page_t * p;
    int i;
    
    if (trx_id != 0 && trx_table[trx_id]->trx_state == ABORTED) return trx_id;

    p_pgnum = find_leaf(table_id, key);
    if (p_pgnum == 0) return -1;

    int p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    int num_keys = p->num_keys;
    for (i = 0; i < num_keys; i++) {
        if (p->slots[i].key == key) break;
    }
    pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));

    if (i == num_keys) return -1;
    if (ret_val == NULL || val_size == NULL || trx_id == 0) return -2;
    if (lock_acquire(table_id, p_pgnum, i, trx_id, SHARED) != 0) {
        trx_abort(trx_id);
        return trx_id;
    }

    p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    memcpy(ret_val, p->values + p->slots[i].offset - HEADER_SIZE, p->slots[i].size);
    *val_size = p->slots[i].size;
    pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));

    return 0;
}

int db_update(int64_t table_id, int64_t key,
              char * value, uint16_t new_val_size, uint16_t * old_val_size, int trx_id) {
    pagenum_t p_pgnum;
    page_t * p;
    int i;

    if (trx_id != 0 && trx_table[trx_id]->trx_state == ABORTED) return trx_id;
    
    p_pgnum = find_leaf(table_id, key);
    if (p_pgnum == 0) return -1;

    int p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    int num_keys = p->num_keys;
    for (i = 0; i < num_keys; i++) {
        if (p->slots[i].key == key) break;
    }
    pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
    
    if (i == num_keys) return -1;
    if (old_val_size == NULL || trx_id == 0) return -2;
    if (lock_acquire(table_id, p_pgnum, i, trx_id, EXCLUSIVE) != 0) {
        trx_abort(trx_id);
        return trx_id;
    }

    buffer_read_page(table_id, p_pgnum, &p);
    uint16_t offset = p->slots[i].offset - HEADER_SIZE;
    uint16_t size = p->slots[i].size;
    log_t log(table_id, p_pgnum, offset, size);
    memcpy(log.old_value, p->values + offset, size);
    memcpy(p->values + offset, value, new_val_size);
    memcpy(log.new_value, p->values + offset, size);
    *old_val_size = size;
    trx_table[trx_id]->logs.push(log);
    buffer_write_page(table_id, p_pgnum, &p);

    return 0;
}

pagenum_t find_leaf(int64_t table_id, int64_t key) {
    pagenum_t p_pgnum;
    page_t * p, * header;
    int i;
    int header_buffer_idx = buffer_read_page(table_id, 0, &header);
    p_pgnum = header->root_num;
    pthread_mutex_unlock(&(buffers[header_buffer_idx]->page_latch));
    if (p_pgnum == 0) return 0;
    int p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    while (!p->is_leaf) {
        i = 0;
        while (i < p->num_keys) {
            if (key >= p->entries[i].key) i++;
            else break;
        }
        p_pgnum = i ? p->entries[i - 1].child : p->left_child;
        pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
        p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    }
    pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
    return p_pgnum;
}

// INSERTION

int db_insert(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    pagenum_t leaf_pgnum, root_pgnum;
    page_t * leaf, * header;
    int free_space;

    if (db_find(table_id, key, NULL, NULL, 0) == -2) return -1;

    int header_buffer_idx = buffer_read_page(table_id, 0, &header);
    root_pgnum = header->root_num;
    pthread_mutex_unlock(&(buffers[header_buffer_idx]->page_latch));

    if (root_pgnum == 0) {
        start_tree(table_id, key, value, val_size);
        return 0;
    }

    leaf_pgnum = find_leaf(table_id, key);

    int leaf_buffer_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
    free_space = leaf->free_space;
    pthread_mutex_unlock(&(buffers[leaf_buffer_idx]->page_latch));

    if (free_space >= SLOT_SIZE + val_size) {
        insert_into_leaf(table_id, leaf_pgnum, key, value, val_size);
    }
    else {
        insert_into_leaf_split(table_id, leaf_pgnum, key, value, val_size);
    }

    return 0;
}

void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char * value, uint16_t val_size) {
    page_t * leaf;
    int i, insertion_index;
    uint16_t offset;
    
    buffer_read_page(table_id, leaf_pgnum, &leaf);
    
    insertion_index = 0;
    while (insertion_index < leaf->num_keys && leaf->slots[insertion_index].key < key) {
        insertion_index++;
    }
    offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;

    for (i = leaf->num_keys; i > insertion_index; i--) {
        leaf->slots[i].key = leaf->slots[i - 1].key;
        leaf->slots[i].size = leaf->slots[i - 1].size;
        leaf->slots[i].offset = leaf->slots[i - 1].offset;
        leaf->slots[i].trx_id = leaf->slots[i - 1].trx_id;
    }
    leaf->slots[insertion_index].key = key;
    leaf->slots[insertion_index].size = val_size;
    leaf->slots[insertion_index].offset = offset - val_size + HEADER_SIZE;
    leaf->slots[insertion_index].trx_id = 0;
    memcpy(leaf->values + offset - val_size, value, val_size);

    leaf->num_keys++;
    leaf->free_space -= (SLOT_SIZE + val_size);

    buffer_write_page(table_id, leaf_pgnum, &leaf);
}

void insert_into_leaf_split(int64_t table_id, pagenum_t leaf_pgnum,
                            int64_t key, char * value, uint16_t val_size) {
    pagenum_t new_pgnum;
    page_t * leaf, * new_leaf;
    int i, j, split, insertion_index, num_keys, total_size;
    int64_t new_key;
    uint16_t offset;
    slot_t temp_slots[65];
    char temp_values[3968];
    
    buffer_read_page(table_id, leaf_pgnum, &leaf);

    insertion_index = 0;
    while (insertion_index < leaf->num_keys && leaf->slots[insertion_index].key < key) {
        insertion_index++;
    }
    offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;

    for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_slots[j].key = leaf->slots[i].key;
        temp_slots[j].size = leaf->slots[i].size;
        temp_slots[j].offset = leaf->slots[i].offset;
        temp_slots[j].trx_id = leaf->slots[i].trx_id;
    }
    temp_slots[insertion_index].key = key;
    temp_slots[insertion_index].size = val_size;
    temp_slots[insertion_index].offset = offset - val_size + HEADER_SIZE;
    temp_slots[insertion_index].trx_id = 0;
    memcpy(temp_values + offset, leaf->values + offset, FREE_SPACE - offset);
    memcpy(temp_values + offset - val_size, value, val_size);

    num_keys = leaf->num_keys;
    leaf->num_keys = 0;
    leaf->free_space = FREE_SPACE;

    total_size = 0;
    for (split = 0; split <= num_keys; split++) {
        total_size += (SLOT_SIZE + temp_slots[split].size);
        if (total_size >= FREE_SPACE / 2) break;
    }
    new_pgnum = make_leaf(table_id);

    buffer_read_page(table_id, new_pgnum, &new_leaf);
    
    offset = FREE_SPACE;
    for (i = 0; i < split; i++) {
        offset -= temp_slots[i].size;
        leaf->slots[i].key = temp_slots[i].key;
        leaf->slots[i].size = temp_slots[i].size;
        leaf->slots[i].offset = offset + HEADER_SIZE;
        leaf->slots[i].trx_id = temp_slots[i].trx_id;
        memcpy(leaf->values + offset,
                temp_values + temp_slots[i].offset - HEADER_SIZE, temp_slots[i].size);
        leaf->num_keys++;
        leaf->free_space -= (SLOT_SIZE + temp_slots[i].size);
    }
    
    offset = FREE_SPACE;
    for (i = split, j = 0; i <= num_keys; i++, j++) {
        offset -= temp_slots[i].size;
        new_leaf->slots[j].key = temp_slots[i].key;
        new_leaf->slots[j].size = temp_slots[i].size;
        new_leaf->slots[j].offset = offset + HEADER_SIZE;
        new_leaf->slots[j].trx_id = temp_slots[i].trx_id;
        memcpy(new_leaf->values + offset,
                temp_values + temp_slots[i].offset - HEADER_SIZE, temp_slots[i].size);
        new_leaf->num_keys++;
        new_leaf->free_space -= (SLOT_SIZE + temp_slots[i].size);
    }

    new_leaf->sibling = leaf->sibling;
    leaf->sibling = new_pgnum;

    new_leaf->parent = leaf->parent;
    new_key = new_leaf->slots[0].key;

    buffer_write_page(table_id, leaf_pgnum, &leaf);
    buffer_write_page(table_id, new_pgnum, &new_leaf);

    insert_into_parent(table_id, leaf_pgnum, new_key, new_pgnum);
}

void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t parent_pgnum;
    page_t * left, * parent;
    int left_index, num_keys;

    int left_buffer_idx = buffer_read_page(table_id, left_pgnum, &left);
    parent_pgnum = left->parent;
    pthread_mutex_unlock(&(buffers[left_buffer_idx]->page_latch));

    if (parent_pgnum == 0) {
        insert_into_new_root(table_id, left_pgnum, key, right_pgnum);
        return;
    }

    left_index = get_left_index(table_id, parent_pgnum, left_pgnum);
    
    int parent_buffer_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    num_keys = parent->num_keys;
    pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));

    if (num_keys < ENTRY_ORDER - 1) {
        insert_into_page(table_id, parent_pgnum, left_index, key, right_pgnum);
    }
    else {
        insert_into_page_split(table_id, parent_pgnum, left_index, key, right_pgnum);
    }
}

void insert_into_page(int64_t table_id, pagenum_t p_pgnum,
                      int left_index, int64_t key, pagenum_t right_pgnum) {
    page_t * p;
    int i;

    buffer_read_page(table_id, p_pgnum, &p);

    for (i = p->num_keys; i > left_index; i--) {
        p->entries[i].key = p->entries[i - 1].key;
        p->entries[i].child = p->entries[i - 1].child;
    }
    p->entries[left_index].child = right_pgnum;
    p->entries[left_index].key = key;
    p->num_keys++;

    buffer_write_page(table_id, p_pgnum, &p);
}

void insert_into_page_split(int64_t table_id, pagenum_t old_pgnum,
                            int left_index, int64_t key, pagenum_t right_pgnum) {
    pagenum_t new_pgnum;
    page_t * old_page, * new_page, * child;
    int i, j, split;
    int64_t k_prime;
    pagenum_t temp_left_child;
    entry_t temp[ENTRY_ORDER];

    buffer_read_page(table_id, old_pgnum, &old_page);

    temp_left_child = old_page->left_child;
    for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp[j].child = old_page->entries[i].child;
    }
    for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp[j].key = old_page->entries[i].key;
    }
    temp[left_index].child = right_pgnum;
    temp[left_index].key = key;

    split = ENTRY_ORDER / 2 + 1;
    new_pgnum = make_page(table_id);

    buffer_read_page(table_id, new_pgnum, &new_page);
    
    old_page->num_keys = 0;
    old_page->left_child = temp_left_child;
    for (i = 0; i < split - 1; i++) {
        old_page->entries[i].child = temp[i].child;
        old_page->entries[i].key = temp[i].key;
        old_page->num_keys++;
    }
    k_prime = temp[split - 1].key;
    new_page->left_child = temp[i].child;
    for (++i, j = 0; i < ENTRY_ORDER; i++, j++) {
        new_page->entries[j].child = temp[i].child;
        new_page->entries[j].key = temp[i].key;
        new_page->num_keys++;
    }
    new_page->parent = old_page->parent;
    
    buffer_read_page(table_id, new_page->left_child, &child);
    child->parent = new_pgnum;
    buffer_write_page(table_id, new_page->left_child, &child);
    for (i = 0; i < new_page->num_keys; i++) {
        buffer_read_page(table_id, new_page->entries[i].child, &child);
        child->parent = new_pgnum;
        buffer_write_page(table_id, new_page->entries[i].child, &child);
    }
    
    buffer_write_page(table_id, new_pgnum, &new_page);
    buffer_write_page(table_id, old_pgnum, &old_page);

    insert_into_parent(table_id, old_pgnum, k_prime, new_pgnum);
}

void start_tree(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    pagenum_t root_pgnum;
    page_t * root, * header;

    root_pgnum = make_leaf(table_id);

    buffer_read_page(table_id, root_pgnum, &root);
    buffer_read_page(table_id, 0, &header);

    root->slots[0].key = key;
    root->slots[0].size = val_size;
    root->slots[0].offset = PAGE_SIZE - val_size;
    root->slots[0].trx_id = 0;
    memcpy(root->values + FREE_SPACE - val_size, value, val_size);
    root->free_space -= (SLOT_SIZE + val_size);
    root->parent = 0;
    root->num_keys++;
    header->root_num = root_pgnum;

    buffer_write_page(table_id, root_pgnum, &root);
    buffer_write_page(table_id, 0, &header);
}

void insert_into_new_root(int64_t table_id,
                          pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t root_pgnum;
    page_t * left, * right, * root, * header;
    
    root_pgnum = make_page(table_id);

    buffer_read_page(table_id, left_pgnum, &left);
    buffer_read_page(table_id, right_pgnum, &right);
    buffer_read_page(table_id, root_pgnum, &root);
    buffer_read_page(table_id, 0, &header);

    root->left_child = left_pgnum;
    root->entries[0].key = key;
    root->entries[0].child = right_pgnum;
    root->num_keys++;
    root->parent = 0;
    left->parent = root_pgnum;
    right->parent = root_pgnum;
    header->root_num = root_pgnum;

    buffer_write_page(table_id, left_pgnum, &left);
    buffer_write_page(table_id, right_pgnum, &right);
    buffer_write_page(table_id, root_pgnum, &root);
    buffer_write_page(table_id, 0, &header);
}

pagenum_t make_leaf(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t * new_leaf;
    new_pgnum = make_page(table_id);
    buffer_read_page(table_id, new_pgnum, &new_leaf);
    new_leaf->is_leaf = 1;
    new_leaf->free_space = FREE_SPACE;
    new_leaf->sibling = 0;
    buffer_write_page(table_id, new_pgnum, &new_leaf);
    return new_pgnum;
}

pagenum_t make_page(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t * new_page;
    new_pgnum = buffer_alloc_page(table_id);
    buffer_read_page(table_id, new_pgnum, &new_page);
    new_page->parent = 0;
    new_page->is_leaf = 0;
    new_page->num_keys = 0;
    buffer_write_page(table_id, new_pgnum, &new_page);
    return new_pgnum;
}

int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum) {
    page_t * parent;
    int left_index;
    int parent_buffer_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    left_index = 0;
    if (parent->left_child == left_pgnum) {
        pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));
        return left_index;
    }
    while (parent->entries[left_index].child != left_pgnum) {
        left_index++;
    }
    pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));
    return ++left_index;
}

// Deletion

int db_delete(int64_t table_id, int64_t key) {
    pagenum_t leaf_pgnum, sibling_pgnum, parent_pgnum;;
    page_t * leaf, * sibling, * parent;
    int sibling_index, k_prime_index;
    int leaf_num_keys, leaf_free_space, sibling_free_space;
    int64_t k_prime;

    if (db_find(table_id, key, NULL, NULL, 0) == -1) return -1;

    leaf_pgnum = find_leaf(table_id, key);
    
    delete_from_leaf(table_id, leaf_pgnum, key);

    int leaf_buffer_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
    parent_pgnum = leaf->parent;
    leaf_num_keys = leaf->num_keys;
    leaf_free_space = leaf->free_space;
    pthread_mutex_unlock(&(buffers[leaf_buffer_idx]->page_latch));

    if (parent_pgnum == 0) {
        if (leaf_num_keys > 0) return 0;
        end_tree(table_id, leaf_pgnum);
        return 0;
    }

    if (leaf_free_space < THRESHOLD) {
        return 0;
    }

    sibling_index = get_sibling_index(table_id, parent_pgnum, leaf_pgnum);

    int parent_buffer_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent->entries[k_prime_index].key;
    if (sibling_index == -1) sibling_pgnum = parent->entries[0].child;
    else if (sibling_index == 0) sibling_pgnum = parent->left_child;
    else sibling_pgnum = parent->entries[sibling_index - 1].child;
    pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));

    int sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);
    sibling_free_space = sibling->free_space;
    pthread_mutex_unlock(&(buffers[sibling_buffer_idx]->page_latch));
    
    if (sibling_free_space + leaf_free_space >= FREE_SPACE) {
        merge_leaves(table_id, leaf_pgnum,
                     sibling_pgnum, sibling_index, k_prime);
    }
    else {
        redistribute_leaves(table_id, leaf_pgnum,
                            sibling_pgnum, sibling_index, k_prime_index);
    }

    return 0;
}

void delete_from_leaf(int64_t table_id, pagenum_t leaf_pgnum, int64_t key) {
    page_t * leaf;
    int i, key_index;
    uint16_t val_size, insertion_offset, deletion_offset;

    buffer_read_page(table_id, leaf_pgnum, &leaf);
    
    key_index = 0;
    while (leaf->slots[key_index].key != key) key_index++;
    
    val_size = leaf->slots[key_index].size;
    deletion_offset = leaf->slots[key_index].offset - HEADER_SIZE;
    insertion_offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;

    for (i = key_index + 1; i < leaf->num_keys; i++) {
        leaf->slots[i - 1].key = leaf->slots[i].key;
        leaf->slots[i - 1].size = leaf->slots[i].size;
        leaf->slots[i - 1].offset = leaf->slots[i].offset;
        leaf->slots[i - 1].trx_id = leaf->slots[i].trx_id;
    }
    memmove(leaf->values + insertion_offset + val_size,
            leaf->values + insertion_offset,
            deletion_offset - insertion_offset);

    leaf->num_keys--;
    leaf->free_space += (SLOT_SIZE + val_size);

    for (i = 0; i < leaf->num_keys; i++) {
        if (leaf->slots[i].offset - HEADER_SIZE < deletion_offset) {
            leaf->slots[i].offset += val_size;
        }
    }

    buffer_write_page(table_id, leaf_pgnum, &leaf);
}

void merge_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                  pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    pagenum_t parent_pgnum;
    page_t * leaf, * sibling;
    int i, j, leaf_size, sibling_size;
    uint16_t leaf_offset, sibling_offset;

    int leaf_buffer_idx, sibling_buffer_idx;
    if (sibling_index != -1) {
        leaf_buffer_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
        sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &leaf);
        leaf_buffer_idx = buffer_read_page(table_id, leaf_pgnum, &sibling);
    }

    sibling_offset = sibling->free_space + SLOT_SIZE * sibling->num_keys;
    sibling_size = FREE_SPACE - sibling_offset;
    leaf_offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;
    leaf_size = FREE_SPACE - leaf_offset;

    for (i = 0, j = sibling->num_keys; i < leaf->num_keys; i++, j++) {
        sibling->slots[j].key = leaf->slots[i].key;
        sibling->slots[j].size = leaf->slots[i].size;
        sibling->slots[j].offset = leaf->slots[i].offset - sibling_size;
        sibling->slots[j].trx_id = leaf->slots[i].trx_id;
        sibling->num_keys++;
        sibling->free_space -= (leaf->slots[i].size + SLOT_SIZE);
    }
    memcpy(sibling->values + sibling_offset - leaf_size,
            leaf->values + leaf_offset, leaf_size);
    sibling->sibling = leaf->sibling;

    parent_pgnum = leaf->parent;

    if (sibling_index != -1) {
        pthread_mutex_unlock(&(buffers[leaf_buffer_idx]->page_latch));
        buffer_write_page(table_id, sibling_pgnum, &sibling);
        delete_from_child(table_id, parent_pgnum, k_prime, leaf_pgnum);
    }
    else {
        pthread_mutex_unlock(&(buffers[sibling_buffer_idx]->page_latch));
        buffer_write_page(table_id, leaf_pgnum, &sibling);
        delete_from_child(table_id, parent_pgnum, k_prime, sibling_pgnum);
    }
}

void redistribute_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                         pagenum_t sibling_pgnum, int sibling_index, int k_prime_index) {
    page_t * leaf, * sibling, * parent;
    int i, src_index, dest_index;
    int64_t rotate_key;
    int16_t src_size, dest_offset;

    buffer_read_page(table_id, leaf_pgnum, &leaf);
    int sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);

    while (leaf->free_space >= THRESHOLD) {
        src_index = (sibling_index != -1) ? sibling->num_keys - 1 : 0;
        dest_index = (sibling_index != -1) ? 0 : leaf->num_keys;
        src_size = sibling->slots[src_index].size;
        dest_offset = leaf->free_space + SLOT_SIZE * leaf->num_keys - src_size;

        if (sibling_index != -1) {
            for (i = leaf->num_keys; i > 0; i--) {
                leaf->slots[i].key = leaf->slots[i - 1].key;
                leaf->slots[i].size = leaf->slots[i - 1].size;
                leaf->slots[i].offset = leaf->slots[i - 1].offset;
                leaf->slots[i].trx_id = leaf->slots[i - 1].trx_id;
            }
        }
        leaf->slots[dest_index].key = sibling->slots[src_index].key;
        leaf->slots[dest_index].size = sibling->slots[src_index].size;
        leaf->slots[dest_index].offset = dest_offset + HEADER_SIZE;
        leaf->slots[dest_index].trx_id = sibling->slots[src_index].trx_id;
        memcpy(leaf->values + dest_offset,
                sibling->values + sibling->slots[src_index].offset - HEADER_SIZE,
                src_size);
        
        leaf->num_keys++;
        leaf->free_space -= (SLOT_SIZE + src_size);
        rotate_key = sibling->slots[src_index].key;

        pthread_mutex_unlock(&(buffers[sibling_buffer_idx]->page_latch));
        delete_from_leaf(table_id, sibling_pgnum, rotate_key);
        sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);
    }
    
    buffer_read_page(table_id, leaf->parent, &parent);
    parent->entries[k_prime_index].key = (sibling_index != -1) ?
                                        leaf->slots[0].key :
                                        sibling->slots[0].key;
    buffer_write_page(table_id, leaf->parent, &parent);

    buffer_write_page(table_id, leaf_pgnum, &leaf);
    buffer_write_page(table_id, sibling_pgnum, &sibling);
}

void delete_from_child(int64_t table_id,
                       pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum) {
    pagenum_t sibling_pgnum, parent_pgnum;
    page_t * p, * sibling, * parent;
    int sibling_index, k_prime_index;
    int p_num_keys, sibling_num_keys;
    int64_t k_prime;

    delete_from_page(table_id, p_pgnum, key, child_pgnum);

    int p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
    parent_pgnum = p->parent;
    p_num_keys = p->num_keys;
    pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));

    if (parent_pgnum == 0) {
        if (p_num_keys > 0) return;
        adjust_root(table_id, p_pgnum);
        return;
    }

    if (p_num_keys >= ENTRY_ORDER / 2) {
        return;
    }

    sibling_index = get_sibling_index(table_id, parent_pgnum, p_pgnum);

    int parent_buffer_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent->entries[k_prime_index].key;
    if (sibling_index == 0) sibling_pgnum = parent->left_child;
    else if (sibling_index == -1) sibling_pgnum = parent->entries[0].child;
    else sibling_pgnum = parent->entries[sibling_index - 1].child;
    pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));

    int sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);
    sibling_num_keys = sibling->num_keys;
    pthread_mutex_unlock(&(buffers[sibling_buffer_idx]->page_latch));

    if (sibling_num_keys + p_num_keys < ENTRY_ORDER - 1) {
        merge_pages(table_id, p_pgnum, sibling_pgnum, sibling_index, k_prime);
    }
    else {
        redistribute_pages(table_id, p_pgnum,
                           sibling_pgnum, sibling_index, k_prime_index, k_prime);
    }
}

void delete_from_page(int64_t table_id, pagenum_t p_pgnum,
                      int64_t key, pagenum_t child_pgnum) {
    page_t * p;
    int i;

    buffer_read_page(table_id, p_pgnum, &p);

    i = 0;
    while (p->entries[i].key != key) i++;
    for (++i; i < p->num_keys; i++) {
        p->entries[i - 1].key = p->entries[i].key;
    }

    i = 0;
    if (p->left_child != child_pgnum) {
        i++;
        while (p->entries[i - 1].child != child_pgnum) i++;
    }
    for (; i < p->num_keys; i++) {
        if (i == 0) p->left_child = p->entries[0].child;
        else p->entries[i - 1].child = p->entries[i].child;
    }

    p->num_keys--;

    buffer_write_page(table_id, p_pgnum, &p);

    buffer_free_page(table_id, child_pgnum);
}

void merge_pages(int64_t table_id, pagenum_t p_pgnum,
                 pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    pagenum_t parent_pgnum;
    page_t * p, * sibling, * nephew;
    int i, j, insertion_index, p_end;
    
    int p_buffer_idx, sibling_buffer_idx;
    if (sibling_index != -1) {
        p_buffer_idx = buffer_read_page(table_id, p_pgnum, &p);
        sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        sibling_buffer_idx = buffer_read_page(table_id, sibling_pgnum, &p);
        p_buffer_idx = buffer_read_page(table_id, p_pgnum, &sibling);
    }

    insertion_index = sibling->num_keys;
    sibling->entries[insertion_index].key = k_prime;
    sibling->num_keys++;
    p_end = p->num_keys;

    sibling->entries[insertion_index].child = p->left_child;
    for (i = insertion_index + 1, j = 0; j < p_end; i++, j++) {
        sibling->entries[i].key = p->entries[j].key;
        sibling->entries[i].child = p->entries[j].child;
        sibling->num_keys++;
    }

    buffer_read_page(table_id, sibling->left_child, &nephew);
    nephew->parent = (sibling_index != -1) ? sibling_pgnum : p_pgnum;
    buffer_write_page(table_id, sibling->left_child, &nephew);
    for (i = 0; i < sibling->num_keys; i++) {
        buffer_read_page(table_id, sibling->entries[i].child, &nephew);
        nephew->parent = (sibling_index != -1) ? sibling_pgnum : p_pgnum;
        buffer_write_page(table_id, sibling->entries[i].child, &nephew);
    }

    parent_pgnum = p->parent;

    if (sibling_index != -1) {
        pthread_mutex_unlock(&(buffers[p_buffer_idx]->page_latch));
        buffer_write_page(table_id, sibling_pgnum, &sibling);
        delete_from_child(table_id, parent_pgnum, k_prime, p_pgnum);
    }
    else {
        pthread_mutex_unlock(&(buffers[sibling_buffer_idx]->page_latch));
        buffer_write_page(table_id, p_pgnum, &sibling);
        delete_from_child(table_id, parent_pgnum, k_prime, sibling_pgnum);
    }
}

void redistribute_pages(int64_t table_id, pagenum_t p_pgnum,
                        pagenum_t sibling_pgnum, int sibling_index,
                        int k_prime_index, int64_t k_prime) {
    page_t * p, * sibling, * parent, * child;
    int i;

    buffer_read_page(table_id, p_pgnum, &p);
    buffer_read_page(table_id, sibling_pgnum, &sibling);

    if (sibling_index != -1) {
        for (i = p->num_keys; i > 0; i--) {
            p->entries[i].key = p->entries[i - 1].key;
            p->entries[i].child = p->entries[i - 1].child;
        }
        p->entries[0].child = p->left_child;

        p->left_child = sibling->entries[sibling->num_keys - 1].child;
        buffer_read_page(table_id, p->left_child, &child);
        child->parent = p_pgnum;
        buffer_write_page(table_id, p->left_child, &child);
        p->entries[0].key = k_prime;

        buffer_read_page(table_id, p->parent, &parent);
        parent->entries[k_prime_index].key = sibling->entries[sibling->num_keys - 1].key;
        buffer_write_page(table_id, p->parent, &parent);
    }

    else {
        p->entries[p->num_keys].key = k_prime;
        p->entries[p->num_keys].child = sibling->left_child;
        buffer_read_page(table_id, p->entries[p->num_keys].child, &child);
        child->parent = p_pgnum;
        buffer_write_page(table_id, p->entries[p->num_keys].child, &child);

        buffer_read_page(table_id, p->parent, &parent);
        parent->entries[k_prime_index].key = sibling->entries[0].key;
        buffer_write_page(table_id, p->parent, &parent);

        sibling->left_child = sibling->entries[0].child;
        for (i = 0; i < sibling->num_keys - 1; i++) {
            sibling->entries[i].key = sibling->entries[i + 1].key;
            sibling->entries[i].child = sibling->entries[i + 1].child;
        }
    }

    p->num_keys++;
    sibling->num_keys--;

    buffer_write_page(table_id, p_pgnum, &p);
    buffer_write_page(table_id, sibling_pgnum, &sibling);
}

void end_tree(int64_t table_id, pagenum_t root_pgnum) {
    page_t * header;
    buffer_read_page(table_id, 0, &header);

    header->root_num = 0;

    buffer_write_page(table_id, 0, &header);
    buffer_free_page(table_id, root_pgnum);
}

void adjust_root(int64_t table_id, pagenum_t root_pgnum) {
    page_t * root, * new_root, * header;

    int root_buffer_idx = buffer_read_page(table_id, root_pgnum, &root);
    buffer_read_page(table_id, root->left_child, &new_root);
    buffer_read_page(table_id, 0, &header);

    new_root->parent = 0;
    header->root_num = root->left_child;

    buffer_write_page(table_id, 0, &header);
    buffer_write_page(table_id, root->left_child, &new_root);
    pthread_mutex_unlock(&(buffers[root_buffer_idx]->page_latch));

    buffer_free_page(table_id, root_pgnum);
}

int get_sibling_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t p_pgnum) {
    page_t * parent;
    int sibling_index;
    int parent_buffer_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    sibling_index = -1;
    if (parent->left_child == p_pgnum) {
        pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));
        return sibling_index;
    }
    do {
        sibling_index++;
    } while (parent->entries[sibling_index].child != p_pgnum);
    pthread_mutex_unlock(&(buffers[parent_buffer_idx]->page_latch));
    return sibling_index;
}
