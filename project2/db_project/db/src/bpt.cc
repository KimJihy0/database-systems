#include "bpt.h"
#include "file.h"
#include "hash.h"

// DBMS

int init_db() {
    memset(tables, 0x00, NUM_TABLES * sizeof(table_t));
    return 0;
}

int shutdown_db() {
    file_close_table_file();
    memset(tables, 0x00, NUM_TABLES * sizeof(table_t));
    return 0;
}

int64_t open_table(char* pathname) {
    int64_t table_id;
    table_id = file_open_table_file(pathname);
    if (table_id < 0) return -1;
    return table_id;
}

// SEARCH

int db_find(int64_t table_id, int64_t key,
            char* ret_val = NULL, uint16_t* val_size = NULL) {
    pagenum_t page_num;
    page_t page;
    int i;
    page_num = find_leaf(table_id, key);
    if (page_num == 0) return -1;
    file_read_page(table_id, page_num, &page);
    for (i = 0; i < page.num_keys; i++) {
        if (page.slots[i].key == key) break;
    }
    if (i == page.num_keys) return -1;
    if (ret_val == NULL || val_size == NULL) return 1;
    memmove(ret_val, page.values + page.slots[i].offset - HEADER_SIZE, page.slots[i].size);
    *val_size = page.slots[i].size;
    return 0;
}

pagenum_t find_leaf(int64_t table_id, int64_t key) {
    int i;
    pagenum_t temp_pgnum;
    page_t temp, header;
    file_read_page(table_id, 0, &header);
    temp_pgnum = header.root_num;
    file_read_page(table_id, temp_pgnum, &temp);
    if (temp_pgnum == 0) return 0;
    while (!temp.is_leaf) {
        i = 0;
        while (i < temp.num_keys) {
            if (key >= temp.entries[i].key) i++;
            else break;
        }
        temp_pgnum = i ? temp.entries[i - 1].child : temp.left_child;
        file_read_page(table_id, temp_pgnum, &temp);
    }
    return temp_pgnum;
}

// INSERTION

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t leaf_pgnum;
    page_t leaf, header;

    if (val_size < 50 || val_size > 112) {
        printf("val_size should be between 50 and 120.\n");
        return -2;
    }

    if (db_find(table_id, key) == 1) return -1;

    file_read_page(table_id, 0, &header);
    if (header.root_num == 0) {
        start_tree(table_id, key, value, val_size);
        return 0;
    }

    leaf_pgnum = find_leaf(table_id, key);

    file_read_page(table_id, leaf_pgnum, &leaf);

    if (leaf.free_space >= SLOT_SIZE + val_size) {
        insert_into_leaf(table_id, leaf_pgnum, key, value, val_size);
    }
    else {
        insert_into_leaf_split(table_id, leaf_pgnum, key, value, val_size);
    }

    return 0;
}

void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char* value, uint16_t val_size) {
    page_t leaf;
    int i, insertion_point;
    uint16_t offset;
    
    file_read_page(table_id, leaf_pgnum, &leaf);
    
    insertion_point = 0;
    while (insertion_point < leaf.num_keys && leaf.slots[insertion_point].key < key) {
        insertion_point++;
    }
    offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;

    for (i = leaf.num_keys; i > insertion_point; i--) {
        leaf.slots[i].key = leaf.slots[i - 1].key;
        leaf.slots[i].size = leaf.slots[i - 1].size;
        leaf.slots[i].offset = leaf.slots[i - 1].offset;
    }
    leaf.slots[insertion_point].key = key;
    leaf.slots[insertion_point].size = val_size;
    leaf.slots[insertion_point].offset = offset - val_size + HEADER_SIZE;
    memmove(leaf.values + offset - val_size, value, val_size);

    leaf.num_keys++;
    leaf.free_space -= (SLOT_SIZE + val_size);

    file_write_page(table_id, leaf_pgnum, &leaf);
}

void insert_into_leaf_split(int64_t table_id, pagenum_t leaf_pgnum,
                            int64_t key, char* value, uint16_t val_size) {
    pagenum_t new_pgnum;
    page_t leaf, new_leaf;
    slot_t temp_slots[65];
    char temp_values[3968];
    int insertion_index, split, new_key, i, j;
    uint16_t offset;
    int total_size, num_keys;
    
    new_pgnum = make_leaf(table_id);

    file_read_page(table_id, leaf_pgnum, &leaf);
    file_read_page(table_id, new_pgnum, &new_leaf);

    insertion_index = 0;
    while (insertion_index < leaf.num_keys && leaf.slots[insertion_index].key < key) {
        insertion_index++;
    }
    offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;

    for (i = 0, j = 0; i < leaf.num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_slots[j].key = leaf.slots[i].key;
        temp_slots[j].size = leaf.slots[i].size;
        temp_slots[j].offset = leaf.slots[i].offset;
    }
    temp_slots[insertion_index].key = key;
    temp_slots[insertion_index].size = val_size;
    temp_slots[insertion_index].offset = offset - val_size + HEADER_SIZE;
    memmove(temp_values + offset, leaf.values + offset, FREE_SPACE - offset);
    memmove(temp_values + offset - val_size, value, val_size);

    num_keys = leaf.num_keys;
    leaf.num_keys = 0;
    leaf.free_space = FREE_SPACE;

    total_size = 0;
    for (split = 0; split <= num_keys; split++) {
        total_size += (SLOT_SIZE + temp_slots[split].size);
        if (total_size >= FREE_SPACE / 2) break;
    }

    offset = FREE_SPACE;
    for (i = 0; i < split; i++) {
        offset -= temp_slots[i].size;
        leaf.slots[i].key = temp_slots[i].key;
        leaf.slots[i].size = temp_slots[i].size;
        leaf.slots[i].offset = offset + HEADER_SIZE;
        memmove(leaf.values + offset,
                temp_values + temp_slots[i].offset - HEADER_SIZE, temp_slots[i].size);
        leaf.num_keys++;
        leaf.free_space -= (SLOT_SIZE + temp_slots[i].size);
    }
    
    offset = FREE_SPACE;
    for (i = split, j = 0; i <= num_keys; i++, j++) {
        offset -= temp_slots[i].size;
        new_leaf.slots[j].key = temp_slots[i].key;
        new_leaf.slots[j].size = temp_slots[i].size;
        new_leaf.slots[j].offset = offset + HEADER_SIZE;
        memmove(new_leaf.values + offset,
                temp_values + temp_slots[i].offset - HEADER_SIZE, temp_slots[i].size);
        new_leaf.num_keys++;
        new_leaf.free_space -= (SLOT_SIZE + temp_slots[i].size);
    }

    new_leaf.sibling = leaf.sibling;
    leaf.sibling = new_pgnum;

    new_leaf.parent = leaf.parent;
    new_key = new_leaf.slots[0].key;

    file_write_page(table_id, leaf_pgnum, &leaf);
    file_write_page(table_id, new_pgnum, &new_leaf);

    insert_into_parent(table_id, leaf_pgnum, new_key, new_pgnum);
}

void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t parent_pgnum;
    page_t left, right, parent;
    int left_index;

    file_read_page(table_id, left_pgnum, &left);
    file_read_page(table_id, right_pgnum, &right);

    parent_pgnum = left.parent;

    file_read_page(table_id, parent_pgnum, &parent);

    if (parent_pgnum == 0) {
        insert_into_new_root(table_id, left_pgnum, key, right_pgnum);
        return;
    }


    left_index = get_left_index(table_id, parent_pgnum, left_pgnum);
    
    if (parent.num_keys < ENTRY_ORDER - 1) {
        insert_into_page(table_id, parent_pgnum, left_index, key, right_pgnum);
    }

    else {
        insert_into_page_split(table_id, parent_pgnum, left_index, key, right_pgnum);
    }
    
}

void insert_into_page(int64_t table_id, pagenum_t p_pgnum,
                      int left_index, int64_t key, pagenum_t right_pgnum) {
    page_t p, right;
    int i;

    file_read_page(table_id, p_pgnum, &p);
    file_read_page(table_id, right_pgnum, &right);

    for (i = p.num_keys; i > left_index; i--) {
        p.entries[i].key = p.entries[i - 1].key;
        p.entries[i].child = p.entries[i - 1].child;
    }
    p.entries[left_index].child = right_pgnum;
    p.entries[left_index].key = key;
    p.num_keys++;

    file_write_page(table_id, p_pgnum, &p);
}

void insert_into_page_split(int64_t table_id, pagenum_t old_pgnum,
                            int left_index, int64_t key, pagenum_t right_pgnum) {
    pagenum_t new_pgnum;
    page_t old_page, right, new_page, child;
    int i, j, split, k_prime;
    pagenum_t left_child;
    entry_t temp[ENTRY_ORDER];
    
    file_read_page(table_id, old_pgnum, &old_page);
    file_read_page(table_id, right_pgnum, &right);

    left_child = old_page.left_child;
    for (i = 0, j = 0; i < old_page.num_keys; i++, j++) {
        if (j == left_index) j++;
        temp[j].child = old_page.entries[i].child;
    }
    for (i = 0, j = 0; i < old_page.num_keys; i++, j++) {
        if (j == left_index) j++;
        temp[j].key = old_page.entries[i].key;
    }
    temp[left_index].child = right_pgnum;
    temp[left_index].key = key;

    split = ENTRY_ORDER / 2 + 1;
    new_pgnum = make_page(table_id);

    file_read_page(table_id, new_pgnum, &new_page);
    
    old_page.num_keys = 0;
    old_page.left_child = left_child;
    for (i = 0; i < split - 1; i++) {
        old_page.entries[i].child = temp[i].child;
        old_page.entries[i].key = temp[i].key;
        old_page.num_keys++;
    }
    k_prime = temp[split - 1].key;
    new_page.left_child = temp[i].child;
    for (++i, j = 0; i < ENTRY_ORDER; i++, j++) {
        new_page.entries[j].child = temp[i].child;
        new_page.entries[j].key = temp[i].key;
        new_page.num_keys++;
    }

    file_read_page(table_id, new_page.left_child, &child);
    child.parent = new_pgnum;
    file_write_page(table_id, new_page.left_child, &child);
    for (i = 0; i < new_page.num_keys; i++) {
        file_read_page(table_id, new_page.entries[i].child, &child);
        child.parent = new_pgnum;
        file_write_page(table_id, new_page.entries[i].child, &child);
    }
    
    file_write_page(table_id, new_pgnum, &new_page);
    file_write_page(table_id, old_pgnum, &old_page);

    insert_into_parent(table_id, old_pgnum, k_prime, new_pgnum);
}

void start_tree(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t root_pgnum;
    page_t root, header;
    
    root_pgnum = make_leaf(table_id);

    file_read_page(table_id, root_pgnum, &root);
    file_read_page(table_id, 0, &header);

    root.slots[0].key = key;
    root.slots[0].size = val_size;
    root.slots[0].offset = PAGE_SIZE - val_size;
    memmove(root.values + FREE_SPACE - val_size, value, val_size);
    root.free_space -= (SLOT_SIZE + val_size);
    root.parent = 0;
    root.num_keys++;
    header.root_num = root_pgnum;

    file_write_page(table_id, root_pgnum, &root);
    file_write_page(table_id, 0, &header);
}

void insert_into_new_root(int64_t table_id,
                          pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t root_pgnum;
    page_t left, right, root, header;
    
    root_pgnum = make_page(table_id);

    file_read_page(table_id, left_pgnum, &left);
    file_read_page(table_id, right_pgnum, &right);
    file_read_page(table_id, root_pgnum, &root);
    file_read_page(table_id, 0, &header);

    root.left_child = left_pgnum;
    root.entries[0].key = key;
    root.entries[0].child = right_pgnum;
    root.num_keys++;
    root.parent = 0;
    left.parent = root_pgnum;
    right.parent = root_pgnum;
    header.root_num = root_pgnum;

    file_write_page(table_id, left_pgnum, &left);
    file_write_page(table_id, right_pgnum, &right);
    file_write_page(table_id, root_pgnum, &root);
    file_write_page(table_id, 0, &header);
}

pagenum_t make_leaf(int64_t table_id) {
    pagenum_t leaf_pgnum;
    page_t leaf_page;
    leaf_pgnum = make_page(table_id);
    file_read_page(table_id, leaf_pgnum, &leaf_page);
    leaf_page.is_leaf = 1;
    leaf_page.free_space = FREE_SPACE;
    leaf_page.sibling = 0;
    file_write_page(table_id, leaf_pgnum, &leaf_page);
    return leaf_pgnum;
}

pagenum_t make_page(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t new_page;
    new_pgnum = file_alloc_page(table_id);
    file_read_page(table_id, new_pgnum, &new_page);
    new_page.parent = 0;
    new_page.is_leaf = 0;
    new_page.num_keys = 0;
    file_write_page(table_id, new_pgnum, &new_page);
    return new_pgnum;
}

int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum) {
    int left_index;
    page_t parent;
    file_read_page(table_id, parent_pgnum, &parent);
    left_index = 0;
    if (parent.left_child == left_pgnum) return left_index;
    while (left_index < parent.num_keys - 1 &&
           parent.entries[left_index].child != left_pgnum) {
        left_index++;
    }
    return ++left_index;
}

// Deletion

int db_delete(int64_t table_id, int64_t key) {
    pagenum_t leaf_pgnum, sibling_pgnum;
    page_t leaf, sibling, parent;
    int sibling_index, k_prime_index;
    int64_t k_prime;

    if (db_find(table_id, key) == -1) return -1;
    leaf_pgnum = find_leaf(table_id, key);

    delete_from_leaf(table_id, leaf_pgnum, key);

    file_read_page(table_id, leaf_pgnum, &leaf);
    file_read_page(table_id, leaf.parent, &parent);

    if (leaf.parent == 0) {
        if (leaf.num_keys > 0) return 0;
        end_tree(table_id, leaf_pgnum);
        return 0;
    }

    if (leaf.free_space < 2500) {
        return 0;
    }

    sibling_index = get_sibling_index(table_id, leaf_pgnum);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent.entries[k_prime_index].key;
    if (sibling_index == -1) sibling_pgnum = parent.entries[0].child;
    else if (sibling_index == 0) sibling_pgnum = parent.left_child;
    else sibling_pgnum = parent.entries[sibling_index - 1].child;

    file_read_page(table_id, sibling_pgnum, &sibling);
    
    if (sibling.free_space + leaf.free_space >= FREE_SPACE) {
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
    page_t leaf;
    int i, key_index;
    uint16_t val_size, insertion_offset, deletion_offset;

    file_read_page(table_id, leaf_pgnum, &leaf);
    
    key_index = 0;
    while (leaf.slots[key_index].key != key) key_index++;
    
    val_size = leaf.slots[key_index].size;
    deletion_offset = leaf.slots[key_index].offset - HEADER_SIZE;
    insertion_offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;

    for (i = key_index + 1; i < leaf.num_keys; i++) {
        leaf.slots[i - 1].key = leaf.slots[i].key;
        leaf.slots[i - 1].size = leaf.slots[i].size;
        leaf.slots[i - 1].offset = leaf.slots[i].offset;
    }
    memmove(leaf.values + insertion_offset + val_size,
            leaf.values + insertion_offset,
            deletion_offset - insertion_offset);

    leaf.num_keys--;
    leaf.free_space += (SLOT_SIZE + val_size);

    for (i = 0; i < leaf.num_keys; i++) {
        if (leaf.slots[i].offset - HEADER_SIZE < deletion_offset) {
            leaf.slots[i].offset += val_size;
        }
    }

    file_write_page(table_id, leaf_pgnum, &leaf);
}

void merge_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                  pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    page_t leaf, sibling;
    int i, j, leaf_size, sibling_size;
    uint16_t leaf_offset, sibling_offset;

    if (sibling_index != -1) {
        file_read_page(table_id, leaf_pgnum, &leaf);
        file_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        file_read_page(table_id, sibling_pgnum, &leaf);
        file_read_page(table_id, leaf_pgnum, &sibling);
    }

    sibling_offset = sibling.free_space + SLOT_SIZE * sibling.num_keys;
    sibling_size = FREE_SPACE - sibling_offset;
    leaf_offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;
    leaf_size = FREE_SPACE - leaf_offset;

    for (i = 0, j = sibling.num_keys; i < leaf.num_keys; i++, j++) {
        sibling.slots[j].key = leaf.slots[i].key;
        sibling.slots[j].size = leaf.slots[i].size;
        sibling.slots[j].offset = leaf.slots[i].offset - sibling_size;
        sibling.num_keys++;
        sibling.free_space -= (leaf.slots[i].size + SLOT_SIZE);
    }
    memmove(sibling.values + sibling_offset - leaf_size,
            leaf.values + leaf_offset, leaf_size);
    sibling.sibling = leaf.sibling;

    file_write_page(table_id, sibling_pgnum, &sibling);

    delete_from_child(table_id, leaf.parent, k_prime, leaf_pgnum);
}

void redistribute_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                         pagenum_t sibling_pgnum, int sibling_index, int k_prime_index) {
    page_t leaf, sibling, parent;
    int i, src_index, dst_index;
    int16_t src_size, offset;

    file_read_page(table_id, leaf_pgnum, &leaf);
    file_read_page(table_id, sibling_pgnum, &sibling);

    while (leaf.free_space >= 2500) {
        src_index = (sibling_index != -1) ? sibling.num_keys - 1 : 0;
        dst_index = (sibling_index != -1) ? 0 : leaf.num_keys;
        src_size = sibling.slots[src_index].size;
        offset = leaf.free_space + SLOT_SIZE * leaf.num_keys - src_size;

        if (sibling_index != -1) {
            for (i = leaf.num_keys; i > 0; i--) {
                leaf.slots[i].key = leaf.slots[i - 1].key;
                leaf.slots[i].size = leaf.slots[i - 1].size;
                leaf.slots[i].offset = leaf.slots[i - 1].offset;
            }
        }
        leaf.slots[dst_index].key = sibling.slots[src_index].key;
        leaf.slots[dst_index].size = sibling.slots[src_index].size;
        leaf.slots[dst_index].offset = offset + HEADER_SIZE;
        memmove(leaf.values + offset,
                sibling.values + sibling.slots[src_index].offset - HEADER_SIZE,
                src_size);
        
        leaf.num_keys++;
        leaf.free_space -= (SLOT_SIZE + src_size);

        delete_from_leaf(table_id, sibling_pgnum, sibling.slots[src_index].key);
        file_read_page(table_id, sibling_pgnum, &sibling);
    }

    file_write_page(table_id, leaf_pgnum, &leaf);
    file_write_page(table_id, sibling_pgnum, &sibling);
    
    file_read_page(table_id, leaf.parent, &parent);
    parent.entries[k_prime_index].key = (sibling_index != -1) ?
                                        leaf.slots[0].key :
                                        sibling.slots[0].key;
    file_write_page(table_id, leaf.parent, &parent);
}

void delete_from_child(int64_t table_id,
                       pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum) {
    pagenum_t sibling_pgnum;
    page_t p, sibling, parent;
    int sibling_index, k_prime_index;
    int64_t k_prime;

    delete_from_page(table_id, p_pgnum, key, child_pgnum);

    file_read_page(table_id, p_pgnum, &p);
    file_read_page(table_id, p.parent, &parent);

    if (p.parent == 0) {
        if (p.num_keys > 0) return;
        adjust_root(table_id, p_pgnum);
        return;
    }

    if (p.num_keys >= ENTRY_ORDER / 2) {
        return;
    }

    sibling_index = get_sibling_index(table_id, p_pgnum);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent.entries[k_prime_index].key;
    if (sibling_index == 0) sibling_pgnum = parent.left_child;
    else if (sibling_index == -1) sibling_pgnum = parent.entries[0].child;
    else sibling_pgnum = parent.entries[sibling_index - 1].child;

    file_read_page(table_id, sibling_pgnum, &sibling);

    if (sibling.num_keys + p.num_keys < ENTRY_ORDER - 1) {
        merge_pages(table_id, p_pgnum, sibling_pgnum, sibling_index, k_prime);
    }
    else {
        redistribute_pages(table_id, p_pgnum,
                           sibling_pgnum, sibling_index, k_prime_index, k_prime);
    }
}

void delete_from_page(int64_t table_id, pagenum_t p_pgnum,
                      int64_t key, pagenum_t child_pgnum) {
    page_t p;
    int i;

    file_read_page(table_id, p_pgnum, &p);

    i = 0;
    while (p.entries[i].key != key) i++;
    for (++i; i < p.num_keys; i++) {
        p.entries[i - 1].key = p.entries[i].key;
    }

    i = 0;
    if (p.left_child != child_pgnum) {
        i++;
        while (p.entries[i - 1].child != child_pgnum) i++;
    }
    for (; i <= p.num_keys; i++) {
        if (i == 0) p.left_child = p.entries[0].child;
        else p.entries[i - 1].child = p.entries[i].child;
    }

    p.num_keys--;

    file_write_page(table_id, p_pgnum, &p);
    file_free_page(table_id, child_pgnum);
}

void merge_pages(int64_t table_id, pagenum_t p_pgnum,
                 pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    page_t p, sibling, nephew;
    int i, j, sibling_insertion_index, p_end;
    
    if (sibling_index != -1) {
        file_read_page(table_id, p_pgnum, &p);
        file_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        file_read_page(table_id, sibling_pgnum, &p);
        file_read_page(table_id, p_pgnum, &sibling);
    }

    sibling_insertion_index = sibling.num_keys;
    sibling.entries[sibling_insertion_index].key = k_prime;
    sibling.num_keys++;

    p_end = p.num_keys;
    for (i = sibling_insertion_index + 1, j = 0; j < p_end; i++, j++) {
        sibling.entries[i].key = p.entries[j].key;
        sibling.entries[i].child = p.entries[j].child;
        sibling.num_keys++;
    }
    sibling.entries[sibling_insertion_index].child = p.left_child;

    file_read_page(table_id, sibling.left_child, &nephew);
    nephew.parent = sibling_pgnum;
    file_write_page(table_id, sibling.left_child, &nephew);
    for (i = 0; i < sibling.num_keys; i++) {
        file_read_page(table_id, sibling.entries[i].child, &nephew);
        nephew.parent = sibling_pgnum;
        file_write_page(table_id, sibling.entries[i].child, &nephew);
    }

    file_write_page(table_id, sibling_pgnum, &sibling);

    delete_from_child(table_id, p.parent, k_prime, p_pgnum);
}

void redistribute_pages(int64_t table_id, pagenum_t p_pgnum,
                        pagenum_t sibling_pgnum, int sibling_index,
                        int k_prime_index, int64_t k_prime) {
    page_t p, sibling, parent, temp;
    int i;

    file_read_page(table_id, p_pgnum, &p);
    file_read_page(table_id, sibling_pgnum, &sibling);

    if (sibling_index != -1) {
        for (i = p.num_keys; i > 0; i--) {
            p.entries[i].key = p.entries[i - 1].key;
            p.entries[i].child = p.entries[i - 1].child;
        }
        p.entries[0].child = p.left_child;

        p.left_child = sibling.entries[sibling.num_keys - 1].child;
        file_read_page(table_id, p.left_child, &temp);
        temp.parent = p_pgnum;
        file_write_page(table_id, p.left_child, &temp);
        p.entries[0].key = k_prime;

        file_read_page(table_id, p.parent, &parent);
        parent.entries[k_prime_index].key = sibling.entries[sibling.num_keys - 1].key;
        file_write_page(table_id, p.parent, &parent);
    }

    else {
        p.entries[p.num_keys].key = k_prime;
        p.entries[p.num_keys].child = sibling.left_child;
        file_read_page(table_id, p.entries[p.num_keys].child, &temp);
        temp.parent = p_pgnum;
        file_write_page(table_id, p.entries[p.num_keys].child, &temp);

        file_read_page(table_id, p.parent, &parent);
        parent.entries[k_prime_index].key = sibling.entries[0].key;
        file_write_page(table_id, p.parent, &parent);

        sibling.left_child = sibling.entries[0].child;
        for (i = 0; i < sibling.num_keys - 1; i++) {
            sibling.entries[i].key = sibling.entries[i + 1].key;
            sibling.entries[i].child = sibling.entries[i + 1].child;
        }
    }

    p.num_keys++;
    sibling.num_keys--;

    file_write_page(table_id, p_pgnum, &p);
    file_write_page(table_id, sibling_pgnum, &sibling);
}

void end_tree(int64_t table_id, pagenum_t root_pgnum) {
    page_t header;

    file_read_page(table_id, 0, &header);

    header.root_num = 0;

    file_write_page(table_id, 0, &header);
    file_free_page(table_id, root_pgnum);
}

void adjust_root(int64_t table_id, pagenum_t root_pgnum) {
    page_t root, new_root, header;

    file_read_page(table_id, 0, &header);
    file_read_page(table_id, root_pgnum, &root);
    file_read_page(table_id, root.left_child, &new_root);

    header.root_num = root.left_child;
    new_root.parent = 0;

    file_write_page(table_id, 0, &header);
    file_free_page(table_id, root_pgnum);
    file_write_page(table_id, root.left_child, &new_root);
}

int get_sibling_index(int64_t table_id, pagenum_t p_pgnum) {
    page_t p, parent;
    int i;
    file_read_page(table_id, p_pgnum, &p);
    file_read_page(table_id, p.parent, &parent);
    if (parent.left_child == p_pgnum) return -1;
    for (i = 0; i < parent.num_keys; i++) {
        if (parent.entries[i].child == p_pgnum) {
            return i;
        }
    }
    return -2;
}
