#include "../include/bpt.h"
#include "../include/file.h"

/* Opens existing data file using pathname,
 * or creates one if not existed.
 * If success, returns the unique table id.
 * Otherwise, returns -1.
 */
int64_t open_table(char* pathname) {
    int64_t table_id = file_open_database_file(pathname);
    return table_id;
}

/* Initializes DBMS.
 * If success, returns 0. Otherwise, returns -1. 
 */
int init_db() {
    open_table((char*)"my_db.db");
    return 0;
}

/* Shutdowns DBMS.
 * If success, returns 0. Otherwise, returns -1.
 */
int shutdown_db() {
    file_close_database_file();
    return 0;
}

// OUTPUT AND UTILITIES

/* Traces the path from the root to a leaf, searching by key.
 * Returns a page # of the leaf containg the given key.
 */
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

/* Finds the record containing a key.
 * Stores value & size corresponding to the key.
 * If success, returns 0. Otherwise, returns -1.
 */
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
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
    
    memmove(ret_val, page.values + page.slots[i].offset - HEADER_SIZE,
            page.slots[i].size);
    *val_size = page.slots[i].size;
    return 0;
}

// INSERTION

/* Creates a new general page, which can be adapted
 * to serve as either a leaf or an internal page.
 */
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

/* Creates a new leaf by creating a page
 * and then adpapting it appropriately.
 */
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

/* Hepler function used in insert_into_parent
 * to find the index of the parent's child to
 * the page to the left of the key to be inserted.
 */
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

/* Inserts new key & value into a leaf.
 */
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

/* Inserts new key & value into a leaf so as to exceed
 * the page's size, causing the leaf to be split in half.
 */
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
        total_size += (SLOT_SIZE + temp_slots[i].size);
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

/* Insert new key & child into a page.
 */
void insert_into_page(int64_t table_id, pagenum_t p_pgnum, int left_index,
                      int64_t key, pagenum_t right_pgnum) {
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

/* Inserts new key & child into a page so as to exceed
 * the entry order, causing the page to be split in half.
 */
void insert_into_page_split(int64_t table_id, pagenum_t old_pgnum, int left_index,
                            int64_t key, pagenum_t right_pgnum) {
    pagenum_t new_pgnum;
    page_t old_page, right, new_page, child;
    int i, j, split, k_prime;
    pagenum_t left_child;
    entry_t temp[ENTRY_ORDER];
    
    file_read_page(table_id, old_pgnum, &old_page);
    file_read_page(table_id, right_pgnum, &right);

    /* First create a temporary set of keys & childs
     * to hold everything in order, including
     * the new key & child, inserted in right places.
     * Then create a new page and copy half of the
     * keys & childs to the old page and
     * the other half to the new page.
    */

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

    /* Create the new page and copy
     * half the keys & childs to the
     * old and half to the new.
     */

    split = ENTRY_ORDER / 2 + 1;
    new_pgnum = make_page(table_id);

    file_read_page(table_id, new_pgnum, &new_page);
    
    old_page.num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        if (i) old_page.entries[i - 1].child = temp[i - 1].child;
        else old_page.left_child = left_child;
        old_page.entries[i].key = temp[i].key;
        old_page.num_keys++;
    }
    old_page.entries[i - 1].child = temp[i - 1].child;
    k_prime = temp[split - 1].key;
    for (++i, j = 0; i < ENTRY_ORDER; i++, j++) {
        if (j) new_page.entries[j - 1].child = temp[i - 1].child;
        else new_page.left_child = temp[i- 1].child;
        new_page.entries[j].key = temp[i].key;
        new_page.num_keys++;
    }
    new_page.entries[j - 1].child = temp[i - 1].child;
    new_page.parent = old_page.parent;

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

    /* Insert a new key into the parent of the two
     * pages resulting from the split, with
     * the old node to the left and the new to the right.
     */

    insert_into_parent(table_id, old_pgnum, k_prime, new_pgnum);
}

/* Inserts a new page (leaf or internal) into data file.
 */
void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t parent_pgnum;
    page_t left, right, parent;
    int left_index;

    file_read_page(table_id, left_pgnum, &left);
    file_read_page(table_id, right_pgnum, &right);

    parent_pgnum = left.parent;

    file_read_page(table_id, parent_pgnum, &parent);

    /* Case: new root. */
    if (parent_pgnum == 0) {
        insert_into_new_root(table_id, left_pgnum, key, right_pgnum);
        return;
    }

    /* Case: leaf or page.(Rest of function body)
     */

    /* Find the index of parent's child to the left page.
     */
    left_index = get_left_index(table_id, parent_pgnum, left_pgnum);
    
    /* Simple case: the new key fits into the node.
     */
    if (parent.num_keys < ENTRY_ORDER - 1) {
        insert_into_page(table_id, parent_pgnum, left_index, key, right_pgnum);
    }

    /* Harder case: split a node by insertion rule.
     */
    else {
        insert_into_page_split(table_id, parent_pgnum, left_index,
                                         key, right_pgnum);
    }
    
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into the new root.
 */
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

/* Starts a new tree.
 */
void start_new_tree(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
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

/* Master insertion function.
 * Inserts key & value into data file at the right place.
 * If success, returns 0. Otherwise, returns -1.
 */
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t leaf_pgnum;
    page_t leaf, header;

    // Duplicate?

    file_read_page(table_id, 0, &header);

    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    if (header.root_num == 0) {
        start_new_tree(table_id, key, value, val_size);
        return 0;
    }

    /* Case: the tree already exists.
     * (Rest of function body)
     */

    leaf_pgnum = find_leaf(table_id, key);

    file_read_page(table_id, leaf_pgnum, &leaf);

    /* Case: enough free space to insert.
     * Nothing to do.
     */
    if (leaf.free_space >= SLOT_SIZE + val_size) {
        insert_into_leaf(table_id, leaf_pgnum, key, value, val_size);
    }

    /* Case: no room for the new record.
     * Need to be split.
     */
    else {
        insert_into_leaf_split(table_id, leaf_pgnum, key, value, val_size);
    }

    return 0;
}

// Deletion

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

    printf("Search for nonexistent pointer to node in parent.\n");
    exit(EXIT_FAILURE);
}

void delete_from_page(int64_t table_id, pagenum_t p_pgnum,
                            int64_t key, pagenum_t child_pgnum) {
    page_t p;
    int i;

    file_read_page(table_id, p_pgnum, &p);

    i = 0;
    while (p.entries[i].key != key) {
        i++;
    }
    for (++i; i < p.num_keys; i++) {
        p.entries[i - 1].key = p.entries[i].key;
    }

    i = 0;
    if (p.left_child != child_pgnum) {
        i++;
        while (p.entries[i - 1].child != child_pgnum) {
            i++;
        }
    }
    for (++i; i <= p.num_keys; i++) {
        if (i == 1) p.left_child = p.entries[i - 1].child;
        else p.entries[i - 2].child = p.entries[i - 1].child;
    }

    p.num_keys--;

    file_write_page(table_id, p_pgnum, &p);
}

void delete_from_leaf(int64_t table_id, pagenum_t leaf_pgnum, int key_index) {
    page_t leaf;
    int i;
    uint16_t val_size, offset, deletion_offset;

    file_read_page(table_id, leaf_pgnum, &leaf);
    
    deletion_offset = leaf.slots[key_index].offset - HEADER_SIZE;
    offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;
    val_size = leaf.slots[key_index].size;

    for (i = key_index + 1; i < leaf.num_keys; i++) {
        leaf.slots[i - 1].key = leaf.slots[i].key;
        leaf.slots[i - 1].size = leaf.slots[i].size;
        leaf.slots[i - 1].offset = leaf.slots[i].offset;
    }
    memmove(leaf.values + offset + val_size, leaf.values + offset,
            deletion_offset - offset);

    leaf.num_keys--;
    leaf.free_space += (SLOT_SIZE + val_size);

    for (i = 0; i < leaf.num_keys; i++) {
        if (leaf.slots[i].offset - HEADER_SIZE < deletion_offset) {
            leaf.slots[i].offset += val_size;
        }
    }

    file_write_page(table_id, leaf_pgnum, &leaf);
}

void delete_from_leaf_merge(int64_t table_id, pagenum_t leaf_pgnum,
                            pagenum_t sibling_pgnum, int sibling_index,
                            int64_t k_prime, int key_index) {
    pagenum_t temp_pgnum;
    page_t leaf, sibling;
    int i, j;
    uint16_t sibling_offset, leaf_offset;
    int leaf_size, sibling_size;

    delete_from_leaf(table_id, leaf_pgnum, key_index);

    if (sibling_index == -1) {
        file_read_page(table_id, sibling_pgnum, &leaf);
        file_read_page(table_id, leaf_pgnum, &sibling);
    }
    else {
        file_read_page(table_id, leaf_pgnum, &leaf);
        file_read_page(table_id, sibling_pgnum, &sibling);
    }

    sibling_offset = sibling.free_space + SLOT_SIZE * sibling.num_keys;
    sibling_size = FREE_SPACE - sibling_offset;
    leaf_offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;
    leaf_size = FREE_SPACE - leaf_offset;

    for (i = 0, j = sibling.num_keys; i < leaf.num_keys; i++, j++) {
        sibling.slots[j].key = leaf.slots[i].key;
        sibling.slots[j].size = leaf.slots[i].size;
        sibling.slots[j].offset = leaf.slots[i].offset - sibling_size;
    }
    memmove(sibling.values + sibling_offset - leaf_size,
            leaf.values + leaf_offset, leaf_size);

    sibling.free_space -= (leaf_size + SLOT_SIZE * leaf.num_keys);
    leaf.free_space = FREE_SPACE;
    sibling.num_keys += leaf.num_keys;
    leaf.num_keys = 0;

    file_write_page(table_id, leaf_pgnum, &leaf);
    file_write_page(table_id, sibling_pgnum, &sibling);

    // delete_from_child(table_id, leaf.parent, k_prime, leaf_pgnum);
}

void delete_from_leaf_rotate(int64_t table_id, pagenum_t leaf_pgnum,
                             pagenum_t sibling_pgnum, int sibling_index,
                             int k_prime_index, int64_t k_prime, int key_index) {
    page_t leaf, sibling, parent;
    int i, rotation_index, rotation_size;
    int16_t offset;

    file_read_page(table_id, sibling_pgnum, &sibling);

    delete_from_leaf(table_id, leaf_pgnum, key_index);

    offset = leaf.free_space + SLOT_SIZE * leaf.num_keys;
    for (rotation_index = sibling.num_keys - 1; rotation_index >= 0; rotation_index--) {
        file_read_page(table_id, leaf_pgnum, &leaf);
        file_read_page(table_id, sibling_pgnum, &sibling);
        file_read_page(table_id, leaf.parent, &parent);

        if (leaf.free_space < 2500) break;

        rotation_size = sibling.slots[rotation_index].size;
        offset -= rotation_size;
        for (i = leaf.num_keys; i > 0; i--) {
            leaf.slots[i].key = leaf.slots[i - 1].key;
            leaf.slots[i].size = leaf.slots[i - 1].size;
            leaf.slots[i].offset = leaf.slots[i - 1].offset;
        }
        leaf.slots[0].key = sibling.slots[rotation_index].key;
        leaf.slots[0].size = sibling.slots[rotation_index].size;
        leaf.slots[0].offset = offset + HEADER_SIZE;
        memmove(leaf.values + offset,
                sibling.values + sibling.slots[rotation_index].offset - HEADER_SIZE,
                rotation_size);
        
        // parent.entries[k_prime_index].key = leaf.slots[0].key;
        leaf.num_keys++;
        leaf.free_space -= (SLOT_SIZE + rotation_size);

        delete_from_leaf(table_id, sibling_pgnum, rotation_index);

        file_write_page(table_id, leaf_pgnum, &leaf);
        file_write_page(table_id, leaf.parent, &parent);
    }
}

void delete_from_child(int64_t table_id, pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum) {

}

int db_delete(int64_t table_id, int64_t key) {
    pagenum_t leaf_pgnum, sibling_pgnum;
    page_t leaf, sibling, parent;
    int i, sibling_index, k_prime_index, val_size;
    int64_t k_prime;

    leaf_pgnum = find_leaf(table_id, key);
    file_read_page(table_id, leaf_pgnum, &leaf);
    sibling_index = get_sibling_index(table_id, leaf_pgnum);
    file_read_page(table_id, leaf.parent, &parent);

    k_prime_index = (sibling_index == -1) ? 0 : sibling_index;
    k_prime = parent.entries[k_prime_index].key;

    if (sibling_index == 0) sibling_pgnum = parent.left_child;
    else if (sibling_index == -1) sibling_pgnum = parent.entries[0].child;
    else sibling_pgnum = parent.entries[sibling_index -1].child;
    file_read_page(table_id, sibling_pgnum, &sibling);

    i = 0;
    while (leaf.slots[i].key != key) i++;
    val_size = leaf.slots[i].size;

    if (leaf.free_space + SLOT_SIZE + val_size < 2500) {
        delete_from_leaf(table_id, leaf_pgnum, i); // i?
    }
    else if (sibling.free_space + leaf.free_space >= FREE_SPACE + SLOT_SIZE + val_size) {
        delete_from_leaf_merge(table_id, leaf_pgnum,
                               sibling_pgnum, sibling_index, k_prime, i);
    }
    else {
        delete_from_leaf_rotate(table_id, leaf_pgnum, sibling_pgnum, sibling_index,
                                k_prime_index, k_prime, i);
    }
    return 0;
}


void insert_into_leaf();
void insert_into_leaf_split();
void insert_into_page();
void insert_into_page_split();
void insert_into_parent();
void insert_into_new_root();
void start_new_tree();
void db_insert();

void remove_from_leaf();
void remove_from_leaf_merge();
void remove_from_leaf_rotate();
void remove_from_page();
void remove_from_page_merge();
void remove_from_page_rotate();
void remove_from_chlid();
void adjust_root();
void destroy_tree();
void db_delete();