#include "../include/bpt.h"

#define wpqkf 0
#define verbose 0

void print_buffers() {
	int i;
	printf("\n");
	for (i = 0; i < buf_size; i++) {
		if (buffers[i] != NULL) {
			printf("---buffer%2d---\n", i);
            // printf("parent: %ld\n", buffers[i]->frame.parent);
			// printf("table_id: %ld\n", buffers[i]->table_id);
			printf("page_num: %ld\n", buffers[i]->page_num);
			// printf("is_dirty: %d\n", buffers[i]->is_dirty);
			printf("is_pinned: %d\n", buffers[i]->is_pinned);
			// if (buffers[i]->next_LRU != NULL) printf("next_LRU: %ld\n", buffers[i]->next_LRU->page_num);
			// else printf("next LRU: NULL\n");
			// if (buffers[i]->prev_LRU != NULL) printf("prev_LRU: %ld\n", buffers[i]->prev_LRU->page_num);
			// else printf("prev LRU: NULL\n");
			printf("\n");
		}
		else break;
	}
	printf("\n");
}

// DBMS

/* Initializes DBMS.
 * If successes, returns 0. Otherwise, returns -1.
 */
int init_db(int num_buf) {
    int i;
    memset(tables, 0x00, NUM_TABLES * sizeof(table_t));
    buf_size = num_buf;
    buffers = (buffer_t**)malloc(buf_size * sizeof(buffer_t*));
    if (buffers == NULL) return -1;
    for (i = 0; i < buf_size; i++) {
        buffers[i] = NULL;
    }
    return 0;
}

/* Shutdowns DBMS.
 * If successes, returns 0. Otherwise, returns -1.
 */
int shutdown_db() {
    int i;
    for (i = 0; i < buf_size; i++) {
        if (buffers[i] != NULL) {
            file_write_page(buffers[i]->table_id,
                            buffers[i]->page_num,
                            &(buffers[i]->frame));
            // printf("%d,", buffers[i]->is_pinned);
            free(buffers[i]);
        }
    }
    free(buffers);
    file_close_table_file();
    return 0;
}

/* Opens existing data file using pathname,
 * or creates one if not existed.
 * If successes, returns the unique table id.
 * Otherwise, returns -1.
 */
int64_t open_table(char* pathname) {
    int64_t table_id;
    table_id = file_open_table_file(pathname);
    if (table_id < 0) {
        close(tables[table_id % NUM_TABLES].fd);
        return -1;
    }
    return table_id;
}

// SEARCH

/* Finds the record containing a key.
 * Stores value & size corresponding to the key.
 * If successes, returns 0. If finds the record but 
 * cannot store, returns 1. If cannot find, returns -1.
 */
int db_find(int64_t table_id, int64_t key,
            char* ret_val = NULL, uint16_t* val_size = NULL) {
    pagenum_t p_pgnum;
    page_t* p;
    int i, p_idx;
    p_pgnum = find_leaf(table_id, key);
    if (p_pgnum == 0) return -1;
    p_idx = buffer_read_page(table_id, p_pgnum, &p);
    for (i = 0; i < p->num_keys; i++) {
        if (p->slots[i].key == key) break;
    }
    if (i == p->num_keys) {
        if (p_idx != -1) buffers[p_idx]->is_pinned--;
        return -1;
    }
    if (ret_val == NULL || val_size == NULL) {
        if (p_idx != -1) buffers[p_idx]->is_pinned--;
        return 1;
    }
    memcpy(ret_val, p->values + p->slots[i].offset - HEADER_SIZE, p->slots[i].size);
    *val_size = p->slots[i].size;
    if (p_idx != -1) buffers[p_idx]->is_pinned--;
    return 0;
}

/* Traces the path from the root to a leaf, searching by key.
 * Returns a page # of the leaf containg the given key.
 */
pagenum_t find_leaf(int64_t table_id, int64_t key) {
    int i, p_idx;
    pagenum_t p_pgnum;
    page_t* p;
    p_pgnum = get_root_num(table_id);
    if (p_pgnum == 0) return 0;
    p_idx = buffer_read_page(table_id, p_pgnum, &p);
    #if wpqkf
    if (p_idx != -1) buffers[p_idx]->is_pinned++;
    #endif
    while (!p->is_leaf) {
        i = 0;
        while (i < p->num_keys) {
            if (key >= p->entries[i].key) i++;
            else break;
        }
        p_pgnum = i ? p->entries[i - 1].child : p->left_child;
        if (p_idx != -1) buffers[p_idx]->is_pinned--;
        p_idx = buffer_read_page(table_id, p_pgnum, &p);
        #if wpqkf
        if (p_idx != -1) buffers[p_idx]->is_pinned++;
        #endif
    }
    if (p_idx != -1) buffers[p_idx]->is_pinned--;
    return p_pgnum;
}

// INSERTION

/* Master insertion function.
 * Inserts a new record into data file,
 * and adjusts tree by insertion rule.
 * If successes, returns 0. Otherwise, returns -1.
 */
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t leaf_pgnum, root_pgnum;
    page_t* leaf;
    int free_space;
    #if verbose
    printf("-----db_insert() start-----\n");     
    print_buffers();  
    #endif

    /* Ignore duplicates.
     */
    // if (db_find(table_id, key) == 1) return -1;

    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    root_pgnum = get_root_num(table_id);
    if (root_pgnum == 0) {
        start_tree(table_id, key, value, val_size);
        return 0;
    }

    /* Case: the tree already exists.
     * (Rest of function body)
     */

    leaf_pgnum = find_leaf(table_id, key);

    int leaf_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
    #if wpoqkf
    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned++;
    #endif

    free_space = leaf->free_space;

    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned--;

    /* Case: enough free space to insert.
     * Nothing to do.
     */
    if (free_space >= SLOT_SIZE + val_size) {
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

/* Inserts new record into a leaf.
 */
void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char* value, uint16_t val_size) {
    page_t* leaf;
    int i, insertion_index;
    uint16_t offset;
    
    #if verbose
    printf("-----insert_into_leaf() start-----\n");    
    #endif

    
    int leaf_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
    #if wpqkf
    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned++;
    #endif
    
    insertion_index = 0;
    while (insertion_index < leaf->num_keys && leaf->slots[insertion_index].key < key) {
        insertion_index++;
    }
    offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;

    for (i = leaf->num_keys; i > insertion_index; i--) {
        leaf->slots[i].key = leaf->slots[i - 1].key;
        leaf->slots[i].size = leaf->slots[i - 1].size;
        leaf->slots[i].offset = leaf->slots[i - 1].offset;
    }
    leaf->slots[insertion_index].key = key;
    leaf->slots[insertion_index].size = val_size;
    leaf->slots[insertion_index].offset = offset - val_size + HEADER_SIZE;
    memcpy(leaf->values + offset - val_size, value, val_size);

    leaf->num_keys++;
    leaf->free_space -= (SLOT_SIZE + val_size);

    buffer_write_page(table_id, leaf_pgnum, &leaf);
    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned--;

    #if verbose
    printf("-----insert_into_leaf() end-----\n");    
    #endif

}

/* Inserts new record into a leaf so as to exceed
 * the page's size, causing the leaf to be split in half.
 */
void insert_into_leaf_split(int64_t table_id, pagenum_t leaf_pgnum,
                            int64_t key, char* value, uint16_t val_size) {
    pagenum_t new_pgnum;
    page_t* leaf, * new_leaf;
    int i, j, split, insertion_index, num_keys, total_size;
    int64_t new_key;
    uint16_t offset;
    slot_t temp_slots[65];
    char temp_values[3968];

    #if verbose
    printf("-----insert_into_leaf_split() start-----\n");     
    print_buffers();  
    #endif

    
    int leaf_idx = buffer_read_page(table_id, leaf_pgnum, &leaf);
    #if wpqkf
    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned++;
    #endif
    
    /* First create a temporary set of records
     * to hold everything in order, including
     * the new record, inserted in right places.
     * Then create a new leaf and copy half of the
     * entries to the old page and
     * the other half to the new leaf.
     */

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
    }
    temp_slots[insertion_index].key = key;
    temp_slots[insertion_index].size = val_size;
    temp_slots[insertion_index].offset = offset - val_size + HEADER_SIZE;
    memcpy(temp_values + offset, leaf->values + offset, FREE_SPACE - offset);
    memcpy(temp_values + offset - val_size, value, val_size);

    num_keys = leaf->num_keys;
    leaf->num_keys = 0;
    leaf->free_space = FREE_SPACE;

    /* Create the new leaf and copy
     * half the records to the
     * old and half to the new.
     */

    total_size = 0;
    for (split = 0; split <= num_keys; split++) {
        total_size += (SLOT_SIZE + temp_slots[split].size);
        if (total_size >= FREE_SPACE / 2) break;
    }
    new_pgnum = make_leaf(table_id);

    int new_idx = buffer_read_page(table_id, new_pgnum, &new_leaf);
    #if wpkqf
    if (new_idx != -1) buffers[new_idx]->is_pinned++;
    #endif
    
    offset = FREE_SPACE;
    for (i = 0; i < split; i++) {
        offset -= temp_slots[i].size;
        leaf->slots[i].key = temp_slots[i].key;
        leaf->slots[i].size = temp_slots[i].size;
        leaf->slots[i].offset = offset + HEADER_SIZE;
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
    if (leaf_idx != -1) buffers[leaf_idx]->is_pinned--;
    buffer_write_page(table_id, new_pgnum, &new_leaf);
    if (new_idx != -1) buffers[new_idx]->is_pinned--;

    /* Insert a new key into the parent of the two
     * leafs resulting from the split, with
     * the old leaf to the left and the new to the right.
     */

    insert_into_parent(table_id, leaf_pgnum, new_key, new_pgnum);

    #if verbose
    printf("-----insert_into_leaf_split() end-----\n");    
    #endif

}

/* Inserts a new entry into data file,
 * and adjusts tree by insertion rule.
 */
void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t parent_pgnum;
    page_t* left, * parent;
    int left_index, num_keys;

    #if verbose
    printf("-----insert_into_parent() start-----\n");      
    print_buffers(); 
    #endif


    int left_idx = buffer_read_page(table_id, left_pgnum, &left);
    #if wpqkf
    if (left_idx != -1) buffers[left_idx]->is_pinned++;
    #endif

    parent_pgnum = left->parent;
    
    if (left_idx != -1) buffers[left_idx]->is_pinned--;

    /* Case: new root.
     */
    if (parent_pgnum == 0) {
        insert_into_new_root(table_id, left_pgnum, key, right_pgnum);
        return;
    }

    /* Case: leaf or page.
     * (Rest of function body)
     */

    int parent_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    #if wpqkf
    if (parent_idx != -1) buffers[parent_idx]->is_pinned++;
    #endif

    num_keys = parent->num_keys;

    if (parent_idx != -1) buffers[parent_idx]->is_pinned--;

    left_index = get_left_index(table_id, parent_pgnum, left_pgnum);
    
    /* Case: the new key fits into the node.
     * Nothing to do.
     */
    if (num_keys < ENTRY_ORDER - 1) {
        insert_into_page(table_id, parent_pgnum, left_index, key, right_pgnum);
    }

    /* Case: split a node by insertion rule.
     * Need to be split.
     */
    else {
        insert_into_page_split(table_id, parent_pgnum, left_index, key, right_pgnum);
    }

    #if verbose
    printf("-----insert_into_parent() end-----\n");    
    #endif

}

/* Inserts new entry into a page.
 */
void insert_into_page(int64_t table_id, pagenum_t p_pgnum,
                      int left_index, int64_t key, pagenum_t right_pgnum) {
    page_t* p;
    int i;

    #if verbose
    printf("-----insert_into_page() start-----\n");      
    print_buffers(); 
    #endif


    int p_idx = buffer_read_page(table_id, p_pgnum, &p);
    #if wpqkf
    if (p_idx != -1) buffers[p_idx]->is_pinned++;
    #endif

    for (i = p->num_keys; i > left_index; i--) {
        p->entries[i].key = p->entries[i - 1].key;
        p->entries[i].child = p->entries[i - 1].child;
    }
    p->entries[left_index].child = right_pgnum;
    p->entries[left_index].key = key;
    p->num_keys++;

    buffer_write_page(table_id, p_pgnum, &p);
    if (p_idx != -1) buffers[p_idx]->is_pinned--;

    #if verbose
    printf("-----insert_into_parent() end-----\n");    
    #endif
    
}

/* Inserts new entry into a page so as to exceed
 * the entry order, causing the page to be split in half.
 */
void insert_into_page_split(int64_t table_id, pagenum_t old_pgnum,
                            int left_index, int64_t key, pagenum_t right_pgnum) {
    pagenum_t new_pgnum;
    page_t* old_page, * new_page, * child;
    int i, j, split;
    int64_t k_prime;
    pagenum_t temp_left_child;
    entry_t temp[ENTRY_ORDER];

    #if verbose
    printf("-----insert_into_page_split() start-----\n");   
    print_buffers();    
    #endif

    
    int old_idx = buffer_read_page(table_id, old_pgnum, &old_page);
    #if wpqkf
    if (old_idx != -1) buffers[old_idx]->is_pinned++;
    #endif

    /* First create a temporary set of entries
     * to hold everything in order, including
     * the new entry, inserted in right places.
     * Then create a new page and copy half of the
     * entries to the old page and
     * the other half to the new page.
     */

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

    /* Create the new page and copy
     * half the entries to the
     * old and half to the new.
     */

    split = ENTRY_ORDER / 2 + 1;
    new_pgnum = make_page(table_id);

    int new_idx = buffer_read_page(table_id, new_pgnum, &new_page);
    #if wpqkf
    if (new_idx != -1) buffers[new_idx]->is_pinned++;
    #endif
    
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
    
    int child_idx;
    child_idx = buffer_read_page(table_id, new_page->left_child, &child);
    #if wpqkf
    if (child_idx != -1) buffers[child_idx]->is_pinned++;
    #endif
    child->parent = new_pgnum;
    buffer_write_page(table_id, new_page->left_child, &child);
    if (child_idx != -1) buffers[child_idx]->is_pinned--;
    for (i = 0; i < new_page->num_keys; i++) {
        child_idx = buffer_read_page(table_id, new_page->entries[i].child, &child);
        #if wpqkf
        if (child_idx != -1) buffers[child_idx]->is_pinned++;
        #endif
        child->parent = new_pgnum;
        buffer_write_page(table_id, new_page->entries[i].child, &child);
        if (child_idx != -1) buffers[child_idx]->is_pinned--;
    }
    
    buffer_write_page(table_id, new_pgnum, &new_page);
    if (new_idx != -1) buffers[new_idx]->is_pinned--;
    buffer_write_page(table_id, old_pgnum, &old_page);
    if (old_idx != -1) buffers[old_idx]->is_pinned--;

    /* Insert a new key into the parent of the two
     * pages resulting from the split, with
     * the old node to the left and the new to the right.
     */

    insert_into_parent(table_id, old_pgnum, k_prime, new_pgnum);

    #if verbose
    printf("-----insert_into_page_split() end-----\n");    
    #endif

}

/* Starts a new tree.
 */
void start_tree(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t root_pgnum;
    page_t* root;

    #if verbose
    printf("-----start_tree() start-----\n");    
    print_buffers();   
    #endif


    root_pgnum = make_leaf(table_id);

    int root_idx = buffer_read_page(table_id, root_pgnum, &root);
    #if wpqkf
    if (root_idx != -1) buffers[root_idx]->is_pinned++;
    #endif

    root->slots[0].key = key;
    root->slots[0].size = val_size;
    root->slots[0].offset = PAGE_SIZE - val_size;
    memcpy(root->values + FREE_SPACE - val_size, value, val_size);
    root->free_space -= (SLOT_SIZE + val_size);
    root->parent = 0;
    root->num_keys++;
    set_root_num(table_id, root_pgnum);

    buffer_write_page(table_id, root_pgnum, &root);
    if (root_idx != -1) buffers[root_idx]->is_pinned--;

    #if verbose
    printf("-----start_tree() end-----\n");    
    #endif

}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into the new root.
 */
void insert_into_new_root(int64_t table_id,
                          pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t root_pgnum;
    page_t* left, * right, * root;

    #if verbose
    printf("-----insert_into_new_root() start-----\n");
    print_buffers(); 
    #endif

    
    root_pgnum = make_page(table_id);

    int left_idx = buffer_read_page(table_id, left_pgnum, &left);
    #if wpqkf
    if (left_idx != -1) buffers[left_idx]->is_pinned++;
    #endif
    int right_idx = buffer_read_page(table_id, right_pgnum, &right);
    #if wpqkf
    if (right_idx != -1) buffers[right_idx]->is_pinned++;
    #endif
    int root_idx = buffer_read_page(table_id, root_pgnum, &root);
    #if wpqkf
    if (root_idx != -1) buffers[root_idx]->is_pinned++;
    #endif

    root->left_child = left_pgnum;
    root->entries[0].key = key;
    root->entries[0].child = right_pgnum;
    root->num_keys++;
    root->parent = 0;
    left->parent = root_pgnum;
    right->parent = root_pgnum;

    set_root_num(table_id, root_pgnum);

    buffer_write_page(table_id, left_pgnum, &left);
    if (left_idx != -1) buffers[left_idx]->is_pinned--;
    buffer_write_page(table_id, right_pgnum, &right);
    if (right_idx != -1) buffers[right_idx]->is_pinned--;
    buffer_write_page(table_id, root_pgnum, &root);
    if (root_idx != -1) buffers[root_idx]->is_pinned--;

    #if verbose
    printf("-----insert_into_new_root() start-----\n");   
    print_buffers();  
    #endif

}

/* Creates a new leaf by creating a page
 * and then adpapting it appropriately.
 */
pagenum_t make_leaf(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t* new_leaf;

    #if verbose
    printf("-----make_leaf() start-----\n");    
    print_buffers(); 
    #endif

    new_pgnum = make_page(table_id);
    int new_idx = buffer_read_page(table_id, new_pgnum, &new_leaf);
    #if wpqkf
    if (new_idx != -1) buffers[new_idx]->is_pinned++;
    #endif
    new_leaf->is_leaf = 1;
    new_leaf->free_space = FREE_SPACE;
    new_leaf->sibling = 0;
    buffer_write_page(table_id, new_pgnum, &new_leaf);
    if (new_idx != -1) buffers[new_idx]->is_pinned--;

    #if verbose
    printf("-----make_leaf() end-----\n");    
    #endif

    return new_pgnum;
}

/* Creates a new general page, which can be adapted
 * to serve as either a leaf or an internal page.
 */
pagenum_t make_page(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t* new_page;

    #if verbose
    printf("-----make_page() start-----\n"); 
    print_buffers();    
    #endif

    new_pgnum = buffer_alloc_page(table_id);
    int new_idx = buffer_read_page(table_id, new_pgnum, &new_page);
    #if wpqkf
    if (new_idx != -1) buffers[new_idx]->is_pinned++;
    #endif
    new_page->parent = 0;
    new_page->is_leaf = 0;
    new_page->num_keys = 0;
    buffer_write_page(table_id, new_pgnum, &new_page);
    if (new_idx != -1) buffers[new_idx]->is_pinned--;

    #if verbose
    printf("-----make_page() end-----\n");    
    #endif

    return new_pgnum;
}

/* Helper function for insertion
 * to find the index of the parent's child to
 * the page to the left of the key to be inserted.
 */
int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum) {
    page_t* parent;
    int left_index;
    int parent_idx = buffer_read_page(table_id, parent_pgnum, &parent);
    #if wpqkf
    if (parent_idx != -1) buffers[parent_idx]->is_pinned++;
    #endif
    left_index = 0;
    if (parent->left_child == left_pgnum) {
        if (parent_idx != -1) buffers[parent_idx]->is_pinned--;
        return left_index;
    }
    while (parent->entries[left_index].child != left_pgnum) {
        left_index++;
    }
    if (parent_idx != -1) buffers[parent_idx]->is_pinned--;
    return ++left_index;

}

// Deletion

/* Master deletion function.
 * Deletes a record from data file
 * and adjusts tree by deletion rule.
 * If successes, returns 0. Otherwise, returns -1.
 */
int db_delete(int64_t table_id, int64_t key) {
    pagenum_t leaf_pgnum, sibling_pgnum;
    page_t* leaf, *sibling, *parent;
    int sibling_index, k_prime_index;
    int64_t k_prime;

    /* If the record exists, find leaf
     * and delete the record from leaf->
     */
    if (db_find(table_id, key) == -1) return -1;
    leaf_pgnum = find_leaf(table_id, key);

    delete_from_leaf(table_id, leaf_pgnum, key);

    buffer_read_page(table_id, leaf_pgnum, &leaf);
    buffer_read_page(table_id, leaf->parent, &parent);

    /* Case: deletion from the root.
     * If key does not exist, end a tree.
     */
    if (leaf->parent == 0) {
        if (leaf->num_keys > 0) return 0;
        end_tree(table_id, leaf_pgnum);
        return 0;
    }

    /* Case: free space is under threshold.
     * Nothing to do.
     */
    if (leaf->free_space < THRESHOLD) {
        return 0;
    }

    /* Case: free space exceeds threshold.
     * Need to be merged or redistributed.
     * (Rest of function body)
     */

    sibling_index = get_sibling_index(table_id, leaf_pgnum);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent->entries[k_prime_index].key;
    if (sibling_index == -1) sibling_pgnum = parent->entries[0].child;
    else if (sibling_index == 0) sibling_pgnum = parent->left_child;
    else sibling_pgnum = parent->entries[sibling_index - 1].child;

    buffer_read_page(table_id, sibling_pgnum, &sibling);
    
    /* Case: sibling has enough free space.
     * Need to be merged.
     */
    if (sibling->free_space + leaf->free_space >= FREE_SPACE) {
        merge_leaves(table_id, leaf_pgnum,
                     sibling_pgnum, sibling_index, k_prime);
    }

    /* Case: leaf cannot be merged.
     * Need to be redistributed.
     */
    else {
        redistribute_leaves(table_id, leaf_pgnum,
                            sibling_pgnum, sibling_index, k_prime_index);
    }

    return 0;
}

/* Deletes a record from the leaf->
 */
void delete_from_leaf(int64_t table_id, pagenum_t leaf_pgnum, int64_t key) {
    page_t* leaf;
    int i, key_index;
    uint16_t val_size, insertion_offset, deletion_offset;

    buffer_read_page(table_id, leaf_pgnum, &leaf);
    
    key_index = 0;
    while (leaf->slots[key_index].key != key) key_index++;
    
    /* Size and offset of values for packing
     * them after deletion.
     */
    val_size = leaf->slots[key_index].size;
    deletion_offset = leaf->slots[key_index].offset - HEADER_SIZE;
    insertion_offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;

    /* Remove the record and shift other records accordingly.
     */
    for (i = key_index + 1; i < leaf->num_keys; i++) {
        leaf->slots[i - 1].key = leaf->slots[i].key;
        leaf->slots[i - 1].size = leaf->slots[i].size;
        leaf->slots[i - 1].offset = leaf->slots[i].offset;
    }
    memmove(leaf->values + insertion_offset + val_size,
            leaf->values + insertion_offset,
            deletion_offset - insertion_offset);

    leaf->num_keys--;
    leaf->free_space += (SLOT_SIZE + val_size);

    /* Adjust of offset of the record
     * whose value is packed.
     */
    for (i = 0; i < leaf->num_keys; i++) {
        if (leaf->slots[i].offset - HEADER_SIZE < deletion_offset) {
            leaf->slots[i].offset += val_size;
        }
    }

    buffer_write_page(table_id, leaf_pgnum, &leaf);
}

/* Merges a leaf with a sibling.
 */
void merge_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                  pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    page_t* leaf, * sibling;
    int i, j, leaf_size, sibling_size;
    uint16_t leaf_offset, sibling_offset;

    /* Swap sibling with leaf if leaf is on the
     * extreme left and sibling is to its right.
     */
    if (sibling_index != -1) {
        buffer_read_page(table_id, leaf_pgnum, &leaf);
        buffer_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        buffer_read_page(table_id, sibling_pgnum, &leaf);
        buffer_read_page(table_id, leaf_pgnum, &sibling);
    }

    /* Size and offset of values for copying
     * them from leaf to sibling.
     */
    sibling_offset = sibling->free_space + SLOT_SIZE * sibling->num_keys;
    sibling_size = FREE_SPACE - sibling_offset;
    leaf_offset = leaf->free_space + SLOT_SIZE * leaf->num_keys;
    leaf_size = FREE_SPACE - leaf_offset;

    /* Append the records of leaf to the sibling.
     * Set the sibling's sibling to
     * what head been leaf's sibling
     */
    for (i = 0, j = sibling->num_keys; i < leaf->num_keys; i++, j++) {
        sibling->slots[j].key = leaf->slots[i].key;
        sibling->slots[j].size = leaf->slots[i].size;
        sibling->slots[j].offset = leaf->slots[i].offset - sibling_size;
        sibling->num_keys++;
        sibling->free_space -= (leaf->slots[i].size + SLOT_SIZE);
    }
    memcpy(sibling->values + sibling_offset - leaf_size,
            leaf->values + leaf_offset, leaf_size);
    sibling->sibling = leaf->sibling;

    buffer_write_page(table_id, sibling_pgnum, &sibling);

    delete_from_child(table_id, leaf->parent, k_prime, leaf_pgnum);
}

/* Redistributes leaves.
 */
void redistribute_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                         pagenum_t sibling_pgnum, int sibling_index, int k_prime_index) {
    page_t* leaf, * sibling, * parent;
    int i, src_index, dest_index;
    int16_t src_size, dest_offset;

    buffer_read_page(table_id, leaf_pgnum, &leaf);
    buffer_read_page(table_id, sibling_pgnum, &sibling);

    /* Pull records from sibling
     * until its free space is under threshold.
     */
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
            }
        }
        leaf->slots[dest_index].key = sibling->slots[src_index].key;
        leaf->slots[dest_index].size = sibling->slots[src_index].size;
        leaf->slots[dest_index].offset = dest_offset + HEADER_SIZE;
        memcpy(leaf->values + dest_offset,
                sibling->values + sibling->slots[src_index].offset - HEADER_SIZE,
                src_size);
        
        leaf->num_keys++;
        leaf->free_space -= (SLOT_SIZE + src_size);

        delete_from_leaf(table_id, sibling_pgnum, sibling->slots[src_index].key);
        buffer_read_page(table_id, sibling_pgnum, &sibling);
    }

    buffer_write_page(table_id, leaf_pgnum, &leaf);
    buffer_write_page(table_id, sibling_pgnum, &sibling);
    
    buffer_read_page(table_id, leaf->parent, &parent);
    parent->entries[k_prime_index].key = (sibling_index != -1) ?
                                        leaf->slots[0].key :
                                        sibling->slots[0].key;
    buffer_write_page(table_id, leaf->parent, &parent);
}

/* Deletes an entry from data file
 * and adjusts tree by deletion rule.
 */
void delete_from_child(int64_t table_id,
                       pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum) {
    pagenum_t sibling_pgnum;
    page_t* p, * sibling, * parent;
    int sibling_index, k_prime_index;
    int64_t k_prime;

    /* Delete the entry from page.
     */
    delete_from_page(table_id, p_pgnum, key, child_pgnum);

    buffer_read_page(table_id, p_pgnum, &p);
    buffer_read_page(table_id, p->parent, &parent);

    /* Case: deletion from the root.
     * If key does not exist, adjust root.
     */
    if (p->parent == 0) {
        if (p->num_keys > 0) return;
        adjust_root(table_id, p_pgnum);
        return;
    }

    /* Case: page stays at or above minimum.
     * Nothing to do.
     */
    if (p->num_keys >= ENTRY_ORDER / 2) {
        return;
    }

    /* Case: page is under minimum.
     * Need to be merged or redistributed.
     * (Rest of function body)
     */

    sibling_index = get_sibling_index(table_id, p_pgnum);
    k_prime_index = (sibling_index != -1) ? sibling_index : 0;
    k_prime = parent->entries[k_prime_index].key;
    if (sibling_index == 0) sibling_pgnum = parent->left_child;
    else if (sibling_index == -1) sibling_pgnum = parent->entries[0].child;
    else sibling_pgnum = parent->entries[sibling_index - 1].child;

    buffer_read_page(table_id, sibling_pgnum, &sibling);

    /* Case: sibling can accept additional entries.
     * Need to be merged.
     */
    if (sibling->num_keys + p->num_keys < ENTRY_ORDER - 1) {
        merge_pages(table_id, p_pgnum, sibling_pgnum, sibling_index, k_prime);
    }

    /* Case: page cannot be merged.
     * Need to be redistributed.
     */
    else {
        redistribute_pages(table_id, p_pgnum,
                           sibling_pgnum, sibling_index, k_prime_index, k_prime);
    }
}

/* Deletes an entry from the page.
 */
void delete_from_page(int64_t table_id, pagenum_t p_pgnum,
                      int64_t key, pagenum_t child_pgnum) {
    page_t* p;
    int i;

    buffer_read_page(table_id, p_pgnum, &p);

    /* Remove the key and shift other keys accordingly.
     */
    i = 0;
    while (p->entries[i].key != key) i++;
    for (++i; i < p->num_keys; i++) {
        p->entries[i - 1].key = p->entries[i].key;
    }

    /* Remove the child and shift other childs accordingly.
     */
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

/* Merges a page with a sibling.
 */
void merge_pages(int64_t table_id, pagenum_t p_pgnum,
                 pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime) {
    page_t* p, * sibling, * nephew;
    int i, j, insertion_index, p_end;
    
    /* Swap sibling with page if page is on the
     * extreme left and sibling is to its right.
     */
    if (sibling_index != -1) {
        buffer_read_page(table_id, p_pgnum, &p);
        buffer_read_page(table_id, sibling_pgnum, &sibling);
    }
    else {
        buffer_read_page(table_id, sibling_pgnum, &p);
        buffer_read_page(table_id, p_pgnum, &sibling);
    }

    /* Starting point in the sibling for copying
     * entries from n.
     */
    insertion_index = sibling->num_keys;

    /* Append k_prime.
     */
    sibling->entries[insertion_index].key = k_prime;
    sibling->num_keys++;

    /* Merge.
     */
    p_end = p->num_keys;
    for (i = insertion_index + 1, j = 0; j < p_end; i++, j++) {
        sibling->entries[i].key = p->entries[j].key;
        sibling->entries[i].child = p->entries[j].child;
        sibling->num_keys++;
    }
    sibling->entries[insertion_index].child = p->left_child;

    /* All children must now point up to the same parnet.
     */
    buffer_read_page(table_id, sibling->left_child, &nephew);
    nephew->parent = sibling_pgnum;
    buffer_write_page(table_id, sibling->left_child, &nephew);
    for (i = 0; i < sibling->num_keys; i++) {
        buffer_read_page(table_id, sibling->entries[i].child, &nephew);
        nephew->parent = sibling_pgnum;
        buffer_write_page(table_id, sibling->entries[i].child, &nephew);
    }

    buffer_write_page(table_id, sibling_pgnum, &sibling);

    delete_from_child(table_id, p->parent, k_prime, p_pgnum);
}

/* Redistributes pages.
 */
void redistribute_pages(int64_t table_id, pagenum_t p_pgnum,
                        pagenum_t sibling_pgnum, int sibling_index,
                        int k_prime_index, int64_t k_prime) {
    page_t* p, * sibling, * parent, * child;
    int i;

    buffer_read_page(table_id, p_pgnum, &p);
    buffer_read_page(table_id, sibling_pgnum, &sibling);

    /* Case : p has a sibling to the left->
     * Pull the sibling's last entry over
     * from the sibling's right end to p's left end.
     */
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

    /* Case: p is the leftmost child.
     * Take a entry from the sibling to the right.
     * Move the sibling's leftmost entry
     * to p's rightmost position.
     */
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

/* Ends a tree.
 */
void end_tree(int64_t table_id, pagenum_t root_pgnum) {
    set_root_num(table_id, 0);
    buffer_free_page(table_id, root_pgnum);
}

/* Promotes the first (only) child
 * as the new root.
 */
void adjust_root(int64_t table_id, pagenum_t root_pgnum) {
    page_t* root, * new_root;

    buffer_read_page(table_id, root_pgnum, &root);
    buffer_read_page(table_id, root->left_child, &new_root);

    set_root_num(table_id, root->left_child);
    new_root->parent = 0;

    buffer_free_page(table_id, root_pgnum);
    buffer_write_page(table_id, root->left_child, &new_root);
}

/* Helper function for deletion
 * to find the index of the page's nearest
 * sibling to the left if one exists.
 * If not (page is leftmost) returns -1.
 */
int get_sibling_index(int64_t table_id, pagenum_t p_pgnum) {
    page_t* p, * parent;
    int sibling_index;
    buffer_read_page(table_id, p_pgnum, &p);
    buffer_read_page(table_id, p->parent, &parent);
    sibling_index = -1;
    if (parent->left_child == p_pgnum) return sibling_index;
    do {
        sibling_index++;
    } while (parent->entries[sibling_index].child != p_pgnum);
    return sibling_index;
}
