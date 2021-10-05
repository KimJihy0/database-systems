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
    open_table("my_db.db");
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

void print_page(pagenum_t page_num, page_t page) {
    if (page_num == 0) {
        printf("-----header information-----\n");
        printf("free_num: %ld\n", page.free_num);
        printf("num_pages: %ld\n", page.num_pages);
        printf("root_num: %ld\n", page.root_num);
        printf("\n");
        return;
    }
	if (page.is_leaf) {
        if (page.parent)
		    printf("-----leaf information-----\n");
        else
            printf("-----root information-----\n");
        printf("number: %ld\n", page_num);
		printf("parent: %ld\n", page.parent);
		printf("is_leaf: %d\n", page.is_leaf);
		printf("num_keys: %d\n", page.num_keys);
		printf("free_space: %ld\n", page.free_space);
		printf("sibling: %ld\n", page.sibling);

		for (int i = 0; i < page.num_keys; i++) {
			printf("key: %3ld, size: %3d, offset: %4d, value: %s\n", page.slots[i].key, page.slots[i].size, page.slots[i].offset, page.values + page.slots[i].offset - HEADER_SIZE);
		}
	}
	else {
        if (page.parent)
		    printf("-----page information-----\n");
        else
            printf("-----root information-----\n");
        printf("number: %ld\n", page_num);
		printf("parent: %ld\n", page.parent);
		printf("is_leaf: %d\n", page.is_leaf);
		printf("num_keys: %d\n", page.num_keys);

		printf("left_child: %ld\n", page.left_child);
		for (int i = 0; i < page.num_keys; i++) {
			printf("key: %3ld, child: %ld\n", page.entries[i].key, page.entries[i].child);
		}
	}
    printf("\n");
}

void print_pgnum(int64_t table_id, pagenum_t page_num) {
	page_t page;
	file_read_page(table_id, page_num, &page);
	print_page(page_num, page);
}

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
        if (i) temp_pgnum = temp.entries[i - 1].child;
        else temp_pgnum = temp.left_child;
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
        leaf.slots[i].offset = leaf.slots[i - 1].offset - val_size;
        memmove(leaf.values + offset - val_size,
                leaf.values + offset, leaf.slots[i].size);
        offset += leaf.slots[i].size;
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
void insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf_pgnum,
                                      int64_t key, char* value, uint16_t val_size) {
    pagenum_t new_pgnum;
    page_t leaf, new_leaf;
    union {
        slot_t slots[65];
        char values[4092];
    } temp;
    int insertion_index, split, new_key, i, j;
    uint16_t offset, insertion_offset, new_offset;
    int total_size, num_keys, gap;
    
    new_pgnum = make_leaf(table_id);

    file_read_page(table_id, leaf_pgnum, &leaf);
    file_read_page(table_id, new_pgnum, &new_leaf);

    insertion_index = 0;
    while (insertion_index < leaf.num_keys && leaf.slots[insertion_index].key < key) {
        insertion_index++;
    }

    gap = SLOT_SIZE  + VALUE_SIZE;
    offset = FREE_SPACE;
    insertion_offset = 0;
    for (i = 0, j = 0; i < leaf.num_keys; i++, j++) {
        if (j == insertion_index) {
            gap -= val_size;
            insertion_offset = offset;
            j++;
        }
        offset -= leaf.slots[i].size;
        temp.slots[j].key = leaf.slots[i].key;
        temp.slots[j].size = leaf.slots[i].size;
        temp.slots[j].offset = offset + gap + HEADER_SIZE;
        memmove(temp.values + offset + gap,
                leaf.values + offset, leaf.slots[i].size);
    }
    if (insertion_offset == 0) insertion_offset = offset - val_size;
    temp.slots[insertion_index].key = key;
    temp.slots[insertion_index].size = val_size;
    temp.slots[insertion_index].offset = insertion_offset + gap + HEADER_SIZE;
    memmove(temp.values + insertion_offset + gap, value, val_size); 

    num_keys = leaf.num_keys;
    leaf.num_keys = 0;
    leaf.free_space = FREE_SPACE;

    total_size = 0;
    for (split = 0; split < num_keys; split++) {
        total_size += (SLOT_SIZE + temp.slots[i].size);
        if (total_size >= FREE_SPACE / 2) break;
    }

    offset = FREE_SPACE;
    for (i = 0; i < split; i++) {
        offset -= temp.slots[i].size;
        leaf.slots[i].key = temp.slots[i].key;
        leaf.slots[i].size = temp.slots[i].size;
        leaf.slots[i].offset = offset + HEADER_SIZE;
        memmove(leaf.values + offset,
                temp.values + offset + SLOT_SIZE + VALUE_SIZE, temp.slots[i].size);
        leaf.num_keys++;
        leaf.free_space -= (SLOT_SIZE + temp.slots[i].size);
    }

    new_offset = FREE_SPACE;
    for (i = split, j = 0; i <= num_keys; i++, j++) {
        new_offset -= temp.slots[i].size;
        offset -= temp.slots[i].size;
        new_leaf.slots[j].key = temp.slots[i].key;
        new_leaf.slots[j].size = temp.slots[i].size;
        new_leaf.slots[j].offset = new_offset + HEADER_SIZE;
        memmove(new_leaf.values + new_offset,
                temp.values + offset + SLOT_SIZE + VALUE_SIZE, temp.slots[i].size);
        new_leaf.num_keys++;
        new_leaf.free_space -= (SLOT_SIZE + temp.slots[i].size);
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
void insert_into_page(int64_t table_id, pagenum_t parent_pgnum, int left_index,
                      int64_t key, pagenum_t right_pgnum) {
    page_t parent, right;
    int i;

    file_read_page(table_id, parent_pgnum, &parent);
    file_read_page(table_id, right_pgnum, &right);

    for (i = parent.num_keys; i > left_index; i--) {
        parent.entries[i].key = parent.entries[i - 1].key;
        parent.entries[i].child = parent.entries[i - 1].child;
    }
    parent.entries[left_index].child = right_pgnum;
    parent.entries[left_index].key = key;
    parent.num_keys++;

    file_write_page(table_id, parent_pgnum, &parent);
}

/* Inserts new key & child into a page so as to exceed
 * the entry order, causing the page to be split in half.
 */
void insert_into_page_after_splitting(int64_t table_id, pagenum_t old_pgnum, int left_index,
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
        insert_into_page_after_splitting(table_id, parent_pgnum, left_index,
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
    memmove(root.values + root.slots[0].offset - HEADER_SIZE, value, val_size);
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
     * leaf must be split.
     */
    else {
        insert_into_leaf_after_splitting(table_id, leaf_pgnum, key, value, val_size);
    }

    return 0;
}