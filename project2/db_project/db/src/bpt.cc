#include "../include/bpt.h"
#include "../include/file.h"

void print_page(page_t page) {
	if (page.is_leaf) {
		printf("-----leaf information-----\n");
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
		printf("-----page information-----\n");
		printf("parent: %ld\n", page.parent);
		printf("is_leaf: %d\n", page.is_leaf);
		printf("num_keys: %d\n", page.num_keys);

		printf("this_num: %ld\n", page.this_num);
		for (int i = 0; i < page.num_keys; i++) {
			printf("key: %3ld, page_num: %ld\n", page.entries[i].key, page.entries[i].page_num);
		}
	}
}

void print_leaves(int64_t table_id) {
    int i;
    pagenum_t temp_pgnum;
    page_t header, temp;

    file_read_page(table_id, 0, &header);

    temp_pgnum = header.root_num;

    file_read_page(table_id, temp_pgnum, &temp);

    if (temp_pgnum == 0) {
        printf("Empty tree.\n");
        return;
    }
    while (!temp.is_leaf) {
        temp_pgnum = temp.this_num;
        file_read_page(table_id, temp_pgnum, &temp);
    }
    while (true) {
        for (i = 0; i < temp.num_keys; i++) {
            printf("%ld:%s ", temp.slots[i].key, temp.values + temp.slots[i].offset - 128);
        }
        if (temp.sibling != 0) {
            printf("\n\n");
            temp_pgnum = temp.sibling;
            file_read_page(table_id, temp_pgnum, &temp);
        }
        else break;
    }
    printf("\n");
}


int64_t open_table(char* pathname) {
    int64_t table_id = file_open_database_file(pathname);
    return table_id;
}

// OUTPUT AND UTILITIES

pagenum_t find_leaf(int64_t table_id, int64_t key) {
    int i;
    pagenum_t temp_pgnum;
    page_t temp, header;

    file_read_page(table_id, 0, &header);

    temp_pgnum = header.root_num;

    file_read_page(table_id, temp_pgnum, &temp);

    if (temp_pgnum == 0) return temp_pgnum;
    while (!temp.is_leaf) {
        i = 0;
        while (i < temp.num_keys) {
            if (key >= temp.entries[i].key) i++;
            else break;
        }
        if (i) temp_pgnum = temp.this_num;
        else temp_pgnum = temp.entries[i - 1].page_num;
        file_read_page(table_id, temp_pgnum, &temp);
    }
    return temp_pgnum;
}

// INSERTION

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

pagenum_t make_leaf(int64_t table_id) {
    pagenum_t new_pgnum;
    page_t new_page;
    new_pgnum = make_page(table_id);
    file_read_page(table_id, new_pgnum, &new_page);
    new_page.is_leaf = 1;
    new_page.free_space = FREE_SPACE;
    file_write_page(table_id, new_pgnum, &new_page);
    return new_pgnum;
}

int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum) {
    int left_index;
    page_t parent;

    file_read_page(table_id, parent_pgnum, &parent);

    left_index = 0;
    if (parent.this_num != left_pgnum) {
        left_index++;
        while (left_index <= parent.num_keys &&
                parent.entries[left_index].page_num != left_pgnum) {
            left_index++;
        }
    }

    return left_index;
}

void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char* value, uint16_t val_size) {
    
    int i, insertion_point;
    page_t leaf;
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

void insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf_pgnum,
                                      int64_t key, char* value, uint16_t val_size) {
    
    pagenum_t new_leaf_pgnum;
    page_t leaf, new_leaf;
    union {
        slot_t slots[65];
        char values[4092];
    } temp;
    int insertion_index, split, new_key, i, j;
    uint16_t offset, new_offset;
    uint64_t total_size;
    int num_keys, gap;
    
    new_leaf_pgnum = make_leaf(table_id);

    file_read_page(table_id, leaf_pgnum, &leaf);
    file_read_page(table_id, new_leaf_pgnum, &new_leaf);

    insertion_index = 0;
    while (insertion_index < leaf.num_keys && leaf.slots[insertion_index].key < key) {
        insertion_index++;
    }

    total_size = 0;
    split = 0;
    gap = SLOT_SIZE  + VALUE_SIZE;
    offset = FREE_SPACE;
    for (i = 0, j = 0; i < leaf.num_keys; i++, j++) {
        if (j == insertion_index) {
            gap -= val_size;
            temp.slots[j].key = key;
            temp.slots[j].size = val_size;
            temp.slots[j].offset = offset + gap + HEADER_SIZE;
            memmove(temp.values + offset + gap, value, val_size);

            total_size += (SLOT_SIZE + val_size);
            if (total_size < FREE_SPACE / 2) {
                split++;
            }
            j++;
        }

        offset -= leaf.slots[i].size;
        temp.slots[j].key = leaf.slots[i].key;
        temp.slots[j].size = leaf.slots[i].size;
        temp.slots[j].offset = offset + gap + HEADER_SIZE;
        memmove(temp.values + offset + gap,
                leaf.values + offset, leaf.slots[i].size);

        total_size += (SLOT_SIZE + leaf.slots[i].size);
        if (total_size < FREE_SPACE / 2) {
            split++;
        }
    }

    num_keys = leaf.num_keys;
    leaf.num_keys = 0;
    leaf.free_space = FREE_SPACE;

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
    leaf.sibling = new_leaf_pgnum;

    new_leaf.parent = leaf.parent;
    new_key = new_leaf.slots[0].key;

    file_write_page(table_id, leaf_pgnum, &leaf);
    file_write_page(table_id, new_leaf_pgnum, &new_leaf);

    insert_into_parent(table_id, leaf_pgnum, new_key, new_leaf_pgnum);
}

void insert_into_page(int64_t table_id, pagenum_t parent_pgnum, int left_index,
                      int64_t key, pagenum_t right_pgnum) {
    int i;
    page_t parent, right;

    file_read_page(table_id, parent_pgnum, &parent);
    file_read_page(table_id, right_pgnum, &right);

    for (i = parent.num_keys; i > left_index; i--) {
        parent.entries[i].key = parent.entries[i - 1].key;
        parent.entries[i].page_num = parent.entries[i - 1].page_num;
    }
    parent.entries[left_index].page_num = right_pgnum;
    parent.entries[left_index].key = key;
    parent.num_keys++;

    file_write_page(table_id, parent_pgnum, &parent);
}

void insert_into_page_after_splitting(int64_t table_id, pagenum_t parent_pgnum, int left_index,
                                      int64_t key, pagenum_t right_pgnum) {
    int i, j, split, k_prime;
    pagenum_t new_pgnum;
    page_t parent, right, new_page, child;
    pagenum_t this_num;
    entry_t temp[ENTRY_ORDER];
    
    file_read_page(table_id, parent_pgnum, &parent);
    file_read_page(table_id, right_pgnum, &right);

    for (i = 0, j = 0; i < parent.num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        if (j) temp[j - 1].key = parent.entries[j - 1].key;
        else this_num = parent.this_num;
    }

    for (i = 0, j = 0; i < parent.num_keys; i++, j++) {
        if (j == left_index) j++;
        temp[j].key = parent.entries[i].key;
    }

    temp[left_index].page_num = right_pgnum;
    temp[left_index].key = key;

    split = ENTRY_ORDER / 2 + 1;
    new_pgnum = make_page(table_id);
    
    file_read_page(table_id, new_pgnum, &new_page);

    parent.num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        if (i) parent.entries[i - 1].page_num = temp[i - 1].page_num;
        else parent.this_num = this_num;
        parent.entries[i].key = temp[i].key;
        parent.num_keys++;
    }
    parent.entries[i - 1].page_num = temp[i - 1].page_num;
    k_prime = temp[split - 1].key;
    for (++i, j = 0; i < ENTRY_ORDER; i++, j++) {
        if (j) new_page.entries[j - 1].page_num = temp[i - 1].page_num;
        else new_page.this_num = this_num;
        new_page.entries[j].key = temp[i].key;
        new_page.num_keys++;
    }
    new_page.entries[j - 1].page_num = temp[i - 1].page_num;
    new_page.parent = parent.parent;
    for (i = 0; i <= new_page.num_keys; i++) {
        if (i) {
            file_read_page(table_id, new_page.entries[i - 1].page_num, &child);
            child.parent = new_pgnum;
            file_write_page(table_id, new_page.entries[i - 1].page_num, &child);
        }
        else {
            file_read_page(table_id, new_page.this_num, &child);
            child.parent = new_pgnum;
            file_write_page(table_id, new_page.this_num, &child);
        }
    }

    file_write_page(table_id, parent_pgnum, &parent);
    file_write_page(table_id, right_pgnum, &right);

    insert_into_parent(table_id, parent_pgnum, k_prime, new_pgnum);
}

void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    int left_index;
    pagenum_t parent_pgnum;
    page_t left, right, parent;

    file_read_page(table_id, left_pgnum, &left);
    file_read_page(table_id, right_pgnum, &right);

    parent_pgnum = left.parent;

    file_read_page(table_id, left.parent, &parent);

    if (parent_pgnum == 0) {
        insert_into_new_root(table_id, left_pgnum, key, right_pgnum);
    }
    else {
        left_index = get_left_index(table_id, parent_pgnum, left_pgnum);
        if (parent.num_keys < ENTRY_ORDER - 1) {
            insert_into_page(table_id, parent_pgnum, left_index, key, right_pgnum);
        }
        else {
            insert_into_page_after_splitting(table_id, parent_pgnum, left_index, key, right_pgnum);
        }
    }
}

void insert_into_new_root(int64_t table_id, pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum) {
    pagenum_t root_pgnum;
    page_t left, right, root, header;
    
    root_pgnum = make_page(table_id);

    file_read_page(table_id, left_pgnum, &left);
    file_read_page(table_id, right_pgnum, &right);
    file_read_page(table_id, root_pgnum, &root);
    file_read_page(table_id, 0, &header);

    root.this_num = left_pgnum;
    root.entries[0].key = key;
    root.entries[0].page_num = right_pgnum;
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

    root.parent = 0;
    root.num_keys++;
    root.free_space -= (SLOT_SIZE + val_size);

    header.root_num = root_pgnum;

    file_write_page(table_id, root_pgnum, &root);
    file_write_page(table_id, 0, &header);
}

void insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    pagenum_t leaf_pgnum;
    page_t leaf, header;

    // if (!find(table_id, key)) {
    //     return;
    // }

    file_read_page(table_id, 0, &header);

    if (header.root_num == 0) {
        start_new_tree(table_id, key, value, val_size);
    }
    else {
        leaf_pgnum = find_leaf(table_id, key);

        file_read_page(table_id, leaf_pgnum, &leaf);

        if (leaf.free_space >= SLOT_SIZE + val_size) {
            insert_into_leaf(table_id, leaf_pgnum, key, value, val_size);
        }
        else {
            insert_into_leaf_after_splitting(table_id, leaf_pgnum, key, value, val_size);
        }
    }
}