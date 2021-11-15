#include "bpt.h"
#include "dbms.h"

#include <queue>
#include <vector>
#include <iterator>
#include <algorithm>
#include <random>

#define NUM_KEYS 100
using namespace std;

void print_leaves(int64_t table_id);
void print_page(pagenum_t page_num, page_t page);
void print_pgnum(int64_t table_id, pagenum_t page_num);
void print_all(int64_t table_id);
int path_to_root(int64_t table_id, pagenum_t child_num);
void print_tree(int64_t table_id);
int64_t free_num(int64_t table_id);

char* val(int key);
int size();
int size(int key);

#if 1
int main(int argc, char** argv) {
	vector<int> keys;
	for (int i = 0; i < NUM_KEYS; i++) {
		keys.push_back(i);
	}
	random_device rd;
	mt19937 gen(rd());
	default_random_engine rng(rd());
	char value[112];
	uint16_t val_size;
	shuffle(keys.begin(), keys.end(), rng);

	printf("[TEST START]\n\n");
	init_db();
	int64_t table_id = open_table((char*)"table0");

	printf("[INSERT START]\n");
	int64_t j = 0;
	for (const auto& i : keys) {
		if (j % 10000 == 0) printf("%ld, 	", j);
		if (db_insert(table_id, i, val(i), size(i)) != 0) goto func_exit;
		j++;
	}
	printf("[INSERT END]\n\n");
	printf("[FIND START]\n");
	for (const auto& i : keys) {
		memset(value, 0x00, 112);
		val_size = 0;
		if (db_find(table_id, i, value, &val_size) != 0) goto func_exit;
		// else if (size(i) != val_size ||
		// 		 val(i) != std::string(value, val_size)) {
		// 	printf("value dismatched\n");
		// 	goto func_exit;
		// }
	}
	printf("[FIND END]\n\n");

	print_tree(table_id);

	printf("[DELETE START]\n");
	for (const auto& i : keys) {
		if (db_delete(table_id, i) != 0) goto func_exit;
	}
	printf("[DELETE END]\n\n");

	printf("[FIND START AGAIN]\n");
	for (const auto& i : keys) {
		memset(value, 0x00, 112);
		val_size = 0;
		if (db_find(table_id, i, value, &val_size) == 0) goto func_exit;
	}
	printf("[FIND END AGAIN]\n\n");

	printf("[TEST END]\n\n");
	
	func_exit:
    print_tree(table_id);
	printf("[SHUTDOWN START]\n");
	if (shutdown_db() != 0) {
		return 0;
	}
	printf("[SHUTDOWN END]\n\n");
	return 0;
}
#endif

void print_leaves(int64_t table_id) {
    int i;
    pagenum_t temp_pgnum;
    page_t header, temp;
    file_read_page(table_id, 0, &header);
    temp_pgnum = get_root_num(table_id);
    file_read_page(table_id, temp_pgnum, &temp);
    if (temp_pgnum == 0) {
        printf("Empty tree.\n");
        return;
    }
    while (!temp.is_leaf) {
        temp_pgnum = temp.left_child;
        file_read_page(table_id, temp_pgnum, &temp);
    }
    printf("-----leaves-----\n");
    while (true) {
        for (i = 0; i < temp.num_keys; i++) {
            printf("%3ld:%s ", temp.slots[i].key, temp.values + temp.slots[i].offset - HEADER_SIZE);
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

void print_page(pagenum_t page_num, page_t page) {
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

void print_all(int64_t table_id) {
    pagenum_t root_num, temp_num;
    page_t root, page;

    root_num = get_root_num(table_id);
	printf("-----header information-----\n");
	printf("root_num: %ld\n", root_num);
	printf("\n");
	if (!root_num) return;

    file_read_page(table_id, root_num, &root);
    print_page(root_num, root);
    if (root.is_leaf) return;

    temp_num = root.left_child;
    file_read_page(table_id, temp_num, &page);
    print_page(temp_num, page);
    for (int i = 0; i < root.num_keys; i++) {
        temp_num = root.entries[i].child;
        file_read_page(table_id, temp_num, &page);
        print_page(temp_num, page);
    }
}

char* val(int key) {
	static char value[3];
	sprintf(value, "%02d", key % 100);
	return value;
}

int size() {
	return rand() % 63 + 50;
}
int size(int key) {
	return key % 63 + 50;
}

int path_to_root(int64_t table_id, pagenum_t child_num) {
	int length = 0;
	pagenum_t c_num = child_num;
	page_t c;
	file_read_page(table_id, c_num, &c);
	while (c.parent != 0) {
		c_num = c.parent;
		file_read_page(table_id, c_num, &c);
		length++;
	}
	return length;
}

void print_tree(int64_t table_id) {
	pagenum_t root_pgnum, p_pgnum;
	page_t p, parent;
	int i = 0, rank = 0, new_rank = 0;
	
	root_pgnum = get_root_num(table_id);
	if (root_pgnum == 0) {
		printf("Empty tree.\n");
		return;
	}

	std::queue<pagenum_t> queue;
	queue.push(root_pgnum);
	while (!queue.empty()) {
		p_pgnum = queue.front();
		queue.pop();
		file_read_page(table_id, p_pgnum, &p);
		file_read_page(table_id, p.parent, &parent);
		if (p.parent != 0 && p_pgnum == parent.left_child) {
			new_rank = path_to_root(table_id, p_pgnum);
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		if (p.is_leaf) printf("%ld ~ %ld ", p.slots[0].key, p.slots[p.num_keys - 1].key);
		else for (i = 0; i < p.num_keys; i++) printf("%ld ", p.entries[i].key);
		if (!p.is_leaf) {
			queue.push(p.left_child);
			for (i = 0; i < p.num_keys; i++)
				queue.push(p.entries[i].child);
		}
		printf("| ");
	}
	printf("\n");
}

int64_t free_num(int64_t table_id) {
	page_t p;
	int64_t free_num = 0;

	file_read_page(table_id, 0, &p);

	while (p.next_frpg) {
		file_read_page(table_id, p.next_frpg, &p);
		free_num++;
	}
	
	return free_num;
}