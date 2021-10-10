// #include "db/src/bpt.cc"
// #include "db/src/file.cc"
// #include "db/src/hash.cc"

#include "db/include/bpt.h"
#include "db/include/file.h"
#include "db/include/hash.h"

#include <stdlib.h>

#include <deque>
using namespace std;

void print_leaves(int64_t table_id);
void print_page(pagenum_t page_num, page_t page);
void print_pgnum(int64_t table_id, pagenum_t page_num);
void print_all(int64_t table_id);

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

#if 1
int main() {
	int64_t table0 = file_open_table_file((char*)"table0");
	int64_t table1 = file_open_table_file((char*)"table1");
	int64_t table2 = file_open_table_file((char*)"tacke1");

	for (int i = 0; i < 500; i++) {
		db_insert(table1, i, val(i), size(i));
	}

	print_all(table1);
	print_all(table2);

	shutdown_db();
	remove("table0");
	remove("table1");
	remove("tacke2");
}
#endif

#if 0
int main() {
	int64_t table_id = file_open_table_file((char*)"deletion.db");

	deque<int> keys;

	int end = 10000;

	for (int i = 0; i < end; i++) {
		keys.push_back(i);
	}

	for (int i = 0; i < 7000; i++) {
		int key;
		if (rand() % 2) {
			key = keys.back();
			keys.pop_back();
		}
		else {
			key = keys.front();
			keys.pop_front();
		}

		db_insert(table_id, key, val(key), rand() % 63 + 50);
	}

	for (int i = 500; i < 2447; i++)
	db_delete(table_id, i);

	// db_delete(table_id, 1000000);
	print_all(table_id);
	// print_pgnum(table_id, 0);
	// print_pgnum(table_id, 2557);
	// print_pgnum(table_id, 2308);
	// print_pgnum(table_id, 2320);
	// print_pgnum(table_id, 2319);
	// print_pgnum(table_id, 2558);

	shutdown_db();
	remove("deletion.db");

}

#endif

#if 0
int main() {
	int table_id = file_open_database_file((char*)"table0.db");
	
	int loof = 5000;
	for (int i = loof; i > 0; i--) {
		db_insert(table_id, i * 7, values[(i * 7) % 10], 112);
	}
	for (int i = loof; i > 0; i--) {
		db_insert(table_id, i * 7 + 1, values[(i * 7 + 1) % 10], 112);
	}
	// for (int i = 1000; i < 10000; i++) {
	// 	db_insert(table_id, i, values[i % 10], 112);
	// }

	// print_all(table_id);
	print_pgnum(table_id, 2559);
	print_pgnum(table_id, 2370);
	print_pgnum(table_id, 2279);

	shutdown_db();
	remove("table0.db");
} 
#endif

#if 0
int main() {
	int table_id = file_open_database_file("table1.db");

	db_insert(table_id, 2, (char*)"++", 60);
	db_insert(table_id, 105, (char*)"01", 50);
	for (int i = 49; i < 102; i++) {
		db_insert(table_id, i, (char*)"!@", 60);
	}
	db_insert(table_id, 30, (char*)"ab", 70);
	db_insert(table_id, 40, (char*)"..", 80);
	db_insert(table_id, 45, (char*)"--", 80);

	for (int i = 200; i < 220; i++) {
		db_insert(table_id, i, (char*)"<>", 70);
	}
	db_insert(table_id, 220, (char*)"()", 50);
	for (int i = 350; i < 13000; i++) {
		db_insert(table_id, i, (char*)"**", 50);
	}
	// db_insert(table_id, 300, (char*)"fuck", 50);
	// db_insert(table_id, 250, (char*)"Tlqkf", 50);

	page_t header;
	file_read_page(table_id, 0, &header);

	page_t root;
	file_read_page(table_id, header.root_num, &root);

	// print_all(table_id);

	print_page(header.root_num, root);

	print_pgnum(table_id, 2559);
	print_pgnum(table_id, 2434);
	print_pgnum(table_id, 2433);
	print_pgnum(table_id, 2309);
	print_pgnum(table_id, 2306);
	print_pgnum(table_id, 2146);

	char ret_value[100];
	uint16_t val_size;

	db_find(table_id, 220, ret_value, &val_size);

	printf("value: %s\n",ret_value);
	printf("size: %d\n", val_size);

	shutdown_db();
	remove("table1.db");
}
#endif

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
    if (page_num == 0) {
        printf("-----header information-----\n");
        // printf("free_num: %ld\n", page.free_num);
        // printf("num_pages: %ld\n", page.num_pages);
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

void print_all(int64_t table_id) {
    pagenum_t root_num, temp_num;
    page_t header, root, page;

    file_read_page(table_id, 0, &header);
    print_page(0, header);

    root_num = header.root_num;
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