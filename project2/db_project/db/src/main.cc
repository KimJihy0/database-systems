#include "bpt.cc"
#include "file.cc"




// void print_page(page_t page) {
// 	if (page.is_leaf) {
// 		printf("-----leaf information-----\n");
// 		printf("parent: %ld\n", page.parent);
// 		printf("is_leaf: %d\n", page.is_leaf);
// 		printf("num_keys: %d\n", page.num_keys);
// 		printf("free_space: %ld\n", page.free_space);
// 		printf("sibling: %ld\n", page.sibling);

// 		for (int i = 0; i < page.num_keys; i++) {
// 			printf("key: %3ld, offset: %d, value: %s\n", page.slots[i].key, page.slots[i].offset, page.values + page.slots[i].offset - 128);
// 		}
// 	}
// 	else {
// 		printf("-----page information-----\n");
// 		printf("parent: %ld\n", page.parent);
// 		printf("is_leaf: %d\n", page.is_leaf);
// 		printf("num_keys: %d\n", page.num_keys);

// 		printf("this_num: %ld\n", page.this_num);
// 		for (int i = 0; i < page.num_keys; i++) {
// 			printf("key: %3ld, page_num: %ld\n", page.entries[i].key, page.entries[i].page_num);
// 		}
// 	}
// }


#if 0
int main() {
	int table_id = file_open_database_file("table1.dbdb");

	pagenum_t leaf_pgnum;
	leaf_pgnum = make_leaf(table_id);

	page_t leaf;
	file_read_page(table_id, leaf_pgnum, &leaf);

	int64_t key;
	
	char* value1 = (char*)"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPPPPPPPQ";
	uint16_t val_size1 = 50;

	char* value2 = (char*)"12345678909285939502951927503974720570602039511029561252000";
	uint16_t val_size2 = 60;

	char* value3 = (char*)"&*(^*(%(&%(*&%(*&%(*%";
	uint16_t val_size3 = 70;

	
	insert_into_leaf(table_id, leaf_pgnum, 100, value1, val_size1);
	for (key = 54; key > 1; key--) {
		insert_into_leaf(table_id, leaf_pgnum, key, value2, val_size2);
	}
	insert_into_leaf(table_id, leaf_pgnum, 66, value3, val_size3);
	insert_into_leaf_after_splitting(table_id, leaf_pgnum, 88, (char*)"11111", 6);
	
	// insert_into_leaf_after_splitting(table_id, leaf_pgnum, 64, value, val_size);

	file_read_page(table_id, leaf_pgnum, &leaf);
	print_page(leaf);

	pagenum_t new_leaf_num;
	new_leaf_num= leaf.sibling;
	page_t new_leaf;
	file_read_page(table_id, new_leaf_num, &new_leaf);
	print_page(new_leaf);

	print_leaves(table_id);

	file_close_database_file();
	remove("table1.dbdb");

}
#endif

#if 1
int main() {
	int table_id = file_open_database_file("table1.db");

	insert(table_id, 2, (char*)"++", 60);
	insert(table_id, 105, (char*)"01", 50);
	for (int i = 49; i < 102; i++) {
		insert(table_id, i, (char*)"!@", 60);
	}
	insert(table_id, 30, (char*)"ab", 70);
	insert(table_id, 40, (char*)"..", 80);
	insert(table_id, 45, (char*)"--", 80);

	for (int i = 200; i < 220; i++) {
		insert(table_id, i, (char*)"<>", 70);
	}
	insert(table_id, 220, (char*)"()", 50);
	for (int i = 350; i < 9000; i++) {
		insert(table_id, i, (char*)"**", 50);
	}
	insert(table_id, 250, (char*)"Tlqkf", 50);
	insert(table_id, 300, (char*)"fuck", 50);

	// print_leaves(table_id);

	page_t header;
	file_read_page(table_id, 0, &header);

	page_t root;
	file_read_page(table_id, header.root_num, &root);

	print_page(header.root_num, root);

	print_pgnum(table_id, 2433);

	file_close_database_file();

	remove("table1.db");
}
#endif