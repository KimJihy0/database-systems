#include "bpt.cc"
#include "file.cc"

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


	file_close_database_file();
	remove("table1.db");
}