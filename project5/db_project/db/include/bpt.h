#ifndef DB_BPT_H_
#define DB_BPT_H_

#include "trx.h"

#define HEADER_SIZE 128
#define FREE_SPACE 3968
#define SLOT_SIZE 16
#define ENTRY_ORDER 249
#define THRESHOLD 2500

// DBMS

int init_db(int num_buf);
int shutdown_db();
int64_t open_table(char* pathname);

// SEARCH & UPDATE

int db_find(int64_t table_id, int64_t key,
            char* ret_val, uint16_t* val_size, int trx_id);
int db_update(int64_t table_id, int64_t key,
              char* value, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);
pagenum_t find_leaf(int64_t table_id, int64_t key);

// INSERTION

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);
void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char* value, uint16_t val_size);
void insert_into_leaf_split(int64_t table_id, pagenum_t leaf_pgnum,
                            int64_t key, char* value, uint16_t val_size);
void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum);
void insert_into_page(int64_t table_id, pagenum_t parent_pgnum,
                      int left_index, int64_t key, pagenum_t right_pgnum);
void insert_into_page_split(int64_t table_id, pagenum_t parent_pgnum,
                            int left_index, int64_t key, pagenum_t right_pgnum);
void start_tree(int64_t table_id, int64_t key, char* value, uint16_t val_size);
void insert_into_new_root(int64_t table_id,
                          pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum);
pagenum_t make_leaf(int64_t table_id);
pagenum_t make_page(int64_t table_id);
int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum);

// DELETION

int db_delete(int64_t table_id, int64_t key);
void delete_from_leaf(int64_t table_id, pagenum_t leaf_pgnum, int64_t key);
void merge_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                  pagenum_t sibling_pgnum, int sibling_index, int64_t key_prime);
void redistribute_leaves(int64_t table_id, pagenum_t leaf_pgnum,
                         pagenum_t sibling_pgnum, int sibling_index, int key_index);
void delete_from_child(int64_t table_id,
                       pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum);
void delete_from_page(int64_t table_id,
                      pagenum_t p_pgnum, int64_t key, pagenum_t child_pgnum);
void merge_pages(int64_t table_id, pagenum_t p_pgnum,
                 pagenum_t sibling_pgnum, int sibling_index, int64_t k_prime);
void redistribute_pages(int64_t table_id, pagenum_t p_pgnum,
                        pagenum_t sibling_pgnum, int sibling_index,
                        int k_prime_index, int64_t k_prime);
void end_tree(int64_t table_id, pagenum_t root_pgnum);
void adjust_root(int64_t table_id, pagenum_t root_pgnum);
int get_sibling_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t p_pgnum);

#endif
