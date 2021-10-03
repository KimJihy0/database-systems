#ifndef __BPT_H__
#define __BPT_H__

#include <stdint.h>

typedef uint64_t pagenum_t;

int64_t open_table(char* pathname);

pagenum_t find_leaf(int64_t table_id, int64_t key);

pagenum_t make_page(int64_t table_id);
pagenum_t make_leaf(int64_t table_id);
int get_left_index(int64_t table_id, pagenum_t parent_pgnum, pagenum_t left_pgnum);
void insert_into_leaf(int64_t table_id, pagenum_t leaf_pgnum,
                      int64_t key, char* value, uint16_t val_size);
void insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf_pgnum,
                                      int64_t key, char* value, uint16_t val_size);
void insert_into_page(int64_t table_id, pagenum_t parent_pgnum,
                      int left_index, int64_t key, pagenum_t right_pgnum);
void insert_into_page_after_splitting(int64_t table_id, pagenum_t parent_pgnum,
                                      int left_index, int64_t key, pagenum_t right_pgnum);
void insert_into_parent(int64_t table_id,
                        pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum);
void insert_into_new_root(int64_t table_id,
                          pagenum_t left_pgnum, int64_t key, pagenum_t right_pgnum);
void start_new_tree(int64_t table_id, int64_t key, char* value, uint16_t val_size);
void insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);


#endif /* __BPT_H__*/
