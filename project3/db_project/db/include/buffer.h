#ifndef _DB_BUFFER_H
#define _DB_BUFFER_H

#include "file.h"

struct buffer_t {
    page_t frame;
    int64_t table_id;
    pagenum_t page_num;
    int is_dirty;
    int is_pinned;
    buffer_t* next_LRU;
    buffer_t* prev_LRU;
};

extern buffer_t** buffers;
extern int buf_size;

int get_first_LRU_idx();
int get_last_LRU_idx();
int get_buffer_idx(int64_t table_id, pagenum_t page_num);
int read_buffer(int64_t table_id, pagenum_t page_num);

// int64_t file_open_table_file(const char* pathname);
pagenum_t buffer_alloc_page(int64_t table_id);
void buffer_free_page(int64_t table_id, pagenum_t page_num);
int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t** dest_page);
void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t* const* src_page);
// void file_close_table_file();

pagenum_t get_root_num(int64_t table_id);
void set_root_num(int64_t table_id, pagenum_t root_num);

#endif