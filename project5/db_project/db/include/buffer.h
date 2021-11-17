#ifndef DB_BUFFER_H
#define DB_BUFFER_H

#include "file.h"

struct buffer_t {
    page_t frame;
    int64_t table_id;
    pagenum_t page_num;
    uint16_t is_dirty;
    uint16_t is_pinned;
    buffer_t * next_LRU;
    buffer_t * prev_LRU;
};

extern buffer_t ** buffers;
extern int buffer_size;

int buffer_init_buffer(int num_buf);
int buffer_shutdown_buffer();

int buffer_get_first_LRU_idx();
int buffer_get_last_LRU_idx();
int buffer_get_buffer_idx(int64_t table_id, pagenum_t page_num);
int buffer_request_page(int64_t table_id, pagenum_t page_num);

pagenum_t buffer_alloc_page(int64_t table_id);
void buffer_free_page(int64_t table_id, pagenum_t page_num);
int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t ** dest);
void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t * const * src);

#endif
