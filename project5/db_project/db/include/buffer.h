#ifndef DB_BUFFER_H
#define DB_BUFFER_H

#include <pthread.h>

#include "file.h"

#define UNPIN(i) ({ pthread_mutex_unlock(&(buffers[(i)]->page_latch)); })

struct buffer_t {
    page_t frame;
    int64_t table_id;
    pagenum_t page_num;
    uint16_t is_dirty;
    pthread_mutex_t page_latch;
    buffer_t* prev_LRU;
    buffer_t* next_LRU;
};

extern buffer_t** buffers;

int init_buffer(int num_buf);
int shutdown_buffer();

int buffer_get_first_LRU_idx();
int buffer_get_last_LRU_idx();
int buffer_get_buffer_idx(int64_t table_id, pagenum_t page_num);
int buffer_request_page(int64_t table_id, pagenum_t page_num);

pagenum_t buffer_alloc_page(int64_t table_id);
void buffer_free_page(int64_t table_id, pagenum_t page_num);
int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t** dest);
void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t* const* src);

#endif
