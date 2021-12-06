#include "buffer.h"

#include <errno.h>

buffer_t** buffers;
int buffer_size;
pthread_mutex_t buffer_latch;

int init_buffer(int num_buf) {
    buffer_size = num_buf;
    buffers = new buffer_t*[buffer_size];
    for (int i = 0; i < buffer_size; i++) {
        buffers[i] = NULL;
    }
    if (pthread_mutex_init(&buffer_latch, 0) != 0)
        return -1;
    return 0;
}

int shutdown_buffer() {
    for (int i = 0; i < buffer_size; i++) {
        if (buffers[i] != NULL && buffers[i]->is_dirty != 0)
            file_write_page(buffers[i]->table_id, buffers[i]->page_num, &(buffers[i]->frame));
        delete buffers[i];
    }
    delete[] buffers;
    if (pthread_mutex_destroy(&buffer_latch) != 0)
        return -1;
    return 0;
}

int buffer_get_first_LRU_idx() {
    for (int i = 0; i < buffer_size; i++)
        if (buffers[i] != NULL && buffers[i]->prev_LRU == NULL) return i;
    return -1;
}

int buffer_get_last_LRU_idx() {
    for (int i = 0; i < buffer_size; i++)
        if (buffers[i] != NULL && buffers[i]->next_LRU == NULL) return i;
    return -1;
}

int buffer_get_buffer_idx(int64_t table_id, pagenum_t page_num) {
    for (int i = 0; i < buffer_size; i++) {
        if (buffers[i] == NULL ||
            (buffers[i]->table_id == table_id && buffers[i]->page_num == page_num))
            return i;
    }
    return -1;
}

int buffer_request_page(int64_t table_id, pagenum_t page_num) {
    pthread_mutex_lock(&buffer_latch);

    int buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    if (buffer_idx != -1) {
        if (buffers[buffer_idx] == NULL) {
            buffers[buffer_idx] = new buffer_t;
            buffers[buffer_idx]->is_dirty = 0;
            buffers[buffer_idx]->page_latch = PTHREAD_MUTEX_INITIALIZER;
            buffers[buffer_idx]->prev_LRU = NULL;
            buffers[buffer_idx]->next_LRU = NULL;
            buffers[buffer_idx]->table_id = table_id;
            buffers[buffer_idx]->page_num = page_num;
            file_read_page(table_id, page_num, &(buffers[buffer_idx]->frame));
        }
        pthread_mutex_lock(&(buffers[buffer_idx]->page_latch));
    } else {
        buffer_t* victim;
        for (victim = buffers[buffer_get_first_LRU_idx()]; victim; victim = victim->next_LRU) {
            if (pthread_mutex_trylock(&(victim->page_latch)) != EBUSY) break;
        }
        buffer_idx = buffer_get_buffer_idx(victim->table_id, victim->page_num);
        if (buffers[buffer_idx]->is_dirty != 0) {
            file_write_page(buffers[buffer_idx]->table_id,
                            buffers[buffer_idx]->page_num,
                            &(buffers[buffer_idx]->frame));
            buffers[buffer_idx]->is_dirty = 0;
        }
        buffers[buffer_idx]->table_id = table_id;
        buffers[buffer_idx]->page_num = page_num;
        file_read_page(table_id, page_num, &(buffers[buffer_idx]->frame));
    }

    if (buffers[buffer_idx]->prev_LRU != NULL)
        buffers[buffer_idx]->prev_LRU->next_LRU = buffers[buffer_idx]->next_LRU;
    if (buffers[buffer_idx]->next_LRU != NULL)
        buffers[buffer_idx]->next_LRU->prev_LRU = buffers[buffer_idx]->prev_LRU;
    buffers[buffer_idx]->next_LRU = buffers[buffer_idx];
    buffers[buffer_idx]->prev_LRU = buffers[buffer_idx];
    int last_LRU_idx = buffer_get_last_LRU_idx();
    if (last_LRU_idx != -1) {
        buffers[last_LRU_idx]->next_LRU = buffers[buffer_idx];
        buffers[buffer_idx]->prev_LRU = buffers[last_LRU_idx];
        buffers[buffer_idx]->next_LRU = NULL;
    } else {
        buffers[buffer_idx]->next_LRU = NULL;
        buffers[buffer_idx]->prev_LRU = NULL;
    }

    pthread_mutex_unlock(&buffer_latch);
    return buffer_idx;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
    pagenum_t page_num;
    page_t *header, *alloc;
    buffer_read_page(table_id, 0, &header);
    if (header->next_frpg == 0) {
        file_write_page(table_id, 0, header);
        page_num = file_alloc_page(table_id);
        buffer_read_page(table_id, 0, &header);
        buffer_unpin_page(table_id, 0);
        return page_num;
    }
    page_num = header->next_frpg;
    buffer_read_page(table_id, page_num, &alloc);
    header->next_frpg = alloc->next_frpg;
    buffer_unpin_page(table_id, page_num);
    buffer_write_page(table_id, 0);
    return page_num;
}

void buffer_free_page(int64_t table_id, pagenum_t page_num) {
    page_t *header, *free;
    buffer_read_page(table_id, 0, &header);
    buffer_read_page(table_id, page_num, &free);
    free->next_frpg = header->next_frpg;
    header->next_frpg = page_num;
    buffer_write_page(table_id, 0);
    buffer_write_page(table_id, page_num);
}

void buffer_read_page(int64_t table_id, pagenum_t page_num, page_t** dest) {
    int buffer_idx = buffer_request_page(table_id, page_num);
    *dest = &(buffers[buffer_idx]->frame);
}

void buffer_write_page(int64_t table_id, pagenum_t page_num) {
    int buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    buffers[buffer_idx]->is_dirty = 1;
    pthread_mutex_unlock(&(buffers[buffer_idx]->page_latch));
}

void buffer_unpin_page(int64_t table_id, pagenum_t page_num) {
    int buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    pthread_mutex_unlock(&(buffers[buffer_idx]->page_latch));
}
