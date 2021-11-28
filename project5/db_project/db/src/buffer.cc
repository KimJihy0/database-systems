#include "buffer.h"

buffer_t ** buffers;
int buffer_size;
pthread_mutex_t buffer_latch;

int init_buffer(int num_buf) {
    int i;
    buffer_size = num_buf;
    buffers = (buffer_t **)malloc(buffer_size * sizeof(buffer_t *));
    if (buffers == NULL) return -1;
    for (i = 0; i < buffer_size; i++) {
        buffers[i] = NULL;
    }
    pthread_mutex_init(&buffer_latch, 0);
    return 0;
}

int shutdown_buffer() {
    int i;
    for (i = 0; i < buffer_size; i++) {
        if (buffers[i] != NULL) {
            file_write_page(buffers[i]->table_id,
                            buffers[i]->page_num,
                            &(buffers[i]->frame));
            delete buffers[i];
        }
    }
    free(buffers);
    pthread_mutex_destroy(&buffer_latch);
    return 0;
}

int buffer_get_first_LRU_idx() {
    int i;
    for (i = 0; i < buffer_size; i++)
        if (buffers[i] != NULL && buffers[i]->prev_LRU == NULL) return i;
    return -1;
}

int buffer_get_last_LRU_idx() {
    int i;
    for (i = 0; i < buffer_size; i++)
        if (buffers[i] != NULL && buffers[i]->next_LRU == NULL) return i;
    return -1;
}

int buffer_get_buffer_idx(int64_t table_id, pagenum_t page_num) {
    int i;
    for (i = 0; i < buffer_size; i++)
        if (buffers[i] != NULL &&
            buffers[i]->table_id == table_id &&
            buffers[i]->page_num == page_num)
            return i;
    return -1;
}

int buffer_request_page(int64_t table_id, pagenum_t page_num) {
    pthread_mutex_lock(&buffer_latch);

    buffer_t * victim;
    int i, buffer_idx, last_LRU_idx;
    buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    if (buffer_idx == -1) {
        for (i = 0; i < buffer_size; i++) {
            if (buffers[i] == NULL) {
                buffer_idx = i;
                buffers[buffer_idx] = new buffer_t;
                buffers[buffer_idx]->next_LRU = NULL;
                buffers[buffer_idx]->prev_LRU = NULL;
                buffers[buffer_idx]->is_dirty = 0;
                buffers[buffer_idx]->page_latch = PTHREAD_MUTEX_INITIALIZER;
                pthread_mutex_lock(&(buffers[buffer_idx]->page_latch));
                break;
            }
        }
        if (i == buffer_size) {
            for (victim = buffers[buffer_get_first_LRU_idx()]; victim; victim = victim->next_LRU) {
                if (pthread_mutex_trylock(&(victim->page_latch)) != EBUSY) break;
            }
            if (victim == NULL) return -1;
            buffer_idx = buffer_get_buffer_idx(victim->table_id, victim->page_num);
            if (buffers[buffer_idx]->is_dirty != 0) {
                file_write_page(buffers[buffer_idx]->table_id,
                                buffers[buffer_idx]->page_num,
                                &(buffers[buffer_idx]->frame));
                buffers[buffer_idx]->is_dirty = 0;
            }
        }
        file_read_page(table_id, page_num, &(buffers[buffer_idx]->frame));
        buffers[buffer_idx]->table_id = table_id;
        buffers[buffer_idx]->page_num = page_num;
    }
    else pthread_mutex_lock(&(buffers[buffer_idx]->page_latch));

    if (buffers[buffer_idx]->prev_LRU)
        buffers[buffer_idx]->prev_LRU->next_LRU = buffers[buffer_idx]->next_LRU;
    if (buffers[buffer_idx]->next_LRU)
        buffers[buffer_idx]->next_LRU->prev_LRU = buffers[buffer_idx]->prev_LRU;
    buffers[buffer_idx]->next_LRU = buffers[buffer_idx];
    buffers[buffer_idx]->prev_LRU = buffers[buffer_idx];
    last_LRU_idx = buffer_get_last_LRU_idx();
    if (last_LRU_idx != -1) {
        buffers[last_LRU_idx]->next_LRU = buffers[buffer_idx];
        buffers[buffer_idx]->prev_LRU = buffers[last_LRU_idx];
        buffers[buffer_idx]->next_LRU = NULL;
    }
    else {
        buffers[buffer_idx]->next_LRU = NULL;
        buffers[buffer_idx]->prev_LRU = NULL;
    }

    pthread_mutex_unlock(&buffer_latch);
    return buffer_idx;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
    pagenum_t page_num;
    page_t * header, * alloc;
    int header_buffer_idx, alloc_buffer_idx;
    header_buffer_idx = buffer_read_page(table_id, 0, &header);
    if (header->next_frpg == 0) {
        if (header_buffer_idx != -1) {
            if (buffers[header_buffer_idx]->is_dirty)
                file_write_page(table_id, 0, header);
            buffers[header_buffer_idx]->is_dirty = 0;
        }
        page_num = file_alloc_page(table_id);
        if (header_buffer_idx != -1) {
            file_read_page(table_id, 0, &(buffers[header_buffer_idx]->frame));
            pthread_mutex_unlock(&(buffers[header_buffer_idx]->page_latch));
        }
        return page_num;
    }
    page_num = header->next_frpg;
    alloc_buffer_idx = buffer_read_page(table_id, page_num, &alloc);
    header->next_frpg = alloc->next_frpg;
    pthread_mutex_unlock(&(buffers[alloc_buffer_idx]->page_latch));
    buffer_write_page(table_id, 0, &header);
    return page_num;
}

void buffer_free_page(int64_t table_id, pagenum_t page_num) {
    page_t * header, * free;
    buffer_read_page(table_id, 0, &header);
    buffer_read_page(table_id, page_num, &free);
    free->next_frpg = header->next_frpg;
    header->next_frpg = page_num;
    buffer_write_page(table_id, 0, &header);
    buffer_write_page(table_id, page_num, &free);
}

int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t ** dest) {
    int buffer_idx = buffer_request_page(table_id, page_num);
    *dest = &(buffers[buffer_idx]->frame);
    return buffer_idx;
}

void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t * const * src) {
    int buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    buffers[buffer_idx]->is_dirty = 1;
    pthread_mutex_unlock(&(buffers[buffer_idx]->page_latch));
}
