#include "buffer.h"

buffer_t ** buffers;
int buffer_size;

int buffer_init_buffer(int num_buf) {
    int i;
    buffer_size = num_buf;
    buffers = (buffer_t **)malloc(buffer_size * sizeof(buffer_t *));
    if (buffers == NULL) return -1;
    for (i = 0; i < buffer_size; i++) {
        buffers[i] = NULL;
    }
    return 0;
}

int buffer_shutdown_buffer() {
    int i;
    for (i = 0; i < buffer_size; i++) {
        if (buffers[i] != NULL) {
            file_write_page(buffers[i]->table_id,
                            buffers[i]->page_num,
                            &(buffers[i]->frame));
            free(buffers[i]);
        }
    }
    free(buffers);
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
    buffer_t * victim;
    int i, buffer_idx, last_LRU_idx;
    buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    if (buffer_idx == -1) {
        for (i = 0; i < buffer_size; i++) {
            if (buffers[i] == NULL) {
                buffer_idx = i;
                buffers[buffer_idx] = (buffer_t *)malloc(sizeof(buffer_t));
                buffers[buffer_idx]->next_LRU = NULL;
                buffers[buffer_idx]->prev_LRU = NULL;
                break;
            }
        }
        if (i == buffer_size) {
            for (victim = buffers[buffer_get_first_LRU_idx()]; victim; victim = victim->next_LRU) {
                if (victim->is_pinned == 0) break;
            }
            if (victim == NULL) return -1;
            buffer_idx = buffer_get_buffer_idx(victim->table_id, victim->page_num);
            if (buffers[buffer_idx]->is_dirty) {
                file_write_page(buffers[buffer_idx]->table_id,
                                buffers[buffer_idx]->page_num,
                                &(buffers[buffer_idx]->frame));
            }
        }
        file_read_page(table_id, page_num, &(buffers[buffer_idx]->frame));
        buffers[buffer_idx]->table_id = table_id;
        buffers[buffer_idx]->page_num = page_num;
        buffers[buffer_idx]->is_dirty = 0;
        buffers[buffer_idx]->is_pinned = 0;
    }
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
            buffers[header_buffer_idx]->is_pinned--;
        }
        return page_num;
    }
    page_num = header->next_frpg;
    alloc_buffer_idx = buffer_read_page(table_id, page_num, &alloc);
    header->next_frpg = alloc->next_frpg;
    if (alloc_buffer_idx != -1) buffers[alloc_buffer_idx]->is_pinned--;
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
    int buffer_idx;
    buffer_idx = buffer_request_page(table_id, page_num);
    if (buffer_idx != -1) {
        buffers[buffer_idx]->is_pinned++;
        *dest = &(buffers[buffer_idx]->frame);
    }
    else {
        page_t * page = (page_t *)malloc(sizeof(page_t));
        file_read_page(table_id, page_num, page); 
        *dest = page;
    }
    return buffer_idx;
}

void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t * const * src) {
    int buffer_idx;
    buffer_idx = buffer_get_buffer_idx(table_id, page_num);
    if (buffer_idx != -1) {
        buffers[buffer_idx]->is_dirty = 1;
        buffers[buffer_idx]->is_pinned--;
    }
    else {
        file_write_page(table_id, page_num, *src);
        free(*src);
    }
} 

pagenum_t get_root_num(int64_t table_id) {
    page_t * header;
    int header_buffer_idx;
    header_buffer_idx = buffer_read_page(table_id, 0, &header);
    pagenum_t root_num = header->root_num;
    if (header_buffer_idx != -1) buffers[header_buffer_idx]->is_pinned--;
    return root_num;
}

void set_root_num(int64_t table_id, pagenum_t root_num) {
    page_t * header;
    buffer_read_page(table_id, 0, &header);
    header->root_num = root_num;
    buffer_write_page(table_id, 0, &header);
} 
