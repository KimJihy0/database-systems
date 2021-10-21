#include "buffer.h"

buffer_t** buffers;
int buf_size;

int get_first_LRU_idx() {
    int i;
    for (i = 0; i < buf_size; i++)
        if (buffers[i] != NULL && buffers[i]->prev_LRU == NULL) return i;
    return -1;
}

int get_last_LRU_idx() {
    int i;
    for (i = 0; i < buf_size; i++)
        if (buffers[i] != NULL && buffers[i]->next_LRU == NULL) return i;
    return -1;
}

int get_buffer_idx(int64_t table_id, pagenum_t page_num) {
    int i;
    for (i = 0; i < buf_size; i++)
        if (buffers[i] != NULL &&
            buffers[i]->table_id == table_id &&
            buffers[i]->page_num == page_num)
            return i;
    return -1;
}

int read_buffer(int64_t table_id, pagenum_t page_num) {
    buffer_t* victim;
    int i, buffer_idx, last_LRU_idx;
    buffer_idx = get_buffer_idx(table_id, page_num);
    if (buffer_idx == -1) {
        for (i = 0; i < buf_size; i++) {
            if (buffers[i] == NULL) {
                buffer_idx = i;
                buffers[buffer_idx] = (buffer_t*)malloc(sizeof(buffer_t));
                buffers[buffer_idx]->next_LRU = NULL;
                buffers[buffer_idx]->prev_LRU = NULL;
                break;
            }
        }
        if (i == buf_size) {
            for (victim = buffers[get_first_LRU_idx()]; victim; victim = victim->next_LRU) {
                if (victim->is_pinned == 0) break;
            }
            if (victim == NULL) return -1;
            buffer_idx = get_buffer_idx(victim->table_id, victim->page_num);
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
    last_LRU_idx = get_last_LRU_idx();
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
    page_t* header;
    buffer_read_page(table_id, 0, &header);

    pagenum_t page_num;
    if (header->free_num == 0) {
        page_num = file_double_page(table_id, header->num_pages);
        header->free_num = page_num - 1;
        header->num_pages = page_num;
    }

    page_t* p;
    page_num = header->free_num;
    buffer_read_page(table_id, page_num, &p);

	header->free_num = p->next_frpg;
    buffer_write_page(table_id, 0, &header);

	return page_num;
}

void buffer_free_page(int64_t table_id, pagenum_t page_num) {
    page_t* header;
    buffer_read_page(table_id, 0, &header);

    page_t* p;
    p->next_frpg = header->free_num;
    buffer_write_page(table_id, page_num, &p);

    header->free_num = page_num;
    buffer_write_page(table_id, 0, &header);
}

int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t** dest_page) {
    int buffer_idx;
    buffer_idx = read_buffer(table_id, page_num);
    if (buffer_idx != -1) {
        *dest_page = &(buffers[buffer_idx]->frame);
    }
    else {
        file_read_page(table_id, page_num, *dest_page); 
    }
    return buffer_idx;
}

void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t* const* src_page) {
    int buffer_idx;
    buffer_idx = read_buffer(table_id, page_num);
    if (buffer_idx != -1) {
        buffers[buffer_idx]->is_dirty = 1;
    }
    else {
        file_write_page(table_id, page_num, *src_page);
    }
}

// void file_close_table_file();

pagenum_t get_root_num(int64_t table_id) {
    page_t* header;
    buffer_read_page(table_id, 0, &header);
    return header->root_num;
}

void set_root_num(int64_t table_id, pagenum_t root_num) {
    page_t* header;
    buffer_read_page(table_id, 0, &header);
    header->root_num = root_num;
    buffer_write_page(table_id, 0, &header);
}

/* ---To do---
 * Find에서 Input/ouput error 이유찾기.
 * doubling시 버퍼처리
 * replacement시 pin?
 * is_pinned 최적화**********(그냥 함수시작끝에 각각?)
 * 갈아엎기 -- 메모리 복사가 아닌 메모리 참조를 해야함!!
 * page_t* const* ? const* page_t*?
 * 
 * ---Done---
 * all buffers are in use. 처리
 * 지역변수 -> 동적할당(필요없음)
 * 
 * ---Recent Modification
 * doubling 추가
 * read_buffer에 합침 (빈버퍼 찾았을 때랑 victim 찾았을 때)
 */