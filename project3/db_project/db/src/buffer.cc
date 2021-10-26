#include "buffer.h"

buffer_t ** buffers;
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
            buffers[i]->table_id == table_id && buffers[i]->page_num == page_num)
            return i;
    return -1;
}

int request_page(int64_t table_id, pagenum_t page_num) {
    buffer_t * victim;
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
    pagenum_t page_num;
    page_t * header, * alloc;
    int header_buffer_idx, alloc_buffer_idx;
    header_buffer_idx = buffer_read_page(table_id, 0, &header);
    if (header->free_num == 0) {
        if (header_buffer_idx != -1) {
            if (buffers[header_buffer_idx]->is_dirty)
                file_write_page(table_id, 0, header);
            page_num = file_alloc_page(table_id);
            file_read_page(table_id, 0, &(buffers[header_buffer_idx]->frame));
        }
        else page_num = file_alloc_page(table_id);
        if (header_buffer_idx != -1) buffers[header_buffer_idx]->is_pinned--;
        return page_num;
    }
    page_num = header->free_num;
    alloc_buffer_idx = buffer_read_page(table_id, page_num, &alloc);
    header->free_num = alloc->next_frpg;
    if (alloc_buffer_idx != -1) buffers[alloc_buffer_idx]->is_pinned--;
    buffer_write_page(table_id, 0, &header);
    return page_num;
}

void buffer_free_page(int64_t table_id, pagenum_t page_num) {
    page_t * header, * free;
    int header_buffer_idx, free_buffer_idx;
    header_buffer_idx = buffer_read_page(table_id, 0, &header);
    free_buffer_idx = buffer_read_page(table_id, page_num, &free);
    free->next_frpg = header->free_num;
    header->free_num = page_num;
    buffer_write_page(table_id, 0, &header);
    buffer_write_page(table_id, page_num, &free);
}

int buffer_read_page(int64_t table_id, pagenum_t page_num, page_t ** dest_page) {
    int buffer_idx;
    buffer_idx = request_page(table_id, page_num);
    if (buffer_idx != -1) {
        buffers[buffer_idx]->is_pinned++;
        *dest_page = &(buffers[buffer_idx]->frame);
    }
    else {
        file_read_page(table_id, page_num, *dest_page); 
    }
    return buffer_idx;
}

void buffer_write_page(int64_t table_id, pagenum_t page_num, page_t ** src_page) {
    int buffer_idx;
    buffer_idx = request_page(table_id, page_num);
    if (buffer_idx != -1) {
        buffers[buffer_idx]->is_dirty = 1;
        buffers[buffer_idx]->is_pinned--;
    }
    else {
        file_write_page(table_id, page_num, *src_page);
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
    int header_buffer_idx;
    header_buffer_idx = buffer_read_page(table_id, 0, &header);
    header->root_num = root_num;
    buffer_write_page(table_id, 0, &header);
}

/* ---To do---
 * 0644
 * 파일 경로 ? (e.g. "/home/table1")
 * page_t** or const* page_t* or page_t* const*
 * 
 * 
 * ---Done---
 * Find에서 Input/ouput error 이유찾기.
 * NUM_KEYS = 10000, NUM_BUFS = 100
 * insert_into_page_split() ENTRY_ORDER + 1 -> ENTRY_ORDER
 * 모든 함수 모든 경우의 수 디버깅
 * ************************** parent 수정 어디서 안되는지 확인 ; 이거때메 오류남 **************************
 * unpin() 함수풀기
 * merge에서 swap-> idx 설정 틀릴 수 있음
 * alloc / free 실시간동기화
 * delete (projec2랑 비교하면서 해야됨)
 * file_open_table_file() 안에서 hash 처리
 * replacement시 pin?
 * pin, unpin 위치 index -> buffer로 이동
 * 구조(page.h, hash.h, hash.cc)
 * file.h specification 변경금지
 * 갈아엎기 -- 메모리 복사가 아닌 메모리 참조를 해야함!!
 * all buffers are in use. 처리
 * 지역변수 -> 동적할당(필요없음)
 * 
 * ---Recent Modification---
 * doubling 추가
 * request_page에 합침 (빈버퍼 찾았을 때랑 victim 찾았을 때)
 * buffer_write_page 수정 (인자, 내용)
 */