#include "log.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define logfile 0

static int logbuffer_size;
static char* logbuffer;
static int log_tail;
static uint64_t LSN;
static uint64_t flushed_LSN;
static int log_fd;
static pthread_mutex_t logbuffer_latch;

#if logfile
FILE* logfile_fp;
#endif

int init_log(char* log_path) {

    #if logfile
    if ((logfile_fp = fopen("readable_logfile.csv", "w")) == NULL)
        ERR_SYS("failure to open readable_logfile(fopen error).");
    fprintf(logfile_fp, "log_size, LSN, prev_LSN, trx_id, type, table_id, page_num, offset, size, old_image, new_image, next_undo_LSN\n");
    #endif

    logbuffer_size = 100001;
    logbuffer = new char[logbuffer_size];
    log_fd = open(log_path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0)
        ERR_SYS("Failure to open log file(open error)");
    LSN = lseek(log_fd, 0, SEEK_END);
    printf("LSN : %ld\n", LSN);
    flushed_LSN = lseek(log_fd, 0, SEEK_END);
    log_tail = 0;
    if (LSN == 0) {
        log_write_log(0, 0, -1);
    }
    pthread_mutex_init(&logbuffer_latch, 0);

    return 0;
}

int shutdown_log() {
    delete[] logbuffer;
    close(log_fd);
    pthread_mutex_destroy(&logbuffer_latch);

    #if logfile
    fclose(logfile_fp);
    #endif

    return 0;
}

void trunc_log() {
    ftruncate(log_fd, 0);
    LSN = 0;
    flushed_LSN = 0;
    log_tail = 0;
    log_write_log(0, 0, -1);
}

uint64_t log_read_log(uint64_t dest_LSN, log_t* dest) {
    if (dest_LSN >= LSN) return 0;
    uint32_t log_size;
    if (dest_LSN >= flushed_LSN) {
        memcpy(&log_size, logbuffer + (dest_LSN - flushed_LSN), 4);
        memcpy(dest, logbuffer + (dest_LSN - flushed_LSN), log_size);
    } else {
        if (pread(log_fd, &log_size, 4, dest_LSN) != 4)
            ERR_SYS("Failure to read log(read error)");
        if (pread(log_fd, dest, log_size, dest_LSN) != log_size)
            ERR_SYS("Failure to read log(read error)");
    }
    return dest_LSN + log_size;
}

uint64_t log_write_log(uint64_t prev_LSN, int trx_id, int type) {
    pthread_mutex_lock(&logbuffer_latch);

    uint32_t log_size = sizeof(log_header_t);
    log_consider_force(log_size);

    uint64_t src_LSN = LSN;

    log_header_t* new_log = (log_header_t*)malloc(log_size);
    new_log->log_size = log_size;
    new_log->LSN = LSN;
    new_log->prev_LSN = prev_LSN;
    new_log->trx_id = trx_id;
    new_log->type = type;
    memcpy(logbuffer + log_tail, new_log, log_size);
    free(new_log);

    log_tail += log_size;
    LSN += log_size;

    #if logfile
    const char* types[] = { "BEGIN", "UPDATE", "COMMIT", "ROLLBACK", "COMPENSATE" };
    fprintf(logfile_fp, "%u, %lu, %lu, %d, %s\n", log_size, src_LSN, prev_LSN, trx_id, type < 0 ? "NIL" : types[type]);
    #endif

    pthread_mutex_unlock(&logbuffer_latch);
    return src_LSN;
}

uint64_t log_write_log(uint64_t prev_LSN, int trx_id, int type,
                       int64_t table_id, pagenum_t page_num, uint16_t offset, uint16_t size,
                       char* old_image, char* new_image, uint64_t next_undo_LSN) {
    pthread_mutex_lock(&logbuffer_latch);

    uint32_t log_size = sizeof(log_t) + 2 * size + (type == COMPENSATE ? 8 : 0);
    log_consider_force(log_size);

    uint64_t src_LSN = LSN;

    log_t* new_log = (log_t*)malloc(log_size);
    new_log->log_size = log_size;
    new_log->LSN = LSN;
    new_log->prev_LSN = prev_LSN;
    new_log->trx_id = trx_id;
    new_log->type = type;
    new_log->table_id = table_id;
    new_log->page_num = page_num;
    new_log->offset = offset;
    new_log->size = size;
    memcpy(new_log->trailer, old_image, size);
    memcpy(new_log->trailer + size, new_image, size);
    if (type == COMPENSATE)
        memcpy(new_log->trailer + 2 * size, &next_undo_LSN, 8);
    memcpy(logbuffer + log_tail, new_log, log_size);
    free(new_log);

    log_tail += log_size;
    LSN += log_size;

    #if logfile
    if (type == UPDATE)
        fprintf(logfile_fp, "%u, %lu, %lu, %d, %s, %ld, %lu, %d, %d, %s, %s\n",
            log_size, src_LSN, prev_LSN, trx_id, "UPDATE", table_id, page_num, offset, size, old_image, new_image);
    else
        fprintf(logfile_fp, "%u, %lu, %lu, %d, %s, %ld, %lu, %d, %d, %s, %s, %lu\n",
            log_size, src_LSN, prev_LSN, trx_id, "COMPENSATE", table_id, page_num, offset, size, old_image, new_image, next_undo_LSN);

    #endif

    pthread_mutex_unlock(&logbuffer_latch);
    return src_LSN;
}

void log_consider_force(uint32_t log_size) {
    if (log_tail + log_size >= logbuffer_size) {
        if (write(log_fd, logbuffer, log_tail) != log_tail)
            ERR_SYS("Failure to force log(write error)");
        flushed_LSN = LSN;
        log_tail = 0;
    }
}

void log_force() {
    pthread_mutex_lock(&logbuffer_latch);
    if (write(log_fd, logbuffer, log_tail) != log_tail)
        ERR_SYS("Failure to force log(write error)");
    flushed_LSN = LSN;
    log_tail =  0;
    pthread_mutex_unlock(&logbuffer_latch);
}

/* ---To do.---
 * trx_entry에서 삭제한건데 왜 state가 필요한지.
 * logbuffer_size 줄여보기
 * trx_state
 * get_last_LSN(), set_last_LSN() -> log_write_log로 넣어버리기
 * page_latch <-> lock_latch 데드락
 * recov.h, trx.h, log.h -> 소스코드로?
 * assert 많이 넣어보기
 * undo 순서 trx별로인지 시간순서대로인지 확인

 * 가변멤버 new delete 가능?
 * 8byte 4byte 확인 (next_undo_LSN)
 * trx_table[trx_id]->last_LSN 확인
 * trunc_log()?
 * 
 * ---Done.---
 * reopen 처리.
 * NIL 처리
 * rollback할때 page_LSN 갱신해야되는거 아닌가...
 * 로그버퍼사이즈는 자유인지
 * O_APPEND read는 lseek로 가능한지 확인 -> 가능!
 * anls_pass() 다시짜기
 * insert, delete : find_leaf 이후에 직접 중복검사
 * LSN mutex
 * 
 */ 