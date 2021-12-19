#include "log.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int logbuffer_size;
static char* logbuffer;
static int log_tail;
static uint64_t LSN;
static uint64_t flushed_LSN;
static int log_fd;
static pthread_mutex_t logbuffer_latch;

int init_log(char* log_path) {
    logbuffer_size = 100000;
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
    return 0;
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
    fsync(log_fd);
    flushed_LSN = LSN;
    log_tail =  0;
    pthread_mutex_unlock(&logbuffer_latch);
}
