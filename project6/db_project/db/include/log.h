#ifndef DB_LOG_H_
#define DB_LOG_H_

#include <stdint.h>

#define LOGBUF_SIZE 4096

#define BEGIN       0
#define UPDATE      1
#define COMMIT      2
#define ROLLBACK    3
#define COMPENSATE  4

#define old_image(log)        ((log)->trailer)
#define new_image(log)        ((log)->trailer + (log)->size)
#define next_undo_LSN(log)    ((log)->trailer + 2 * (log)->size)

#ifndef ERR_SYS
// #define ERR_SYS(s) ({ perror((s)); exit(1); })
#define ERR_SYS(s) ({ perror((s)); for (int i = 0; ; i++); })
#endif

typedef uint64_t pagenum_t;

#pragma pack(push, 1)
struct log_header_t {
    uint32_t log_size;
    uint64_t LSN;
    uint64_t prev_LSN;
    int trx_id;
    int type;
};

struct log_t {
    uint32_t log_size;
    uint64_t LSN;
    uint64_t prev_LSN;
    int trx_id;
    int type;
    int64_t table_id;
    pagenum_t page_num;
    uint16_t offset;
    uint16_t size;
    char trailer[];
};
#pragma pack(pop)

int init_log(char* log_path);
int shutdown_log();
void trunc_log();

uint64_t log_read_log(uint64_t dest_LSN, log_t* dest);
uint64_t log_write_log(uint64_t prev_LSN, int trx_id, int type);
uint64_t log_write_log(uint64_t prev_LSN, int trx_id, int type, 
                       int64_t table_id, pagenum_t page_num, uint16_t offset, uint16_t size,
                       char* old_image, char* new_image, uint64_t next_undo_LSN = 0);
void log_consider_force(uint32_t log_size);
void log_force();

#endif