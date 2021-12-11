#include "recov.h"

#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <set>

static std::set<int> winners;
static std::set<int> losers;

void recovery(int flag, int log_num, char* logmsg_path) {
    FILE* fp = fopen(logmsg_path, "w");
    anls_pass(fp);
    redo_pass(fp);
    undo_pass(fp);
    fclose(fp);

    buffer_flush();
    log_force();
    trunc_log();
}

void anls_pass(FILE* fp) {
    fprintf(fp, "[ANALYSIS] Analysis pass start.\n");
    log_t* anls_log = (log_t*)malloc(sizeof(log_t) + 2 * 108 + 8);
    uint64_t cur_LSN = 0;
    while (cur_LSN = log_read_log(cur_LSN, anls_log)) {
        if (anls_log->type == BEGIN) {
            losers.insert(anls_log->trx_id);
        } else if (anls_log->type == COMMIT || anls_log->type == ROLLBACK) {
            losers.erase(anls_log->trx_id);
            winners.insert(anls_log->trx_id);
        }
    }
    fprintf(fp, "[ANALYSIS] Analysis pass end");
    fprintf(fp, ". Winner:");
    for (const auto& winner : winners)
        fprintf(fp, " %d", winner);
    fprintf(fp, ", Loser:");
    for (const auto& loser : losers)
        fprintf(fp, " %d", loser);
    fprintf(fp, "\n");
}

void redo_pass(FILE* fp) {
    fprintf(fp, "[REDO] Redo pass start.\n");
    log_t* redo_log = (log_t*)malloc(sizeof(log_t) + 2 * 108 + 8);
    uint64_t cur_LSN = 0;
    while (cur_LSN = log_read_log(cur_LSN, redo_log)) {
        switch (redo_log->type) {
            case BEGIN:
                fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                break;
            case UPDATE:
                page_t* redo_page;
                buffer_read_page(redo_log->table_id, redo_log->page_num, &redo_page);
                if (redo_log->LSN > redo_page->page_LSN) {
                    memcpy((char*)redo_page + redo_log->offset, redo_log->trailer + redo_log->size, redo_log->size);
                    redo_page->page_LSN = redo_log->LSN;
                    buffer_write_page(redo_log->table_id, redo_log->page_num);
                    fprintf(fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", redo_log->LSN, redo_log->trx_id);
                } else {
                    buffer_unpin_page(redo_log->table_id, redo_log->page_num);
                    fprintf(fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                }
                if (trx_is_active(redo_log->trx_id)) {
                    trx_set_last_LSN(redo_log->trx_id, redo_log->LSN);
                }
                break;
            case COMMIT:
                fprintf(fp, "LSN %lu [COMMIT] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                break;
            case ROLLBACK:
                fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                break;
            case COMPENSATE:
                fprintf(fp, "LSN %lu [COMPENSATE] next undo lsn %lu\n", redo_log->LSN, *(redo_log->trailer + 2 * redo_log->size));
                break;
        }
    }
    fprintf(fp, "[REDO] Redo pass end.\n");
}

void undo_pass(FILE* fp) {
    fprintf(fp, "[UNDO] Undo pass start.\n");
    page_t* undo_page;
    log_t* undo_log = (log_t*)malloc(sizeof(log_t) + 2 * 108 + 8);
    for  (const auto& loser : losers) {
        uint64_t undo_LSN = trx_get_last_LSN(loser);
        while (log_read_log(undo_LSN, undo_log) && undo_log->type != BEGIN) {
            uint64_t ret_LSN = log_write_log(trx_get_last_LSN(loser), loser, COMPENSATE,
                    undo_log->table_id, undo_log->page_num, undo_log->offset, undo_log->size,
                    undo_log->trailer + undo_log->size, undo_log->trailer, undo_log->prev_LSN);
            trx_set_last_LSN(loser, ret_LSN);

            buffer_read_page(undo_log->table_id, undo_log->page_num, &undo_page);
            memcpy((char*)undo_page + undo_log->offset, undo_log->trailer, undo_log->size);
            undo_page->page_LSN = ret_LSN;
            buffer_write_page(undo_log->table_id, undo_log->page_num);

            fprintf(fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", undo_log->LSN, undo_log->trx_id);
            undo_LSN = undo_log->type == UPDATE ?
                       undo_log->prev_LSN : *(undo_log->trailer + 2 * undo_log->size);
        }
        log_write_log(trx_get_last_LSN(loser), loser, ROLLBACK);
        trx_remove_entry(loser);
    }
    free(undo_log);
    fprintf(fp, "[UNDO] Undo pass end.\n");
}