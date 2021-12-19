#include "recov.h"

#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <set>

static std::set<int> winners;
static std::set<int> losers;

void recovery(int flag, int log_num, char* logmsg_path) {
    int crash = 0;

    FILE* fp = fopen(logmsg_path, "w");
    anls_pass(fp);
    if (!crash) {
        crash = redo_pass(fp, flag == 1 ? log_num : -1);
    }
    if (!crash) {
        crash = undo_pass(fp, flag == 2 ? log_num : -1);
    }
    fclose(fp);

    buffer_flush();
    file_close_table_file();
    log_force();
}

void anls_pass(FILE* fp) {
    fprintf(fp, "[ANALYSIS] Analysis pass start.\n");
    log_t* anls_log = (log_t*)malloc(300);
    uint64_t cur_LSN = 0;
    std::set<int> tables;
    while (cur_LSN = log_read_log(cur_LSN, anls_log)) {
        if (anls_log->table_id && tables.find(anls_log->table_id) == tables.end()) {
            char pathname[256];
            sprintf(pathname, "DATA%ld", anls_log->table_id);
            file_open_table_file(pathname);
            tables.insert(anls_log->table_id);
        }
        if (anls_log->trx_id > trx_get_trx_id()) trx_set_trx_id(anls_log->trx_id);
        if (anls_log->type == BEGIN) {
            losers.insert(anls_log->trx_id);
        } else if (anls_log->type == COMMIT || anls_log->type == ROLLBACK) {
            losers.erase(anls_log->trx_id);
            winners.insert(anls_log->trx_id);
        }
    }
    free(anls_log);
    for (const auto& loser : losers) {
        trx_resurrect_entry(loser);
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

int redo_pass(FILE* fp, int log_num) {
    fprintf(fp, "[REDO] Redo pass start.\n");
    log_t* redo_log = (log_t*)malloc(300);
    page_t* redo_page;
    uint64_t cur_LSN = 0;
    int count = log_num;
    while (cur_LSN = log_read_log(cur_LSN, redo_log)) {
        if (count-- == 0) {
            free(redo_log);
            return 1;
        }
        switch (redo_log->type) {
            case BEGIN:
                fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                break;
            case UPDATE:
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
                if (trx_is_active2(redo_log->trx_id)) {
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
                buffer_read_page(redo_log->table_id, redo_log->page_num, &redo_page);
                if (redo_log->LSN > redo_page->page_LSN) {
                    memcpy((char*)redo_page + redo_log->offset, redo_log->trailer + redo_log->size, redo_log->size);
                    redo_page->page_LSN = redo_log->LSN;
                    buffer_write_page(redo_log->table_id, redo_log->page_num);
                    // fprintf(fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", redo_log->LSN, redo_log->trx_id);
                    fprintf(fp, "LSN %lu [COMPENSATE] next undo lsn %lu\n", redo_log->LSN, *(redo_log->trailer + 2 * redo_log->size));
                } else {
                    buffer_unpin_page(redo_log->table_id, redo_log->page_num);
                    fprintf(fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", redo_log->LSN, redo_log->trx_id);
                }
                if (trx_is_active2(redo_log->trx_id)) {
                    trx_set_last_LSN(redo_log->trx_id, redo_log->LSN);
                }
                break;
        }
    }
    free(redo_log);
    fprintf(fp, "[REDO] Redo pass end.\n");
    return 0;
}

int undo_pass(FILE* fp, int log_num) {
    fprintf(fp, "[UNDO] Undo pass start.\n");
    page_t* undo_page;
    log_t* undo_log = (log_t*)malloc(300);
    int count = log_num;

    uint64_t undo_LSN;
    int undo_trx_id;
    std::set<int> to_undo;
    for (const auto& loser : losers) {
        to_undo.insert(trx_get_last_LSN(loser));
    }
    while (!to_undo.empty()) {
        undo_LSN = *std::max_element(to_undo.begin(), to_undo.end());
        to_undo.erase(undo_LSN);
        for (const auto& loser : losers) {
            if (trx_get_last_LSN(loser) == undo_LSN) {
                undo_trx_id = loser;
                break;
            }
        }
        log_read_log(undo_LSN, undo_log);
        if (count-- == 0) {
            free(undo_log);
            return 1;
        }
        if (undo_log->type == UPDATE || undo_log->type == COMPENSATE) {
            uint64_t ret_LSN = log_write_log(trx_get_last_LSN(undo_trx_id), undo_trx_id, COMPENSATE,
                    undo_log->table_id, undo_log->page_num, undo_log->offset, undo_log->size,
                    undo_log->trailer + undo_log->size, undo_log->trailer, undo_log->prev_LSN);
            trx_set_last_LSN(undo_trx_id, ret_LSN);
            
            buffer_read_page(undo_log->table_id, undo_log->page_num, &undo_page);
            memcpy((char*)undo_page + undo_log->offset, undo_log->trailer, undo_log->size);
            undo_page->page_LSN = ret_LSN;
            buffer_write_page(undo_log->table_id, undo_log->page_num);

            fprintf(fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", undo_log->LSN, undo_log->trx_id);
            to_undo.insert(undo_log->type == UPDATE ?
                           undo_log->prev_LSN : *(undo_log->trailer + 2 * undo_log->size));
        }
        else {
            log_write_log(trx_get_last_LSN(undo_trx_id), undo_trx_id, ROLLBACK);
            trx_remove_entry(undo_trx_id);
        }
    }
    free(undo_log);
    fprintf(fp, "[UNDO] Undo pass end.\n");

    return 0;
}