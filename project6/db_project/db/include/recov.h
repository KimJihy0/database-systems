#ifndef DB_RECOV_H_
#define DB_RECOV_H_

#include <stdio.h>

#include "trx.h"

void recovery(int flag, int log_num, char* logmsg_path);
void anls_pass(FILE* fp);
void redo_pass(FILE* fp);
void undo_pass(FILE* fp);

#endif