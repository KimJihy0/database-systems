#ifndef DB_RECOV_H_
#define DB_RECOV_H_

#include <stdio.h>

#include "trx.h"

void recovery(int flag, int log_num, char* logmsg_path);
void anls_pass(FILE* fp);
int redo_pass(FILE* fp, int log_num);
int undo_pass(FILE* fp, int log_num);

#endif