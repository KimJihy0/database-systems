#include "dbms.h"

/* Initializes DBMS.
 * If successes, returns 0. Otherwise, returns -1.
 */
int init_db() {
    memset(tables, 0x00, NUM_TABLES * sizeof(table_t));
    return 0;
}

/* Shutdowns DBMS.
 * If successes, returns 0. Otherwise, returns -1.
 */
int shutdown_db() {
    file_close_table_file();
    return 0;
}
