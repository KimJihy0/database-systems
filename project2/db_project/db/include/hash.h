#ifndef DB_HASH_H_
#define DB_HASH_H_

#include <stdint.h>
#include <string.h>

#define NUM_TABLES 31
#define EMPTY(t) (strlen(t.pathname) == 0)
#define EQUAL(t1,t2) (!strcmp(t1.pathname, t2.pathname))

struct table_t {
    char pathname[256];
    int fd;
};

int64_t hash_function(char* key);
int64_t add_table(table_t table, table_t ht[]);

#endif  // DB_HASH_H_
