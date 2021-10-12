#include "hash.h"

int64_t hash_function(char* key) {
    int64_t number = 0;
    for (int i = 0; i < strlen(key); i++)
        number += key[i];
    return number;
}

int64_t add_table(table_t table, table_t ht[]) {
    int64_t table_id = hash_function(table.pathname);
    int i = table_id % NUM_TABLES;
    while (!EMPTY(ht[i])) {
        if (EQUAL(ht[i], table)) return -1;
        i = (++table_id) % NUM_TABLES;
    }
    ht[i] = table;
    return table_id;
}
