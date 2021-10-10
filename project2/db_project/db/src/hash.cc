#include "hash.h"

int64_t hash_function(char* key) {
    int64_t i, number;
    for (i = 0, number = 0; i < strlen(key); i++)
        number += key[i];
    return number % NUM_TABLES;
}

int64_t add_hash_table(table_t table, table_t ht[]) {
    int64_t i;
    i = hash_function(table.pathname);
    while (!EMPTY(ht[i])) {
        if (EQUAL(ht[i], table)) return -1;
        i = (i + 1) % NUM_TABLES;
    }
    ht[i] = table;
    return i;
}