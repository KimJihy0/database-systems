#ifndef DB_PAGE_H_
#define DB_PAGE_H_

#include <stdint.h>

typedef uint64_t pagenum_t;

struct __attribute__((packed)) slot_t {
    int64_t key;
    uint16_t size;
    uint16_t offset;
};

struct entry_t {
    int64_t key;
    uint64_t child;
};

struct page_t {
    union {
        uint64_t next_frpg;
        uint64_t parent;
    };
    struct {
        uint32_t is_leaf;
        uint32_t num_keys;
    };
    char reserved[96];
    uint64_t free_space;
    union { 
        uint64_t sibling;
        uint64_t left_child;
    };
    union { 
        union {
            slot_t slots[64];
            char values[3968];
        };
        entry_t entries[248];
    };
};

struct header_t {
    uint64_t free_num;
    uint64_t num_pages;
    uint64_t root_num;
    char reserved[4072];
};

#endif  //DB_PAGE_H_
